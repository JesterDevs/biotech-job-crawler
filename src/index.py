from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
import os
from urllib.parse import urljoin, quote

app = FastAPI()

AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

TARGET_TABLE = os.getenv("TARGET_TABLE", "target_company_registry")
JOB_SOURCES_TABLE = os.getenv("JOB_SOURCES_TABLE", "job_sources")

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    raise RuntimeError("Missing AIRTABLE_PAT or AIRTABLE_BASE_ID environment variables")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json"
}

@app.get("/")
def root():
    return {"ok": True, "service": "crawler"}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run/test-discover")
async def test_discover():
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # Airtable table names with special chars/spaces should be URL-encoded
        target_table_encoded = quote(TARGET_TABLE, safe="")
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{target_table_encoded}?maxRecords=1"

        r = await client.get(url, headers=AIRTABLE_HEADERS)
        if r.status_code != 200:
            raise HTTPException(status_code=500, detail={
                "error": "Airtable read failed",
                "status": r.status_code,
                "body": r.text
            })

        data = r.json()
        if not data.get("records"):
            return {"ok": True, "message": "No target companies found in Airtable."}

        record_fields = data["records"][0].get("fields", {})

        company_id = record_fields.get("company_id")
        careers_url = record_fields.get("careers_url")

        if not company_id or not careers_url:
            raise HTTPException(status_code=400, detail={
                "error": "Target record missing required fields",
                "required": ["company_id", "careers_url"],
                "got": list(record_fields.keys())
            })

        # Fetch careers page
        page = await client.get(careers_url)
        if page.status_code != 200:
            raise HTTPException(status_code=500, detail={
                "error": "Careers page fetch failed",
                "status": page.status_code,
                "careers_url": careers_url
            })

        soup = BeautifulSoup(page.text, "html.parser")

        # Extract job-ish links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "job" in href.lower() or "career" in href.lower() or "opening" in href.lower():
                full = urljoin(careers_url, href)
                links.add(full)

        # Write job links to Airtable (limit to avoid 10-record API issues at first)
        records = []
        now = datetime.utcnow().isoformat()
        for link in list(links)[:10]:
            records.append({
                "fields": {
                    "company_id": company_id,
                    "job_url": link,
                    "source_type": "html",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "active": True
                }
            })

        if records:
            job_sources_encoded = quote(JOB_SOURCES_TABLE, safe="")
            wr = await client.post(
                f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{job_sources_encoded}",
                headers=AIRTABLE_HEADERS,
                json={"records": records}
            )
            if wr.status_code not in (200, 201):
                raise HTTPException(status_code=500, detail={
                    "error": "Airtable write failed",
                    "status": wr.status_code,
                    "body": wr.text
                })

        return {
            "company_id": company_id,
            "careers_url": careers_url,
            "jobs_found": len(records),
            "sample": list(links)[:5]
        }

