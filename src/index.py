from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os

app = FastAPI()

# ---- Env vars (Railway Variables) ----
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    raise RuntimeError("Missing AIRTABLE_PAT or AIRTABLE_BASE_ID environment variables")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

@app.get("/")
def root():
    return {"status": "ok", "docs": "/docs", "health": "/health"}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run/test-discover")
async def test_discover():
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) pull 1 company from Airtable
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_COMPANIES_TABLE}?maxRecords=1"
        r = await client.get(url, headers=AIRTABLE_HEADERS)

        if r.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Airtable read failed",
                    "status": r.status_code,
                    "body": r.text,
                },
            )

        data = r.json()
        if not data.get("records"):
            return {"message": "No company records found in target_company_registry"}

        fields = data["records"][0].get("fields", {})
        company_id = fields.get("company_id")
        careers_url = fields.get("careers_url")

        if not company_id or not careers_url:
            raise HTTPException(
                status_code=400,
                detail="Record missing required fields: company_id and careers_url",
            )

        # 2) fetch careers page
        page = await client.get(careers_url)
        page.raise_for_status()
        soup = BeautifulSoup(page.text, "html.parser")

        # 3) discover job-ish links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "job" in href.lower():
                if href.startswith("/"):
                    href = careers_url.rstrip("/") + href
                links.add(href)

        # 4) write job_sources to Airtable (limit to avoid 422 payload too large)
        now = datetime.now(timezone.utc).isoformat()
        records = []
        for link in list(links)[:25]:
            records.append({
                "fields": {
                    "company_id": company_id,
                    "job_url": link,
                    "source_type": "html",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "active": True,
                }
            })

        if records:
            wr = await client.post(
                f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_JOB_SOURCES_TABLE}",
                headers=AIRTABLE_HEADERS,
                json={"records": records},
            )
            if wr.status_code not in (200, 201):
                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": "Airtable write failed",
                        "status": wr.status_code,
                        "body": wr.text,
                    },
                )

        return {
            "company_id": company_id,
            "careers_url": careers_url,
            "jobs_found": len(records),
            "sample_urls": list(links)[:5],
        }
