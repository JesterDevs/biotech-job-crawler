from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
from urllib.parse import urljoin

app = FastAPI()

AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")

def airtable_headers():
    if not AIRTABLE_PAT:
        raise RuntimeError("Missing AIRTABLE_PAT env var")
    return {
        "Authorization": f"Bearer {AIRTABLE_PAT}",
        "Content-Type": "application/json",
    }

@app.get("/")
def root():
    return {"ok": True, "service": "railway-crawler"}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run/test-discover")
async def test_discover():
    if not AIRTABLE_BASE_ID:
        raise HTTPException(status_code=500, detail="Missing AIRTABLE_BASE_ID env var")

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Pull one company from Airtable
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{COMPANIES_TABLE}?maxRecords=1"
        r = await client.get(url, headers=airtable_headers())
        if r.status_code != 200:
            raise HTTPException(status_code=500, detail={
                "error": "Airtable read failed",
                "status": r.status_code,
                "body": r.text
            })

        data = r.json()
        if not data.get("records"):
            return {"message": "No companies found in Airtable table", "table": COMPANIES_TABLE}

        fields = data["records"][0].get("fields", {})
        company_id = fields.get("company_id")
        careers_url = fields.get("careers_url")

        if not company_id or not careers_url:
            return {
                "message": "Missing company_id or careers_url in the first record",
                "fields_found": list(fields.keys())
            }

        # 2) Fetch careers page
        page = await client.get(careers_url, headers={"User-Agent": "JessBDJobCrawler/1.0"})
        if page.status_code != 200:
            raise HTTPException(status_code=500, detail={
                "error": "Careers page fetch failed",
                "status": page.status_code,
                "careers_url": careers_url
            })

        soup = BeautifulSoup(page.text, "html.parser")

        # 3) Discover job-ish links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "job" in href.lower() or "careers" in href.lower() or "positions" in href.lower():
                full = urljoin(careers_url, href)
                links.add(full)

        # limit so Airtable doesn’t reject big payloads
        links = list(links)[:25]

        # 4) Write discovered links to Airtable
        now = datetime.now(timezone.utc).isoformat()
        records = [{
            "fields": {
                "company_id": company_id,
                "job_url": link,
                "first_seen_at": now,
                "last_seen_at": now,
                "active": True,
                "source_type": "html"
            }
        } for link in links]

        if records:
            post_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{JOB_SOURCES_TABLE}"
            pr = await client.post(post_url, headers=airtable_headers(), json={"records": records})
            if pr.status_code not in (200, 201):
                raise HTTPException(status_code=500, detail={
                    "error": "Airtable write failed",
                    "status": pr.status_code,
                    "body": pr.text
                })

        return {
            "company_id": company_id,
            "careers_url": careers_url,
            "jobs_found": len(records),
            "sample_urls": links[:5]
        }


