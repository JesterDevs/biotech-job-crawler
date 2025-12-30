import os
from datetime import datetime
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException

app = FastAPI()

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
    return {"service": "railway-crawler", "docs": "/docs", "health": "/health"}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/run/test-discover")
async def test_discover():
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Read ONE company from Airtable
        companies_table = quote(AIRTABLE_COMPANIES_TABLE, safe="")
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{companies_table}?maxRecords=1"

        r = await client.get(url, headers=AIRTABLE_HEADERS)
        if r.status_code >= 400:
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
            return {"message": "No company records found in Airtable table", "table": AIRTABLE_COMPANIES_TABLE}

        fields = data["records"][0].get("fields", {})
        company_id = fields.get("company_id") or data["records"][0]["id"]
        careers_url = fields.get("careers_url")

        if not careers_url:
            return {"message": "Company record missing careers_url", "company_id": company_id}

        # 2) Fetch careers page
        page = await client.get(careers_url)
        if page.status_code >= 400:
            return {"message": "Failed to fetch careers_url", "status": page.status_code, "careers_url": careers_url}

        soup = BeautifulSoup(page.text, "html.parser")

        # 3) Extract job-ish links (simple heuristic for test)
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "job" in href.lower() or "careers" in href.lower() or "positions" in href.lower():
                if href.startswith("/"):
                    href = careers_url.rstrip("/") + href
                links.add(href)

        # 4) Write to Airtable job_sources
        if not links:
            return {"company_id": company_id, "careers_url": careers_url, "jobs_found": 0}

        records = []
        now = datetime.utcnow().isoformat()
        for link in list(links)[:25]:  # cap for testing
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

        job_sources_table = quote(AIRTABLE_JOB_SOURCES_TABLE, safe="")
        w = await client.post(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{job_sources_table}",
            headers=AIRTABLE_HEADERS,
            json={"records": records},
        )

        if w.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={"error": "Airtable write failed", "status": w.status_code, "body": w.text},
            )

        return {"company_id": company_id, "careers_url": careers_url, "jobs_found": len(records)}

