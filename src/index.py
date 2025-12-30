from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
import os

app = FastAPI()

# -------------------
# Health check
# -------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "airtable_token_present": bool(os.getenv("AIRTABLE_API_TOKEN")),
        "airtable_base_present": bool(os.getenv("AIRTABLE_BASE_ID")),
    }

# -------------------
# Test discover crawl
# -------------------
@app.post("/run/test-discover")
async def test_discover():
    AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_TOKEN")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        raise HTTPException(
            status_code=500,
            detail="Missing AIRTABLE_API_TOKEN or AIRTABLE_BASE_ID in Railway Variables",
        )

    TARGET_TABLE = "target_company_registry"
    JOB_SOURCES_TABLE = "job_sources"

    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "BDJobCrawler/1.0 (+contact: youremail@example.com)",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        # 1. Get ONE test company
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TARGET_TABLE}?maxRecords=1"
        r = await client.get(url, headers=headers)
        r.raise_for_status()

        data = r.json()
        if not data.get("records"):
            raise HTTPException(status_code=404, detail="No companies found in Airtable")

        record = data["records"][0]["fields"]

        company_id = record.get("company_id")
        careers_url = record.get("careers_url")

        if not company_id or not careers_url:
            raise HTTPException(
                status_code=400,
                detail="company_id or careers_url missing in Airtable record",
            )

        # 2. Fetch careers page
        page = await client.get(careers_url, headers=headers)
        page.raise_for_status()

        soup = BeautifulSoup(page.text, "html.parser")

        # 3. Extract job links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "job" in href.lower():
                if href.startswith("/"):
                    href = careers_url.rstrip("/") + href
                links.add(href)

        # 4. Write job links to Airtable
        records = []
        now = datetime.utcnow().isoformat()

        for link in links:
            records.append({
                "fields": {
                    "company_id": company_id,
                    "job_url": link,
                    "source_type": "html",
                    "first_seen_at": now,
                    "active": True,
                }
            })

        if records:
            await client.post(
                f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{JOB_SOURCES_TABLE}",
                headers=headers,
                json={"records": records},
            )

        return {
            "company_id": company_id,
            "careers_url": careers_url,
            "jobs_found": len(records),
        }
