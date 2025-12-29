from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}
from fastapi import FastAPI
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
import os

app = FastAPI()

AIRTABLE_TOKEN = os.getenv("AIRTABLE_API_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

TARGET_TABLE = "target_company_registry"
JOB_SOURCES_TABLE = "job_sources"

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

@app.post("/run/test-discover")
async def test_discover():
    async with httpx.AsyncClient(timeout=20) as client:
        # 1. Get ONE test company
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TARGET_TABLE}?maxRecords=1"
        r = await client.get(url, headers=AIRTABLE_HEADERS)
        data = r.json()
        record = data["records"][0]["fields"]

        company_id = record["company_id"]
        careers_url = record["careers_url"]

        # 2. Fetch careers page
        page = await client.get(careers_url)
        soup = BeautifulSoup(page.text, "html.parser")

        # 3. Extract job links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "job" in href.lower():
                if href.startswith("/"):
                    href = careers_url.rstrip("/") + href
                links.add(href)

        # 4. Write job links to Airtable
        records = []
        for link in links:
            records.append({
                "fields": {
                    "company_id": company_id,
                    "job_url": link,
                    "source_type": "html",
                    "first_seen_at": datetime.utcnow().isoformat(),
                    "active": True
                }
            })

        if records:
            await client.post(
                f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{JOB_SOURCES_TABLE}",
                headers=AIRTABLE_HEADERS,
                json={"records": records}
            )

        return {
            "company_id": company_id,
            "careers_url": careers_url,
            "jobs_found": len(records)
        }
