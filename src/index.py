from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
from urllib.parse import quote

app = FastAPI()

# --- Env vars (set these in Railway > Service > Variables) ---
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# Keep table names configurable, but default to what you're using
TARGET_TABLE = os.getenv("TARGET_TABLE", "target_company_registry")
JOB_SOURCES_TABLE = os.getenv("JOB_SOURCES_TABLE", "job_sources")

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    # This makes debugging WAY easier than a mysterious 500 later
    raise RuntimeError("Missing AIRTABLE_PAT or AIRTABLE_BASE_ID environment variables")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

def airtable_table_url(table_name: str) -> str:
    # Airtable requires URL-encoding if you ever use spaces/special chars
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(table_name)}"

@app.get("/health")
def health():
    return {"ok": True}

# Make this GET so you can test from browser easily (no POST confusion)
@app.get("/run/test-discover")
async def test_discover():
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Grab ONE test company
        url = airtable_table_url(TARGET_TABLE) + "?maxRecords=1"
        r = await client.get(url, headers=AIRTABLE_HEADERS)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Airtable read failed: {r.status_code} {r.text}")

        data = r.json()
        if not data.get("records"):
            return {"message": "No records found in target_company_registry"}

        fields = data["records"][0].get("fields", {})
        company_id = fields.get("company_id")
        careers_url = fields.get("careers_url")

        if not company_id or not careers_url:
            raise HTTPException(
                status_code=400,
                detail="Your target_company_registry record must include company_id and careers_url"
            )

        # 2) Fetch careers page
        page = await client.get(careers_url)
        if page.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Careers page fetch failed: {page.status_code}")

        soup = BeautifulSoup(page.text, "html.parser")

        # 3) Extract job-ish links (basic MVP)
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "job" in href.lower() or "careers" in href.lower() or "positions" in href.lower():
                if href.startswith("/"):
                    href = careers_url.rstrip("/") + href
                links.add(href)

        # 4) Write discovered links to Airtable
        now = datetime.now(timezone.utc).isoformat()

        records = [{
            "fields": {
                "company_id": company_id,
                "job_url": link,
                "source_type": "html",
                "first_seen_at": now,
                "last_seen_at": now,
                "active": True
            }
        } for link in links]

        if records:
            wr = await client.post(
                airtable_table_url(JOB_SOURCES_TABLE),
                headers=AIRTABLE_HEADERS,
                json={"records": records[:10]}  # limit for testing; Airtable max 10 per request
            )
            if wr.status_code not in (200, 201):
                raise HTTPException(status_code=502, detail=f"Airtable write failed: {wr.status_code} {wr.text}")

        return {
            "company_id": company_id,
            "careers_url": careers_url,
            "jobs_found": len(records),
            "sample": list(links)[:5]
        }
