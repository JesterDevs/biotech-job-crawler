from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
from urllib.parse import urljoin

app = FastAPI(title="Biotech Job Crawler", version="1.0.0")

# ---- ENV ----
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")
AIRTABLE_JOBS_TABLE = os.getenv("AIRTABLE_JOBS_TABLE", "jobs")
AIRTABLE_OUTREACH_TABLE = os.getenv("AIRTABLE_OUTREACH_TABLE", "bd_outreach_queue")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    raise RuntimeError("Missing AIRTABLE_PAT or AIRTABLE_BASE_ID environment variables")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

def utc_iso():
    return datetime.now(timezone.utc).isoformat()

async def airtable_request(client: httpx.AsyncClient, method: str, table: str, *, params=None, json=None):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"
    r = await client.request(method, url, headers=AIRTABLE_HEADERS, params=params, json=json)
    return r

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {"version": app.version}

@app.get("/run/airtable-test")
async def airtable_test():
    """
    Confirms token can READ from the companies table and shows which table names/ids you're using.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        r = await airtable_request(client, "GET", AIRTABLE_COMPANIES_TABLE, params={"maxRecords": 1})
        if r.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": f"Airtable auth/read failed ({r.status_code})",
                    "base_id": AIRTABLE_BASE_ID,
                    "table": AIRTABLE_COMPANIES_TABLE,
                    "body": r.text,
                    "debug": {"pat_present": bool(AIRTABLE_PAT)},
                },
            )

        data = r.json()
        sample = {}
        if data.get("records"):
            fields = data["records"][0].get("fields", {})
            sample = {
                "airtable_record_id": data["records"][0].get("id"),
                "company_name": fields.get("Company Name"),
                "careers_url_present": bool(fields.get("Careers_URL")),
            }

        out = {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "env": {
                "companies_table": AIRTABLE_COMPANIES_TABLE,
                "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
                "jobs_table": AIRTABLE_JOBS_TABLE,
                "outreach_table": AIRTABLE_OUTREACH_TABLE,
                "pat_present": bool(AIRTABLE_PAT),
            },
            "sample_company": sample,
        }
        if DEBUG_SHOW_AIRTABLE:
            out["debug"] = {"fields_present": list(data["records"][0].get("fields", {}).keys()) if data.get("records") else []}
        return out

@app.post("/run/airtable-write-test")
async def airtable_write_test():
    """
    Attempts to CREATE 1 record in job_sources.
    If this fails with 403, it's almost certainly token/base/table permission or table is read-only/synced.
    """
    payload = {
        "records": [
            {
                "fields": {
                    "company_id": "write-test",
                    "job_url": "https://example.com/job/123",
                    "source_type": "html",
                    "first_seen_at": utc_iso(),
                    "last_seen_At": utc_iso(),
                    "active": True,
                }
            }
        ]
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await airtable_request(client, "POST", AIRTABLE_JOB_SOURCES_TABLE, json=payload)

        if r.status_code not in (200, 201):
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Airtable write failed",
                    "status": r.status_code,
                    "base_id": AIRTABLE_BASE_ID,
                    "table": AIRTABLE_JOB_SOURCES_TABLE,
                    "body": r.text,
                },
            )

        return {"ok": True, "created": r.json()}

@app.post("/run/test-discover")
async def test_discover():
    """
    Stage A: pick ONE company from target_company_registry and discover job URLs from Careers_URL.
    Writes discovered URLs into job_sources.
    """
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Get one company
        r = await airtable_request(client, "GET", AIRTABLE_COMPANIES_TABLE, params={"maxRecords": 1})
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
            raise HTTPException(status_code=400, detail="No companies found in target_company_registry")

        rec = data["records"][0]
        fields = rec.get("fields", {})

        company_id = rec.get("id")  # safest unique identifier
        careers_url = fields.get("Careers_URL")
        if not careers_url:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Company record missing required fields",
                    "required": ["Careers_URL"],
                    "found_fields": list(fields.keys()),
                },
            )

        # 2) Fetch careers page
        page = await client.get(careers_url)
        if page.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={"error": "Careers page fetch failed", "status": page.status_code, "careers_url": careers_url},
            )

        soup = BeautifulSoup(page.text, "html.parser")

        # 3) Extract job-ish links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue
            if "job" in href.lower() or "careers" in href.lower() or "positions" in href.lower():
                full = urljoin(careers_url, href)
                links.add(full)

        # limit for safety in a test run
        links = list(links)[:50]

        # 4) Write to Airtable job_sources
        now = utc_iso()
        records = [
            {
                "fields": {
                    "company_id": company_id,
                    "job_url": link,
                    "source_type": "html",
                    "first_seen_at": now,
                    "last_seen_At": now,
                    "active": True,
                }
            }
            for link in links
        ]

        if not records:
            return {"ok": True, "company_id": company_id, "careers_url": careers_url, "jobs_found": 0}

        wr = await airtable_request(client, "POST", AIRTABLE_JOB_SOURCES_TABLE, json={"records": records})
        if wr.status_code not in (200, 201):
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Airtable write failed",
                    "status": wr.status_code,
                    "base_id": AIRTABLE_BASE_ID,
                    "table": AIRTABLE_JOB_SOURCES_TABLE,
                    "body": wr.text,
                },
            )

        return {"ok": True, "company_id": company_id, "careers_url": careers_url, "jobs_found": len(records)}
