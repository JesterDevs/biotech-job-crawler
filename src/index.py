from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin
import os
import json

app = FastAPI(title="Biotech Job Crawler", version="0.1.0")

# ---- ENV ----
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")
AIRTABLE_JOBS_TABLE = os.getenv("AIRTABLE_JOBS_TABLE", "jobs")
AIRTABLE_OUTREACH_TABLE = os.getenv("AIRTABLE_OUTREACH_TABLE", "bd_outreach_queue")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

AIRTABLE_API = "https://api.airtable.com/v0"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def require_env():
    missing = []
    if not AIRTABLE_PAT:
        missing.append("AIRTABLE_PAT")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if missing:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Missing required environment variables",
                "missing": missing
            },
        )

def airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_PAT}",
        "Content-Type": "application/json",
    }

async def airtable_get(client: httpx.AsyncClient, table: str, params=None):
    url = f"{AIRTABLE_API}/{AIRTABLE_BASE_ID}/{table}"
    r = await client.get(url, headers=airtable_headers(), params=params)
    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable request failed",
                "status": r.status_code,
                "base_id": AIRTABLE_BASE_ID,
                "table": table,
                "body": r.text,
                "debug": {"pat_present": bool(AIRTABLE_PAT)} if DEBUG_SHOW_AIRTABLE else {}
            },
        )
    return r.json()

async def airtable_post_records(client: httpx.AsyncClient, table: str, records: list[dict]):
    url = f"{AIRTABLE_API}/{AIRTABLE_BASE_ID}/{table}"
    r = await client.post(url, headers=airtable_headers(), json={"records": records})
    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable write failed",
                "status": r.status_code,
                "base_id": AIRTABLE_BASE_ID,
                "table": table,
                "body": r.text,
            },
        )
    return r.json()

# ---- ROUTES ----
@app.get("/")
def root():
    return {"ok": True, "service": "biotech-job-crawler", "ts": now_iso()}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {"name": "biotech-job-crawler", "version": app.version}

@app.get("/run/airtable-test")
async def airtable_test():
    """
    Verifies PAT + base access and returns table names/ids/fields
    using Airtable's metadata endpoint.
    """
    require_env()
    meta_url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(meta_url, headers=airtable_headers())
        if r.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Airtable meta read failed",
                    "status": r.status_code,
                    "body": r.text,
                },
            )

        meta = r.json()
        tables = []
        for t in meta.get("tables", []):
            fields = [f.get("name") for f in t.get("fields", [])]
            tables.append({"name": t.get("name"), "id": t.get("id"), "fields": fields})

        return {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "env": {
                "companies_table": AIRTABLE_COMPANIES_TABLE,
                "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
                "jobs_table": AIRTABLE_JOBS_TABLE,
                "outreach_table": AIRTABLE_OUTREACH_TABLE,
                "pat_present": bool(AIRTABLE_PAT),
            } if DEBUG_SHOW_AIRTABLE else {"pat_present": bool(AIRTABLE_PAT)},
            "tables": tables,
            "hint": "Confirm AIRTABLE_*_TABLE values match ONE of the table names or table IDs listed here.",
        }

@app.post("/run/test-discover")
async def test_discover():
    """
    Stage A (Discover): pull one company from target_company_registry,
    fetch its Careers_URL, discover job-ish links, write to job_sources.
    """
    require_env()

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Fetch one company
        data = await airtable_get(client, AIRTABLE_COMPANIES_TABLE, params={"maxRecords": 1})
        records = data.get("records", [])
        if not records:
            raise HTTPException(status_code=400, detail={"error": "No company records found in companies table"})

        rec = records[0]
        fields = rec.get("fields", {})
        record_id = rec.get("id")

        # IMPORTANT: Your table uses "Careers_URL" field name (per screenshot)
        careers_url = fields.get("Careers_URL") or fields.get("careers_url") or fields.get("Careers URL")
        company_name = fields.get("Company Name") or fields.get("company_name") or "Unknown"

        # Use Airtable record id as company_id unless you have your own field
        company_id = fields.get("company_id") or record_id

        if not careers_url:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Company record missing required fields",
                    "required": ["Careers_URL"],
                    "found_fields": list(fields.keys()),
                    "record_id": record_id,
                },
            )

        # 2) Fetch careers page
        page = await client.get(careers_url)
        if page.status_code >= 400:
            raise HTTPException(
                status_code=400,
                detail={"error": "Failed to fetch careers_url", "status": page.status_code, "careers_url": careers_url},
            )

        # 3) Discover job-ish links
        soup = BeautifulSoup(page.text, "html.parser")
        links = set()

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue

            # Normalize relative URLs
            full = urljoin(careers_url, href)

            # Very simple heuristic for now
            h = full.lower()
            if any(x in h for x in ["job", "career", "opening", "requisition", "apply"]):
                links.add(full)

        # 4) Write into Airtable job_sources
        records_to_write = []
        ts = now_iso()
        for link in sorted(links):
            records_to_write.append(
                {
                    "fields": {
                        "company_id": company_id,
                        "job_url": link,
                        "source_type": "html",
                        "first_seen_at": ts,
                        "last_seen_At": ts,   # matches your field casing in Airtable screenshot
                        "Active": True,       # matches your field name "Active"
                    }
                }
            )

        wrote = 0
        if records_to_write:
            # Airtable API max 10 records per request by default safe chunk
            chunk_size = 10
            for i in range(0, len(records_to_write), chunk_size):
                chunk = records_to_write[i : i + chunk_size]
                await airtable_post_records(client, AIRTABLE_JOB_SOURCES_TABLE, chunk)
                wrote += len(chunk)

        return {
            "ok": True,
            "company_name": company_name,
            "company_id_used": company_id,
            "airtable_company_record_id": record_id,
            "careers_url": careers_url,
            "jobs_found": len(links),
            "jobs_written": wrote,
            "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE if DEBUG_SHOW_AIRTABLE else "hidden",
        }


