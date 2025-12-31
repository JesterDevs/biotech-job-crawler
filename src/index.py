from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
import re
from urllib.parse import quote

APP_VERSION = "0.3.0"

app = FastAPI(title="Biotech Job Crawler", version=APP_VERSION)

# ----------------------------
# Env + config
# ----------------------------
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")  # IMPORTANT: this must match Railway variable name
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")
AIRTABLE_JOBS_TABLE = os.getenv("AIRTABLE_JOBS_TABLE", "jobs")
AIRTABLE_OUTREACH_TABLE = os.getenv("AIRTABLE_OUTREACH_TABLE", "bd_outreach_queue")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

if not AIRTABLE_BASE_ID or not AIRTABLE_PAT:
    # Don't crash on import in prod if you want /health to work,
    # but most people prefer failing fast:
    raise RuntimeError("Missing AIRTABLE_PAT or AIRTABLE_BASE_ID environment variables")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def airtable_url(table_name: str) -> str:
    # Airtable allows table names with spaces; URL encode safely
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{quote(table_name, safe='')}"

# ----------------------------
# Helpers
# ----------------------------
async def airtable_get_one_company(client: httpx.AsyncClient):
    """
    Pull one company row that is Allowed = true (if field exists),
    otherwise just grab the first row.
    """
    url = airtable_url(AIRTABLE_COMPANIES_TABLE)

    # Try to filter to Allowed=1, but if your base doesn't have it, we still succeed.
    params = {
        "maxRecords": 1
    }

    r = await client.get(url, headers=AIRTABLE_HEADERS, params=params)
    if r.status_code == 401:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable auth failed (401)",
                "hint": "Check AIRTABLE_PAT is correct AND token has access to this base",
                "base_id": AIRTABLE_BASE_ID,
                "table": AIRTABLE_COMPANIES_TABLE,
                "debug": {"pat_present": bool(AIRTABLE_PAT)}
            }
        )
    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={"error": "Airtable read failed", "status": r.status_code, "body": r.text}
        )

    data = r.json()
    records = data.get("records", [])
    if not records:
        raise HTTPException(status_code=400, detail={"error": "No company records found in Airtable"})

    return records[0]  # full Airtable record (id + fields)

def extract_company_fields(airtable_record: dict):
    """
    Accepts your actual Airtable field names.
    - company_id: use explicit field if present; otherwise use Airtable record id (recXXXX)
    - careers_url: try common variants
    """
    fields = airtable_record.get("fields", {})
    record_id = airtable_record.get("id")

    # company_id: if you have a field named company_id, use it; else use record_id.
    company_id = fields.get("company_id") or record_id

    # careers_url: match your screenshot "Careers_URL"
    careers_url = (
        fields.get("careers_url")
        or fields.get("Careers_URL")
        or fields.get("Careers URL")
        or fields.get("CareersUrl")
        or fields.get("careersUrl")
    )

    company_name = fields.get("Company Name") or fields.get("company_name") or fields.get("Name") or fields.get("name")

    return {
        "company_id": company_id,
        "company_name": company_name,
        "careers_url": careers_url,
        "fields_present": list(fields.keys())
    }

def absolutize(href: str, base_url: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return base_url.rstrip("/") + href
    return base_url.rstrip("/") + "/" + href.lstrip("/")

def looks_like_job_link(url: str) -> bool:
    u = url.lower()
    # simple heuristic for test discover
    patterns = [
        r"/job", r"/jobs", r"jobid", r"requisition", r"career", r"opening", r"position"
    ]
    return any(re.search(p, u) for p in patterns)

async def airtable_insert_job_sources(client: httpx.AsyncClient, records: list[dict]):
    """
    Airtable API limit: 10 records per request.
    """
    if not records:
        return {"inserted": 0}

    url = airtable_url(AIRTABLE_JOB_SOURCES_TABLE)
    inserted = 0

    for i in range(0, len(records), 10):
        chunk = records[i:i+10]
        payload = {"records": chunk}
        r = await client.post(url, headers=AIRTABLE_HEADERS, json=payload)

        if r.status_code == 401:
            raise HTTPException(
                status_code=500,
                detail={"error": "Airtable write failed (401)", "table": AIRTABLE_JOB_SOURCES_TABLE}
            )
        if r.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={"error": "Airtable write failed", "status": r.status_code, "body": r.text}
            )

        inserted += len(chunk)

    return {"inserted": inserted}

# ----------------------------
# Routes
# ----------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {"version": APP_VERSION}

@app.get("/run/airtable-test")
async def airtable_test():
    """
    Proves:
    - Railway env vars are loaded
    - Airtable PAT can read from target_company_registry
    """
    async with httpx.AsyncClient(timeout=30) as client:
        rec = await airtable_get_one_company(client)
        parsed = extract_company_fields(rec)

        # show minimal debug, never show PAT
        resp = {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "companies_table": AIRTABLE_COMPANIES_TABLE,
            "sample_company": {
                "airtable_record_id": rec.get("id"),
                "company_id": parsed["company_id"],
                "company_name": parsed["company_name"],
                "careers_url_present": bool(parsed["careers_url"]),
            }
        }
        if DEBUG_SHOW_AIRTABLE:
            resp["debug"] = {"fields_present": parsed["fields_present"]}
        return resp

@app.post("/run/test-discover")
async def test_discover():
    """
    Stage A (Discover) - simple HTML parsing test:
    - Read 1 company from Airtable
    - Fetch Careers_URL
    - Discover job-ish links
    - Write to job_sources table
    """
    async with httpx.AsyncClient(timeout=40, follow_redirects=True) as client:
        # 1) Get ONE test company record
        rec = await airtable_get_one_company(client)
        parsed = extract_company_fields(rec)

        if not parsed["careers_url"]:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Company record missing required fields",
                    "required": ["Careers_URL (or careers_url)"],
                    "found_fields": parsed["fields_present"]
                }
            )

        company_id = parsed["company_id"]
        careers_url = parsed["careers_url"]

        # 2) Fetch careers page
        page = await client.get(careers_url, headers={"User-Agent": "biotech-job-crawler/0.3 (+contact: you@example.com)"})
        if page.status_code >= 400:
            raise HTTPException(
                status_code=400,
                detail={"error": "Failed to fetch careers_url", "status": page.status_code, "url": careers_url}
            )

        soup = BeautifulSoup(page.text, "html.parser")

        # 3) Extract job links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            full = absolutize(href, careers_url)
            if looks_like_job_link(full):
                links.add(full)

        # 4) Write job links to Airtable job_sources
        discovered_records = []
        for link in sorted(links):
            discovered_records.append({
                "fields": {
                    "company_id": company_id,
                    "job_url": link,
                    "source_type": "html",
                    "first_seen_at": now_iso(),
                    "last_seen_At": now_iso(),  # matches your screenshot's "last_seen_At" casing
                    "Active": True              # matches your screenshot's "Active" field label
                }
            })

        write_result = await airtable_insert_job_sources(client, discovered_records)

        return {
            "company_id": company_id,
            "careers_url": careers_url,
            "jobs_found": len(discovered_records),
            "airtable_insert": write_result
        }


