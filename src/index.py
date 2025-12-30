from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin
import os
import re

app = FastAPI(title="Biotech Job Crawler", version="0.1.0")

# -----------------------------
# Environment / Airtable config
# -----------------------------
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")
AIRTABLE_JOBS_TABLE = os.getenv("AIRTABLE_JOBS_TABLE", "jobs")
AIRTABLE_OUTREACH_TABLE = os.getenv("AIRTABLE_OUTREACH_TABLE", "bd_outreach_queue")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

AIRTABLE_API_ROOT = "https://api.airtable.com/v0"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def airtable_headers():
    if not AIRTABLE_PAT:
        raise HTTPException(status_code=500, detail={"error": "Missing AIRTABLE_PAT in environment"})
    return {
        "Authorization": f"Bearer {AIRTABLE_PAT}",
        "Content-Type": "application/json",
    }

def require_airtable_env():
    missing = []
    if not AIRTABLE_PAT: missing.append("AIRTABLE_PAT")
    if not AIRTABLE_BASE_ID: missing.append("AIRTABLE_BASE_ID")
    if missing:
        raise HTTPException(
            status_code=500,
            detail={"error": "Missing required environment variables", "missing": missing},
        )

async def airtable_get(client: httpx.AsyncClient, table: str, params: dict | None = None):
    require_airtable_env()
    url = f"{AIRTABLE_API_ROOT}/{AIRTABLE_BASE_ID}/{table}"
    r = await client.get(url, headers=airtable_headers(), params=params)
    if r.status_code == 401:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable auth failed (401)",
                "hint": "Check AIRTABLE_PAT is correct AND token has access to this base",
                "base_id": AIRTABLE_BASE_ID,
                "table": table,
                "debug": {"pat_present": bool(AIRTABLE_PAT)} if DEBUG_SHOW_AIRTABLE else None,
            },
        )
    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={"error": "Airtable request failed", "status": r.status_code, "body": r.text},
        )
    return r.json()

async def airtable_post_records(client: httpx.AsyncClient, table: str, records: list[dict]):
    require_airtable_env()
    url = f"{AIRTABLE_API_ROOT}/{AIRTABLE_BASE_ID}/{table}"
    r = await client.post(url, headers=airtable_headers(), json={"records": records})
    if r.status_code == 401:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable write auth failed (401)",
                "hint": "Token must have data.records:write and access to base",
                "base_id": AIRTABLE_BASE_ID,
                "table": table,
            },
        )
    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={"error": "Airtable write failed", "status": r.status_code, "body": r.text},
        )
    return r.json()

# -----------------------------
# Basic endpoints
# -----------------------------
@app.get("/")
def root():
    return {
        "service": "biotech-job-crawler",
        "ok": True,
        "routes": ["/health", "/version", "/run/airtable-test", "/run/test-discover"],
    }

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {"version": app.version, "time_utc": now_iso()}

# -----------------------------
# Airtable test endpoint
# -----------------------------
@app.get("/run/airtable-test")
async def airtable_test():
    """
    Confirms:
    - env vars exist
    - Airtable auth works
    - we can read 1 record from target_company_registry
    """
    require_airtable_env()
    async with httpx.AsyncClient(timeout=30) as client:
        data = await airtable_get(
            client,
            AIRTABLE_COMPANIES_TABLE,
            params={"maxRecords": 1},
        )

    count = len(data.get("records", []))
    sample_fields = None
    if count:
        sample_fields = list((data["records"][0].get("fields") or {}).keys())

    resp = {
        "ok": True,
        "base_id": AIRTABLE_BASE_ID,
        "companies_table": AIRTABLE_COMPANIES_TABLE,
        "records_returned": count,
        "sample_field_names": sample_fields,
    }
    if DEBUG_SHOW_AIRTABLE:
        resp["debug"] = {
            "pat_present": bool(AIRTABLE_PAT),
            "pat_prefix": (AIRTABLE_PAT[:6] + "…") if AIRTABLE_PAT else None,
        }
    return resp

# -----------------------------
# Stage A: discover job URLs (simple HTML starter)
# -----------------------------
JOBISH_RE = re.compile(r"(job|career|opening|position|opportunit|apply)", re.IGNORECASE)

@app.post("/run/test-discover")
async def test_discover():
    """
    Pulls 1 company from Airtable, fetches its careers_url, discovers job-ish links,
    writes them to job_sources.
    """
    require_airtable_env()

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Get ONE company (must have fields: company_id, careers_url)
        data = await airtable_get(
            client,
            AIRTABLE_COMPANIES_TABLE,
            params={"maxRecords": 1},
        )

        records = data.get("records", [])
        if not records:
            raise HTTPException(status_code=400, detail={"error": "No companies found in Airtable table"})

        fields = records[0].get("fields") or {}
        company_id = fields.get("company_id")
        careers_url = fields.get("Careers_URL") or fields.get("careers_url")  # supports either naming

        if not company_id or not careers_url:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Company record missing required fields",
                    "required": ["company_id", "careers_url (or Careers_URL)"],
                    "found_fields": list(fields.keys()),
                },
            )

        # 2) Fetch careers page
        page = await client.get(careers_url)
        if page.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={"error": "Careers page fetch failed", "status": page.status_code, "url": careers_url},
            )

        soup = BeautifulSoup(page.text, "html.parser")

        # 3) Extract job-ish links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue

            # absolute URL
            abs_url = urljoin(careers_url, href)

            # heuristic filter
            text = (a.get_text(" ", strip=True) or "")
            if JOBISH_RE.search(abs_url) or JOBISH_RE.search(text):
                links.add(abs_url)

        # 4) Write to Airtable job_sources
        now = now_iso()
        out_records = []
        for link in sorted(list(links))[:50]:  # cap so you don't blast Airtable
            out_records.append({
                "fields": {
                    "company_id": company_id,
                    "job_url": link,
                    "source_type": "html",
                    "first_seen_at": now,
                    "last_seen_At": now,   # supports your screenshot naming
                    "Active": True,        # supports your screenshot naming
                }
            })

        wrote = 0
        if out_records:
            # Airtable batch limit is 10 per request; chunk it
            for i in range(0, len(out_records), 10):
                chunk = out_records[i:i+10]
                await airtable_post_records(client, AIRTABLE_JOB_SOURCES_TABLE, chunk)
                wrote += len(chunk)

        return {
            "ok": True,
            "company_id": company_id,
            "careers_url": careers_url,
            "links_found": len(links),
            "records_written": wrote,
            "note": "This is a simple HTML discovery. ATS-specific discovery comes next.",
        }


