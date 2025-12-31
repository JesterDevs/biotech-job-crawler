import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, quote

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException

# -----------------------------
# App
# -----------------------------
app = FastAPI(title="Biotech Job Crawler", version="0.1.0")

# -----------------------------
# Env / Config
# -----------------------------
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")  # Personal access token (pat...)
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")  # app...
AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")
DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

# Optional (helps you verify schema via /meta endpoints)
# Requires scopes: schema.bases:read
AIRTABLE_SCHEMA_SCOPE_ENABLED = True

AIRTABLE_API = "https://api.airtable.com/v0"
AIRTABLE_META_API = "https://api.airtable.com/v0/meta"

USER_AGENT = os.getenv(
    "CRAWLER_USER_AGENT",
    "BioJobsOutreachCrawler/0.1 (+contact: you@example.com)"
)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def mask(s: Optional[str], keep: int = 4) -> Optional[str]:
    if not s:
        return None
    if len(s) <= keep:
        return "*" * len(s)
    return "*" * (len(s) - keep) + s[-keep:]

def require_env() -> None:
    missing = []
    if not AIRTABLE_PAT:
        missing.append("AIRTABLE_PAT")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

def airtable_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AIRTABLE_PAT}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

def airtable_table_url(table: str) -> str:
    # Table can be a name like "job_sources" or a tableId like "tblXXXX"
    # Must be URL encoded.
    return f"{AIRTABLE_API}/{AIRTABLE_BASE_ID}/{quote(table, safe='')}"

def airtable_meta_tables_url() -> str:
    return f"{AIRTABLE_META_API}/bases/{AIRTABLE_BASE_ID}/tables"

async def airtable_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    json: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> httpx.Response:
    r = await client.request(method, url, headers=airtable_headers(), json=json, params=params)
    return r

async def airtable_raise(r: httpx.Response, context: Dict[str, Any]) -> None:
    if 200 <= r.status_code < 300:
        return

    body_text = r.text
    detail = {
        "error": "Airtable request failed",
        "status": r.status_code,
        "context": context,
        "body": body_text[:2000],
        "debug": {
            "pat_present": bool(AIRTABLE_PAT),
            "pat_masked": mask(AIRTABLE_PAT, keep=6) if DEBUG_SHOW_AIRTABLE else None,
            "base_id": AIRTABLE_BASE_ID,
        }
    }

    # Friendlier hints for the common failures
    if r.status_code in (401, 403):
        detail["hint"] = (
            "This is almost always one of: "
            "(1) wrong PAT, "
            "(2) PAT doesn't have access to this base, "
            "(3) wrong table name/tableId, "
            "(4) missing data.records:write scope for writes."
        )
        detail["tables_used"] = {
            "companies": AIRTABLE_COMPANIES_TABLE,
            "job_sources": AIRTABLE_JOB_SOURCES_TABLE,
        }

    raise HTTPException(status_code=500, detail=detail)

# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "biotech-job-crawler", "time": now_iso()}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {"version": app.version, "time": now_iso()}

@app.get("/run/airtable-test")
async def airtable_test():
    """
    Confirms:
      - token works
      - base is correct
      - can read 1 record from companies table
    """
    require_env()
    async with httpx.AsyncClient(timeout=30) as client:
        url = airtable_table_url(AIRTABLE_COMPANIES_TABLE)
        r = await airtable_request(client, "GET", url, params={"maxRecords": 1})
        await airtable_raise(r, {"op": "read_sample_company", "table": AIRTABLE_COMPANIES_TABLE})

        data = r.json()
        records = data.get("records", [])
        sample = records[0] if records else None

        return {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "companies_table": AIRTABLE_COMPANIES_TABLE,
            "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
            "sample_company": {
                "airtable_record_id": sample.get("id") if sample else None,
                "company_name": (sample.get("fields", {}).get("Company Name") if sample else None),
                "careers_url_present": bool(sample and sample.get("fields", {}).get("Careers_URL")),
            },
            "debug": {
                "pat_present": bool(AIRTABLE_PAT),
                "pat_masked": mask(AIRTABLE_PAT, keep=6) if DEBUG_SHOW_AIRTABLE else None,
                "fields_present": list(sample.get("fields", {}).keys()) if sample else [],
            }
        }

@app.get("/run/schema")
async def schema():
    """
    Lists tables + fields Airtable thinks exist in this base.
    Requires token scope: schema.bases:read
    This endpoint is your BEST friend for fixing the 'wrong table' 403.
    """
    require_env()
    if not AIRTABLE_SCHEMA_SCOPE_ENABLED:
        raise HTTPException(status_code=400, detail="Schema endpoint disabled")

    async with httpx.AsyncClient(timeout=30) as client:
        url = airtable_meta_tables_url()
        r = await airtable_request(client, "GET", url)
        await airtable_raise(r, {"op": "meta_tables_list"})

        meta = r.json()
        tables = meta.get("tables", [])
        trimmed = []
        for t in tables:
            trimmed.append({
                "name": t.get("name"),
                "id": t.get("id"),
                "fields": [f.get("name") for f in t.get("fields", [])][:50],
            })

        return {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "tables": trimmed,
            "hint": "Confirm AIRTABLE_*_TABLE values match ONE of the table names or table IDs listed here."
        }

@app.post("/run/test-discover")
async def test_discover():
    """
    Stage A (simple HTML discover):
      - reads 1 company from target_company_registry
      - fetches Careers_URL
      - finds links containing 'job' (naive)
      - writes into job_sources table
    """
    require_env()

    async with httpx.AsyncClient(timeout=40, follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        # 1) Pull one company record
        companies_url = airtable_table_url(AIRTABLE_COMPANIES_TABLE)
        r = await airtable_request(client, "GET", companies_url, params={"maxRecords": 1})
        await airtable_raise(r, {"op": "read_company", "table": AIRTABLE_COMPANIES_TABLE})

        data = r.json()
        records = data.get("records", [])
        if not records:
            raise HTTPException(status_code=400, detail="No records found in companies table")

        rec = records[0]
        fields = rec.get("fields", {})
        company_record_id = rec.get("id")  # IMPORTANT: use Airtable record id as stable company_id

        # Your Airtable field is "Careers_URL" (from your screenshot)
        careers_url = fields.get("Careers_URL")
        company_name = fields.get("Company Name")

        if not careers_url:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Company record missing Careers_URL",
                    "company_record_id": company_record_id,
                    "company_name": company_name,
                    "found_fields": list(fields.keys()),
                },
            )

        # 2) Fetch careers page
        page = await client.get(careers_url)
        if page.status_code >= 400:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Careers page fetch failed",
                    "status": page.status_code,
                    "careers_url": careers_url,
                },
            )

        soup = BeautifulSoup(page.text, "html.parser")

        # 3) Discover job-ish links (naive rule; replace later with ATS parsers)
        links: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href:
                continue
            # skip mailto/tel
            if href.startswith("mailto:") or href.startswith("tel:"):
                continue
            # only job-ish paths
            if "job" not in href.lower():
                continue

            absolute = href
            if href.startswith("/"):
                absolute = urljoin(careers_url, href)
            elif href.startswith("http://") or href.startswith("https://"):
                absolute = href
            else:
                absolute = urljoin(careers_url.rstrip("/") + "/", href)

            links.add(absolute)

        # cap so you don’t accidentally write 500 links in a test
        discovered = sorted(list(links))[:50]

        # 4) Write into job_sources table
        job_sources_url = airtable_table_url(AIRTABLE_JOB_SOURCES_TABLE)

        payload_records = []
        for link in discovered:
            payload_records.append({
                "fields": {
                    "company_id": company_record_id,  # matches your job_sources column
                    "job_url": link,
                    "source_type": "html",
                    "first_seen_at": now_iso(),
                    "last_seen_At": now_iso(),  # note your screenshot shows last_seen_At (capital A)
                    "Active": True,              # your field is "Active" (capital A)
                }
            })

        if payload_records:
            wr = await airtable_request(client, "POST", job_sources_url, json={"records": payload_records})
            await airtable_raise(wr, {"op": "write_job_sources", "table": AIRTABLE_JOB_SOURCES_TABLE})

        return {
            "ok": True,
            "company_record_id": company_record_id,
            "company_name": company_name,
            "careers_url": careers_url,
            "jobs_found": len(payload_records),
            "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
        }


