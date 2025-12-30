APP_VERSION = "2025-12-30-v7-airtable-test"

@app.get("/version")
def version():
    return {"version": APP_VERSION}
    
from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
import urllib.parse

app = FastAPI(title="Biotech Job Crawler", version="1.0.0")

# -----------------------------
# ENV / CONFIG
# -----------------------------
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT") or os.getenv("AIRTABLE_API_TOKEN") or os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")
AIRTABLE_JOBS_TABLE = os.getenv("AIRTABLE_JOBS_TABLE", "jobs")
AIRTABLE_OUTREACH_TABLE = os.getenv("AIRTABLE_OUTREACH_TABLE", "bd_outreach_queue")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    # Fail fast so you don't get confusing 500s later
    raise RuntimeError("Missing AIRTABLE_PAT (or AIRTABLE_API_TOKEN) and/or AIRTABLE_BASE_ID environment variables")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

AIRTABLE_BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"


def airtable_table_url(table_name: str) -> str:
    # Airtable requires URL encoding for table names
    return f"{AIRTABLE_BASE_URL}/{urllib.parse.quote(table_name, safe='')}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# -----------------------------
# BASIC ENDPOINTS
# -----------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "biotech-job-crawler", "docs": "/docs"}


@app.get("/health")
def health():
    return {"ok": True}


# -----------------------------
# DEBUG: TEST AIRTABLE AUTH + ACCESS
# -----------------------------
@app.get("/debug/airtable")
async def debug_airtable():
    """
    Confirms Airtable token/base/table access by attempting a read of 1 record.
    """
    url = airtable_table_url(AIRTABLE_COMPANIES_TABLE) + "?maxRecords=1"
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, headers=AIRTABLE_HEADERS)

    if r.status_code == 401:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "Airtable auth failed (401)",
                "hint": "Check AIRTABLE_PAT is correct AND token has access to this base",
                "base_id": AIRTABLE_BASE_ID,
                "table": AIRTABLE_COMPANIES_TABLE,
            },
        )

    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable read failed",
                "status": r.status_code,
                "body": r.text,
                "base_id": AIRTABLE_BASE_ID,
                "table": AIRTABLE_COMPANIES_TABLE,
            },
        )

    data = r.json()
    record_count = len(data.get("records", []))

    resp = {
        "ok": True,
        "base_id": AIRTABLE_BASE_ID,
        "companies_table": AIRTABLE_COMPANIES_TABLE,
        "records_returned": record_count,
    }

    if DEBUG_SHOW_AIRTABLE:
        # Be careful: don't print token unless you explicitly want it
        resp["debug"] = {
            "airtable_url": url,
            "auth_header_prefix": "Bearer " + (AIRTABLE_PAT[:6] + "..." if AIRTABLE_PAT else None),
        }

    return resp


# -----------------------------
# STAGE A (DISCOVER) TEST
# -----------------------------
@app.post("/run/test-discover")
async def test_discover():
    """
    Pull 1 allowed company from target_company_registry, fetch Careers_URL,
    discover likely job links, write into job_sources table.
    """

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Grab 1 allowed company
        # Your screenshot shows "Allowed" is a checkbox field.
        params = {
            "maxRecords": 1,
            "filterByFormula": "{Allowed}=TRUE()",
        }

        companies_url = airtable_table_url(AIRTABLE_COMPANIES_TABLE)

        r = await client.get(companies_url, headers=AIRTABLE_HEADERS, params=params)

        if r.status_code == 401:
            raise HTTPException(
                status_code=401,
                detail={
                    "error": "Airtable auth failed (401)",
                    "hint": "Token missing/invalid OR token doesn't have access to this base",
                    "base_id": AIRTABLE_BASE_ID,
                    "table": AIRTABLE_COMPANIES_TABLE,
                },
            )

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
        records = data.get("records", [])
        if not records:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "No allowed companies found",
                    "hint": "Make sure at least 1 company has Allowed checked AND Careers_URL filled in.",
                },
            )

        company_record = records[0]
        company_record_id = company_record["id"]
        fields = company_record.get("fields", {})

        # Use field names from your screenshot
        company_name = fields.get("Company Name") or fields.get("Company") or fields.get("Name") or "Unknown"
        careers_url = fields.get("Careers_URL") or fields.get("Careers URL") or fields.get("careers_url")

        if not careers_url:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Missing Careers_URL on the selected company record",
                    "company_name": company_name,
                    "company_record_id": company_record_id,
                    "hint": "Fill in the Careers_URL field in Airtable.",
                },
            )

        # 2) Fetch careers page
        page = await client.get(careers_url)
        if page.status_code >= 400:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Failed to fetch Careers_URL",
                    "status": page.status_code,
                    "careers_url": careers_url,
                },
            )

        soup = BeautifulSoup(page.text, "html.parser")

        # 3) Discover job-like links (simple heuristic)
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()

            # Ignore mailto/tel/javascript
            if href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
                continue

            # Basic job/career heuristics
            hlow = href.lower()
            if any(k in hlow for k in ["job", "jobs", "careers", "requisition", "opening", "apply"]):
                # Make absolute when relative
                if href.startswith("/"):
                    href = careers_url.rstrip("/") + href
                elif href.startswith("//"):
                    href = "https:" + href
                links.add(href)

        # Limit to avoid Airtable batch size issues + garbage links
        discovered = sorted(list(links))[:50]

        # 4) Write discovered links into job_sources table
        # Airtable batch limit is 10 records per request.
        now = utc_now_iso()

        payload_records = []
        for link in discovered:
            payload_records.append(
                {
                    "fields": {
                        # keep it simple: store company_record_id so you can join later
                        "company_record_id": company_record_id,
                        "company_name": company_name,
                        "job_url": link,
                        "source_type": "html",
                        "first_seen_at": now,
                        "last_seen_at": now,
                        "active": True,
                    }
                }
            )

        job_sources_url = airtable_table_url(AIRTABLE_JOB_SOURCES_TABLE)

        created = 0
        for i in range(0, len(payload_records), 10):
            batch = payload_records[i : i + 10]
            pr = await client.post(job_sources_url, headers=AIRTABLE_HEADERS, json={"records": batch})

            if pr.status_code == 401:
                raise HTTPException(
                    status_code=401,
                    detail={
                        "error": "Airtable auth failed writing job_sources (401)",
                        "hint": "Token may not have write scope or base access",
                        "table": AIRTABLE_JOB_SOURCES_TABLE,
                    },
                )

            if pr.status_code >= 400:
                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": "Airtable write failed",
                        "status": pr.status_code,
                        "body": pr.text,
                        "table": AIRTABLE_JOB_SOURCES_TABLE,
                    },
                )

            created += len(batch)

        return {
            "ok": True,
            "company_name": company_name,
            "company_record_id": company_record_id,
            "careers_url": careers_url,
            "jobs_found": len(discovered),
            "records_written_to_job_sources": created,
        }
@app.get("/debug/env/raw")
def debug_env_raw():
    return {
        "AIRTABLE_PAT": bool(os.getenv("AIRTABLE_PAT")),
        "AIRTABLE_PAT_TEST": bool(os.getenv("AIRTABLE_PAT_TEST")),
    }



