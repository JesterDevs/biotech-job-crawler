import os
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Biotech Job Crawler")

# ---- Env ----
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    # Don't crash the container at import time; expose a helpful endpoint instead.
    pass


# ---- Helpers ----
def airtable_headers():
    if not AIRTABLE_PAT:
        raise HTTPException(status_code=500, detail={"error": "Missing AIRTABLE_PAT"})
    return {
        "Authorization": f"Bearer {AIRTABLE_PAT}",
        "Content-Type": "application/json",
    }


def airtable_table_url(table: str) -> str:
    # table can be table name or table id
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"


def today_iso_date() -> str:
    # Airtable date fields are happiest with YYYY-MM-DD
    return datetime.now(timezone.utc).date().isoformat()


def normalize_job_link(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def same_domain(a: str, b: str) -> bool:
    try:
        return urlparse(a).netloc.lower() == urlparse(b).netloc.lower()
    except Exception:
        return False


@app.get("/")
def root():
    return {
        "ok": True,
        "message": "Crawler is running. Try /docs or /health",
    }


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/run/airtable-test")
async def airtable_test():
    """
    Validates Airtable auth + table access and returns one sample company.
    """
    if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
        raise HTTPException(
            status_code=500,
            detail={"error": "Missing AIRTABLE_PAT or AIRTABLE_BASE_ID"},
        )

    async with httpx.AsyncClient(timeout=25) as client:
        # Read 1 record from companies table
        r = await client.get(
            airtable_table_url(AIRTABLE_COMPANIES_TABLE) + "?maxRecords=1",
            headers=airtable_headers(),
        )
        if r.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": f"Airtable read failed ({r.status_code})",
                    "body": r.text,
                    "base_id": AIRTABLE_BASE_ID,
                    "companies_table": AIRTABLE_COMPANIES_TABLE,
                },
            )
        data = r.json()
        rec = (data.get("records") or [None])[0]
        if not rec:
            return {
                "ok": True,
                "base_id": AIRTABLE_BASE_ID,
                "companies_table": AIRTABLE_COMPANIES_TABLE,
                "note": "No company records found (table empty).",
            }

        fields = rec.get("fields", {})
        return {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "env": {
                "companies_table": AIRTABLE_COMPANIES_TABLE,
                "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
                "pat_present": True,
            },
            "sample_company": {
                "airtable_record_id": rec.get("id"),
                "company_name": fields.get("Company Name"),
                "careers_url_present": bool(fields.get("Careers_URL")),
                "allowed": fields.get("Allowed"),
            },
            "debug": {
                "fields_present": list(fields.keys()) if DEBUG_SHOW_AIRTABLE else "set DEBUG_SHOW_AIRTABLE=1",
            },
        }


@app.post("/run/test-discover")
async def test_discover():
    """
    Stage A: discover job URLs for ONE company (maxRecords=1),
    then write to job_sources.
    """
    if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
        raise HTTPException(status_code=500, detail={"error": "Missing AIRTABLE_PAT or AIRTABLE_BASE_ID"})

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Get one test company
        r = await client.get(
            airtable_table_url(AIRTABLE_COMPANIES_TABLE) + "?maxRecords=1",
            headers=airtable_headers(),
        )
        if r.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={"error": "Airtable read failed", "status": r.status_code, "body": r.text},
            )

        data = r.json()
        rec = (data.get("records") or [None])[0]
        if not rec:
            raise HTTPException(status_code=400, detail={"error": "No company records found in companies table"})

        fields = rec.get("fields", {})
        airtable_record_id = rec.get("id")

        # Your table uses "Careers_URL" and does NOT have company_id,
        # so use Airtable record id as company_id.
        careers_url = fields.get("Careers_URL")
        allowed = fields.get("Allowed", True)

        if not careers_url:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Company record missing required fields",
                    "required": ["Careers_URL"],
                    "found_fields": list(fields.keys()),
                },
            )

        if allowed is False:
            return {
                "skipped": True,
                "reason": "Company not allowed for crawling (Allowed=false)",
                "company_airtable_record_id": airtable_record_id,
                "careers_url": careers_url,
            }

        # 2) Fetch careers page
        page = await client.get(careers_url, headers={"User-Agent": "biotech-job-crawler/1.0 (contact: you@example.com)"})
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
            # simple heuristic
            if "job" in href.lower() or "careers" in href.lower() or "positions" in href.lower():
                full = normalize_job_link(careers_url, href)
                # keep links that stay on same domain as careers_url (avoid random external links)
                if same_domain(full, careers_url):
                    links.add(full)

        links = sorted(list(links))

        # 4) Write to job_sources (batch <= 10 for safety)
        # IMPORTANT: your job_sources table fields are:
        # company_id, job_url, source_type, first_seen_at, last_seen_At, Active
        # and source_type choice includes "HTML" (uppercase).
        records = []
        for link in links[:10]:
            records.append(
                {
                    "fields": {
                        "company_id": airtable_record_id,
                        "job_url": link,
                        "source_type": "HTML",
                        "first_seen_at": today_iso_date(),
                        "last_seen_At": today_iso_date(),
                        "Active": True,
                    }
                }
            )

        if records:
            wr = await client.post(
                airtable_table_url(AIRTABLE_JOB_SOURCES_TABLE),
                headers=airtable_headers(),
                json={"records": records},
            )
            if wr.status_code >= 400:
                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": "Airtable write failed",
                        "status": wr.status_code,
                        "body": wr.text,
                        "base_id": AIRTABLE_BASE_ID,
                        "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
                    },
                )

        return {
            "ok": True,
            "company_airtable_record_id": airtable_record_id,
            "company_name": fields.get("Company Name"),
            "careers_url": careers_url,
            "jobs_found_on_page": len(links),
            "records_written": len(records),
            "note": "Wrote up to 10 discovered links to job_sources.",
        }

