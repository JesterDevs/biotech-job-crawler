from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
import re
from urllib.parse import urljoin

app = FastAPI()

VERSION = "0.3.0"

# ----------------------------
# ENV / CONFIG
# ----------------------------
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")  # must be this name
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

# You can use either table *name* OR table *id* (tbl...)
AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    # Don't crash import-time; allow /health for debugging.
    # We'll enforce required vars inside endpoints that need Airtable.
    pass

AIRTABLE_HEADERS = lambda: {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

AIRTABLE_API_BASE = "https://api.airtable.com/v0"


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def require_airtable():
    if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Missing Airtable env vars",
                "required": ["AIRTABLE_PAT", "AIRTABLE_BASE_ID"],
                "debug": {
                    "AIRTABLE_PAT_present": bool(AIRTABLE_PAT),
                    "AIRTABLE_BASE_ID_present": bool(AIRTABLE_BASE_ID),
                },
            },
        )


async def airtable_request(client: httpx.AsyncClient, method: str, table: str, **kwargs):
    """
    Wrapper that gives nicer errors.
    table can be name or tblXXXXXXXXXXXX
    """
    url = f"{AIRTABLE_API_BASE}/{AIRTABLE_BASE_ID}/{table}"
    r = await client.request(method, url, headers=AIRTABLE_HEADERS(), **kwargs)
    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable request failed",
                "status": r.status_code,
                "url": url,
                "body": r.text,
                "debug": {"pat_present": bool(AIRTABLE_PAT)},
            },
        )
    return r.json()


# ----------------------------
# ROUTES
# ----------------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/version")
def version():
    return {"version": VERSION}


@app.get("/run/airtable-test")
async def airtable_test():
    """
    Confirms PAT works + lists tables visible via metadata endpoint.
    NOTE: metadata endpoint requires schema scope; if you don't have schema scope,
    we still do a record read test from companies table.
    """
    require_airtable()

    async with httpx.AsyncClient(timeout=30) as client:
        # 1) simple read test from companies table
        companies = await airtable_request(
            client,
            "GET",
            AIRTABLE_COMPANIES_TABLE,
            params={"maxRecords": 1},
        )

        sample = None
        if companies.get("records"):
            rec = companies["records"][0]
            fields = rec.get("fields", {})
            sample = {
                "airtable_record_id": rec.get("id"),
                "company_name": fields.get("Company Name") or fields.get("company_name"),
                "careers_url_present": bool(fields.get("Careers_URL") or fields.get("careers_url")),
            }

        # 2) list tables (requires schema.bases:read scope)
        tables_info = None
        meta_url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"
        meta_resp = await client.get(meta_url, headers=AIRTABLE_HEADERS())
        if meta_resp.status_code == 200:
            tables_info = meta_resp.json()
        else:
            tables_info = {
                "warning": "Meta tables endpoint not accessible (missing schema scope). This is OK.",
                "status": meta_resp.status_code,
                "body": meta_resp.text,
            }

        return {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "env": {
                "companies_table": AIRTABLE_COMPANIES_TABLE,
                "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
                "pat_present": bool(AIRTABLE_PAT),
            },
            "sample_company": sample,
            "tables": tables_info,
        }


@app.post("/run/test-discover")
async def test_discover(limit: int = 1):
    """
    Stage A: Discover job URLs
    - Pull up to `limit` companies where Allowed is checked (if field exists)
    - Fetch Careers_URL
    - Extract job-like links
    - Write to job_sources table
    """
    require_airtable()

    if limit < 1 or limit > 5:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 5 for test runs")

    async with httpx.AsyncClient(timeout=40, follow_redirects=True) as client:
        # Prefer Allowed = true if the field exists
        # Airtable formula: {Allowed}=TRUE()
        params = {"maxRecords": limit, "view": "Grid view"}
        # We'll try a filtered query; if that fails, fallback to maxRecords only.
        filtered_params = dict(params)
        filtered_params["filterByFormula"] = "{Allowed}=TRUE()"

        try:
            data = await airtable_request(client, "GET", AIRTABLE_COMPANIES_TABLE, params=filtered_params)
        except HTTPException:
            data = await airtable_request(client, "GET", AIRTABLE_COMPANIES_TABLE, params=params)

        records = data.get("records", [])
        if not records:
            raise HTTPException(status_code=400, detail="No company records found in target_company_registry")

        results = []

        for rec in records:
            rec_id = rec.get("id")
            fields = rec.get("fields", {})

            # Your real field names
            company_name = fields.get("Company Name") or fields.get("company_name") or rec_id
            careers_url = fields.get("Careers_URL") or fields.get("careers_url")

            if not careers_url:
                results.append(
                    {
                        "company": company_name,
                        "airtable_record_id": rec_id,
                        "error": "Missing Careers_URL",
                    }
                )
                continue

            # Fetch careers page
            page = await client.get(
                careers_url,
                headers={"User-Agent": "biotech-job-crawler/1.0 (contact: you@example.com)"},
            )

            # Basic HTML parse
            soup = BeautifulSoup(page.text, "html.parser")
            links = set()

            for a in soup.find_all("a", href=True):
                href = a["href"].strip()

                # Skip anchors/mailto/tel
                if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                    continue

                full = urljoin(careers_url, href)

                # Heuristic: keep URLs that look like job postings
                if re.search(r"(job|careers|positions|opportunit|opening|requisition|req)", full, re.IGNORECASE):
                    links.add(full)

            # Build Airtable create payload (Airtable limit: 10 records per request is safe)
            now = utc_now_iso()
            to_create = []
            for link in sorted(list(links))[:25]:  # cap for test
                to_create.append(
                    {
                        "fields": {
                            # job_sources table column is "company_id"
                            # We'll store the registry record id (stable unique ID)
                            "company_id": rec_id,
                            "job_url": link,
                            "source_type": "html",
                            "first_seen_at": now,
                            "last_seen_At": now,  # matches your field name
                            "Active": True,
                        }
                    }
                )

            created = 0
            if to_create:
                # Batch in chunks of 10
                for i in range(0, len(to_create), 10):
                    chunk = to_create[i : i + 10]
                    await airtable_request(
                        client,
                        "POST",
                        AIRTABLE_JOB_SOURCES_TABLE,
                        json={"records": chunk},
                    )
                    created += len(chunk)

            results.append(
                {
                    "company": company_name,
                    "airtable_record_id": rec_id,
                    "careers_url": careers_url,
                    "fetched_status": page.status_code,
                    "links_found": len(links),
                    "records_written": created,
                    "note": "If records_written is 0, the page may be JS-rendered or uses an ATS API endpoint.",
                }
            )

        return {"ok": True, "results": results}


