from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
import re

app = FastAPI(title="Biotech Job Crawler", version="0.1.0")

# --------- ENV ---------
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT") or os.getenv("AIRTABLE_API_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    raise RuntimeError("Missing AIRTABLE_PAT (or AIRTABLE_API_TOKEN) or AIRTABLE_BASE_ID environment variables")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

AIRTABLE_API = "https://api.airtable.com/v0"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def normalize_job_url(base_url: str, href: str) -> str:
    href = href.strip()
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return base_url.rstrip("/") + href
    return base_url.rstrip("/") + "/" + href

def looks_like_job_link(url: str) -> bool:
    u = url.lower()
    # Keep this permissive for now; tighten later per ATS type
    return any(x in u for x in ["job", "careers", "positions", "opportunities", "opening", "requisition", "apply"])

async def airtable_get(client: httpx.AsyncClient, table: str, params=None):
    url = f"{AIRTABLE_API}/{AIRTABLE_BASE_ID}/{table}"
    r = await client.get(url, headers=AIRTABLE_HEADERS, params=params)
    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable read failed",
                "status": r.status_code,
                "table": table,
                "body": r.text,
            },
        )
    return r.json()

async def airtable_post_records(client: httpx.AsyncClient, table: str, records: list[dict]):
    url = f"{AIRTABLE_API}/{AIRTABLE_BASE_ID}/{table}"
    r = await client.post(url, headers=AIRTABLE_HEADERS, json={"records": records})
    if r.status_code >= 400:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Airtable write failed",
                "status": r.status_code,
                "table": table,
                "body": r.text,
                "base_id": AIRTABLE_BASE_ID,
            },
        )
    return r.json()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {
        "service": "biotech-job-crawler",
        "version": app.version,
        "time": utc_now_iso(),
    }

@app.get("/run/airtable-test")
async def airtable_test():
    """
    Verifies:
      - PAT works
      - can read companies table
      - returns sample company + field list
    """
    async with httpx.AsyncClient(timeout=30) as client:
        data = await airtable_get(
            client,
            AIRTABLE_COMPANIES_TABLE,
            params={"maxRecords": 1, "view": "Grid view"},
        )

        if not data.get("records"):
            return {
                "ok": True,
                "base_id": AIRTABLE_BASE_ID,
                "companies_table": AIRTABLE_COMPANIES_TABLE,
                "sample_company": None,
                "hint": "No records found in companies table.",
            }

        rec = data["records"][0]
        fields = list((rec.get("fields") or {}).keys())
        out = {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "companies_table": AIRTABLE_COMPANIES_TABLE,
            "sample_company": {
                "airtable_record_id": rec.get("id"),
                "company_name": (rec.get("fields") or {}).get("Company Name"),
                "careers_url_present": bool((rec.get("fields") or {}).get("Careers_URL")),
            },
        }
        if DEBUG_SHOW_AIRTABLE:
            out["debug"] = {"fields_present": fields}
        return out

@app.post("/run/airtable-write-test")
async def airtable_write_test():
    """
    Writes ONE test record into job_sources.
    If this fails with 403, your PAT is not allowed to write.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        test_record = {
            "fields": {
                "company_id": "TEST",
                "job_url": "https://example.com/test-job",
                "source_type": "test",
                "first_seen_at": utc_now_iso(),
                "last_seen_At": utc_now_iso(),
                "Active": True,
            }
        }
        result = await airtable_post_records(client, AIRTABLE_JOB_SOURCES_TABLE, [test_record])
        return {"ok": True, "written": True, "result": result}

@app.post("/run/test-discover")
async def test_discover():
    """
    Pulls 1 company from target_company_registry and discovers job-ish links
    from Careers_URL, then writes them to job_sources.
    """
    async with httpx.AsyncClient(timeout=40, follow_redirects=True) as client:
        companies = await airtable_get(
            client,
            AIRTABLE_COMPANIES_TABLE,
            params={"maxRecords": 1, "view": "Grid view"},
        )

        if not companies.get("records"):
            raise HTTPException(status_code=400, detail={"error": "No company records found"})

        rec = companies["records"][0]
        fields = rec.get("fields") or {}

        # We store the Airtable record id as our company_id for now (simple + reliable)
        company_id = rec.get("id")
        careers_url = fields.get("Careers_URL") or fields.get("careers_url")

        if not careers_url:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Company record missing Careers_URL",
                    "found_fields": list(fields.keys()),
                },
            )

        # Fetch careers page
        page = await client.get(careers_url)
        if page.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={"error": "Careers page fetch failed", "status": page.status_code, "url": careers_url},
            )

        soup = BeautifulSoup(page.text, "html.parser")

        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = normalize_job_url(careers_url, href)
            if looks_like_job_link(full):
                links.add(full)

        # Build Airtable records (limit per request is 10; keep it small for test)
        now = utc_now_iso()
        records = []
        for link in list(links)[:10]:
            records.append(
                {
                    "fields": {
                        "company_id": company_id,
                        "job_url": link,
                        "source_type": "html",
                        "first_seen_at": now,
                        "last_seen_At": now,
                        "Active": True,
                    }
                }
            )

        if not records:
            return {"ok": True, "company_id": company_id, "careers_url": careers_url, "jobs_found": 0, "written": 0}

        write_result = await airtable_post_records(client, AIRTABLE_JOB_SOURCES_TABLE, records)

        return {
            "ok": True,
            "company_id": company_id,
            "careers_url": careers_url,
            "jobs_found": len(records),
            "written": len(write_result.get("records", [])),
        }
