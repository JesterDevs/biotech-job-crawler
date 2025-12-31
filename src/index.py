import os
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Biotech Job Crawler", version="1.0.0")

# -----------------------------
# Env / Config
# -----------------------------
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT") or os.getenv("AIRTABLE_API_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")
AIRTABLE_JOBS_TABLE = os.getenv("AIRTABLE_JOBS_TABLE", "jobs")
AIRTABLE_OUTREACH_TABLE = os.getenv("AIRTABLE_OUTREACH_TABLE", "bd_outreach_queue")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    raise RuntimeError("Missing AIRTABLE_PAT (or AIRTABLE_API_TOKEN) or AIRTABLE_BASE_ID environment variables")

AIRTABLE_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

AIRTABLE_API = "https://api.airtable.com/v0"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def airtable_request(client: httpx.AsyncClient, method: str, table: str, *, params=None, json=None):
    """Wrapper that returns JSON or raises helpful FastAPI error."""
    url = f"{AIRTABLE_API}/{AIRTABLE_BASE_ID}/{table}"
    r = await client.request(method, url, headers=AIRTABLE_HEADERS, params=params, json=json)

    if r.status_code >= 400:
        detail = {
            "error": "Airtable request failed",
            "status": r.status_code,
            "base_id": AIRTABLE_BASE_ID,
            "table": table,
            "body": r.text[:2000],
        }
        raise HTTPException(status_code=500, detail=detail)

    return r.json()


def normalize_job_link(careers_url: str, href: str) -> str:
    return urljoin(careers_url, href)


def same_domain(a: str, b: str) -> bool:
    try:
        return urlparse(a).netloc.lower() == urlparse(b).netloc.lower()
    except Exception:
        return False


# -----------------------------
# Routes
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/run/airtable-test")
async def airtable_test():
    """
    Confirms we can read the companies table and shows what fields Airtable returns.
    """
    async with httpx.AsyncClient(timeout=25) as client:
        data = await airtable_request(
            client,
            "GET",
            AIRTABLE_COMPANIES_TABLE,
            params={"maxRecords": 1},
        )

        records = data.get("records", [])
        if not records:
            return {
                "ok": True,
                "note": "Companies table is reachable but has 0 records.",
                "base_id": AIRTABLE_BASE_ID,
                "companies_table": AIRTABLE_COMPANIES_TABLE,
            }

        rec = records[0]
        fields = rec.get("fields", {})

        return {
            "ok": True,
            "base_id": AIRTABLE_BASE_ID,
            "companies_table": AIRTABLE_COMPANIES_TABLE,
            "sample_company": {
                "airtable_record_id": rec.get("id"),
                "company_name": fields.get("Company Name"),
                "careers_url_present": bool(fields.get("Careers_URL") or fields.get("Careers URL") or fields.get("careers_url")),
            },
            "debug": {"fields_present": sorted(list(fields.keys()))} if DEBUG_SHOW_AIRTABLE else {"fields_present_count": len(fields)},
        }


@app.post("/run/test-discover")
async def test_discover():
    """
    Stage A (Discover): picks 1 company, fetches Careers_URL, finds job-ish links,
    writes them into job_sources.
    """
    async with httpx.AsyncClient(timeout=35, follow_redirects=True) as client:
        # 1) Get one company record
        data = await airtable_request(
            client,
            "GET",
            AIRTABLE_COMPANIES_TABLE,
            params={"maxRecords": 1},
        )

        records = data.get("records", [])
        if not records:
            raise HTTPException(status_code=400, detail="No companies found in target_company_registry")

        rec = records[0]
        fields = rec.get("fields", {})

        # Your table uses Airtable record id as company_id (good enough)
        company_id = rec.get("id")

        careers_url = (
            fields.get("Careers_URL")
            or fields.get("Careers URL")
            or fields.get("careers_url")
        )

        if not careers_url:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Company record missing required Careers_URL",
                    "company_record_id": company_id,
                    "found_fields": sorted(list(fields.keys())),
                },
            )

        # 2) Fetch careers page
        page = await client.get(careers_url, headers={"User-Agent": "biotech-job-crawler/1.0 (+contact: your-email)"})
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

            # Heuristic: links containing job/career/opening/position
            low = href.lower()
            if any(k in low for k in ["job", "career", "opening", "position", "requisition", "req"]):
                full = normalize_job_link(careers_url, href)

                # Optional: keep only same domain as careers site
                if same_domain(full, careers_url):
                    links.add(full)
                else:
                    # Still keep it if it's clearly an ATS domain (you can expand this list)
                    if any(x in urlparse(full).netloc.lower() for x in ["workday", "myworkdayjobs", "jobvite", "icims", "greenhouse", "lever", "adp"]):
                        links.add(full)

        links = sorted(list(links))

        # 4) Write to job_sources (batch 10 at a time)
        created = 0
        now = utc_now_iso()

        if links:
            chunks = [links[i:i+10] for i in range(0, len(links), 10)]
            for chunk in chunks:
                payload = {
                    "records": [
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
                        for link in chunk
                    ]
                }

                # This is where your 403 happens if AIRTABLE_JOB_SOURCES_TABLE is wrong
                await airtable_request(client, "POST", AIRTABLE_JOB_SOURCES_TABLE, json=payload)
                created += len(chunk)

        return {
            "company_record_id": company_id,
            "careers_url": careers_url,
            "links_found": len(links),
            "job_sources_created": created,
            "sample_links": links[:10],
            "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
        }


