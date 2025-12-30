from fastapi import FastAPI, HTTPException
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
import asyncio
from urllib.parse import urljoin, quote

# ----------------------------
# App
# ----------------------------
app = FastAPI(title="Biotech Job Crawler")

# ----------------------------
# Env vars (match your Railway Variables)
# ----------------------------
AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "target_company_registry")
AIRTABLE_JOB_SOURCES_TABLE = os.getenv("AIRTABLE_JOB_SOURCES_TABLE", "job_sources")
AIRTABLE_JOBS_TABLE = os.getenv("AIRTABLE_JOBS_TABLE", "jobs")
AIRTABLE_OUTREACH_TABLE = os.getenv("AIRTABLE_OUTREACH_TABLE", "bd_outreach_queue")

DEBUG_SHOW_AIRTABLE = os.getenv("DEBUG_SHOW_AIRTABLE", "0") == "1"

if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
    # Don't crash import-time in Railway in case you deploy before adding vars
    # We'll raise a clean error at runtime when an endpoint needs Airtable.
    pass

AIRTABLE_HEADERS = lambda: {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type": "application/json",
}

AIRTABLE_API_ROOT = "https://api.airtable.com/v0"


# ----------------------------
# Compliance layer (simple, effective starter)
# ----------------------------
DOMAIN_LAST_HIT = {}
DOMAIN_LOCK = asyncio.Lock()

DEFAULT_CRAWL_DELAY_SECONDS = 7  # 1 request per ~7 seconds per domain


async def rate_limit(domain: str, crawl_delay: int = DEFAULT_CRAWL_DELAY_SECONDS):
    """Enforce per-domain delay."""
    async with DOMAIN_LOCK:
        last = DOMAIN_LAST_HIT.get(domain)
        now = asyncio.get_event_loop().time()
        if last is not None:
            wait_for = (last + crawl_delay) - now
            if wait_for > 0:
                await asyncio.sleep(wait_for)
        DOMAIN_LAST_HIT[domain] = asyncio.get_event_loop().time()


def parse_domain(url: str) -> str:
    # minimal parse without extra deps
    try:
        return url.split("//", 1)[1].split("/", 1)[0].lower()
    except Exception:
        return ""


async def fetch_robots_rules(client: httpx.AsyncClient, base_url: str, user_agent: str):
    """
    Minimal robots.txt checker.
    Returns: dict { 'disallow': [paths], 'crawl_delay': int|None }
    """
    domain = parse_domain(base_url)
    robots_url = f"https://{domain}/robots.txt"
    rules = {"disallow": [], "crawl_delay": None}

    try:
        await rate_limit(domain)
        r = await client.get(robots_url, headers={"User-Agent": user_agent})
        if r.status_code != 200:
            return rules

        lines = r.text.splitlines()
        active = False

        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            low = line.lower()
            if low.startswith("user-agent:"):
                ua = line.split(":", 1)[1].strip()
                active = (ua == "*" or ua.lower() in user_agent.lower())
                continue

            if active and low.startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    rules["disallow"].append(path)
                continue

            if active and low.startswith("crawl-delay:"):
                val = line.split(":", 1)[1].strip()
                try:
                    rules["crawl_delay"] = int(float(val))
                except Exception:
                    pass

        return rules
    except Exception:
        return rules


def is_allowed_by_robots(target_url: str, robots_rules: dict) -> bool:
    """
    Minimal: if any Disallow prefix matches the path, block it.
    """
    try:
        # path starts after domain
        path = "/" + target_url.split("//", 1)[1].split("/", 1)[1]
    except Exception:
        path = "/"

    for dis in robots_rules.get("disallow", []):
        if dis == "/":
            return False
        if path.startswith(dis):
            return False
    return True


# ----------------------------
# Airtable helpers
# ----------------------------
def require_airtable():
    if not AIRTABLE_PAT or not AIRTABLE_BASE_ID:
        raise HTTPException(
            status_code=500,
            detail="Missing AIRTABLE_PAT or AIRTABLE_BASE_ID in Railway Variables",
        )


def airtable_table_url(table_name: str) -> str:
    # Airtable needs URL encoding for table names
    return f"{AIRTABLE_API_ROOT}/{AIRTABLE_BASE_ID}/{quote(table_name, safe='')}"


async def airtable_get_one_company(client: httpx.AsyncClient):
    """
    Pull 1 company record that is Allowed = 1 (checkbox checked) and has a Careers_URL.
    Adjust field names here to match your Airtable column names.
    """
    require_airtable()

    # These MUST match your Airtable field names EXACTLY
    # From your screenshot: "Careers_URL" and "Allowed"
    formula = "AND({Allowed}=TRUE(), {Careers_URL}!='')"

    url = airtable_table_url(AIRTABLE_COMPANIES_TABLE)
    params = {
        "maxRecords": 1,
        "filterByFormula": formula,
    }

    r = await client.get(url, headers=AIRTABLE_HEADERS(), params=params)
    if r.status_code == 401:
        raise HTTPException(
            status_code=500,
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
            detail={"error": "Airtable read failed", "status": r.status_code, "body": r.text},
        )

    data = r.json()
    records = data.get("records", [])
    if not records:
        raise HTTPException(
            status_code=404,
            detail="No Allowed companies with Careers_URL found in target_company_registry",
        )

    rec = records[0]
    fields = rec.get("fields", {})
    return rec.get("id"), fields


async def airtable_create_job_sources(client: httpx.AsyncClient, rows: list[dict]):
    """
    Airtable create limit is 10 records per request.
    """
    require_airtable()

    if not rows:
        return {"created": 0}

    url = airtable_table_url(AIRTABLE_JOB_SOURCES_TABLE)
    created_total = 0

    # chunk by 10
    for i in range(0, len(rows), 10):
        chunk = rows[i : i + 10]
        payload = {"records": [{"fields": row} for row in chunk]}
        r = await client.post(url, headers=AIRTABLE_HEADERS(), json=payload)
        if r.status_code == 401:
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "Airtable auth failed (401) on write",
                    "hint": "Token needs data.records:write + base access",
                    "table": AIRTABLE_JOB_SOURCES_TABLE,
                },
            )
        if r.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={"error": "Airtable write failed", "status": r.status_code, "body": r.text},
            )
        created_total += len(chunk)

    return {"created": created_total}


# ----------------------------
# Routes
# ----------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "biotech-job-crawler", "docs": "/docs"}


@app.get("/health")
def health():
    # Helpful to confirm env vars exist without exposing secrets
    token_ok = bool(AIRTABLE_PAT)
    base_ok = bool(AIRTABLE_BASE_ID)
    return {
        "ok": True,
        "airtable_pat_present": token_ok,
        "airtable_base_present": base_ok,
        "tables": {
            "companies": AIRTABLE_COMPANIES_TABLE,
            "job_sources": AIRTABLE_JOB_SOURCES_TABLE,
            "jobs": AIRTABLE_JOBS_TABLE,
            "outreach": AIRTABLE_OUTREACH_TABLE,
        },
    }


@app.post("/run/test-discover")
async def run_test_discover():
    """
    Stage A (Discover):
    - Pull 1 Allowed company from Airtable
    - Fetch Careers_URL (HTML)
    - Extract links that look like job postings
    - Write discovered URLs into job_sources
    """
    require_airtable()

    user_agent = "BiotechJobCrawler/1.0 (+contact: your-email@example.com)"
    now_iso = datetime.now(timezone.utc).isoformat()

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        # 1) Get one test company
        record_id, fields = await airtable_get_one_company(client)

        company_name = fields.get("Company Name") or fields.get("Company") or fields.get("Company_Name")
        careers_url = fields.get("Careers_URL")  # from your screenshot
        website = fields.get("Website")

        # If you store a company_id field use it, else fall back to Airtable record id
        company_id = fields.get("company_id") or record_id

        if not careers_url:
            raise HTTPException(status_code=400, detail="Company record missing Careers_URL field")

        # 2) Compliance: robots.txt + rate limit
        domain = parse_domain(careers_url)
        robots = await fetch_robots_rules(client, careers_url, user_agent=user_agent)
        crawl_delay = robots.get("crawl_delay") or DEFAULT_CRAWL_DELAY_SECONDS

        if not is_allowed_by_robots(careers_url, robots):
            return {
                "company_id": company_id,
                "company_name": company_name,
                "careers_url": careers_url,
                "skipped": True,
                "reason": "Blocked by robots.txt",
                "robots_disallow": robots.get("disallow", []),
            }

        await rate_limit(domain, crawl_delay=crawl_delay)

        # 3) Fetch careers page
        page = await client.get(careers_url, headers={"User-Agent": user_agent})
        if page.status_code >= 400:
            raise HTTPException(
                status_code=500,
                detail={"error": "Failed to fetch careers page", "status": page.status_code, "url": careers_url},
            )

        soup = BeautifulSoup(page.text, "html.parser")

        # 4) Extract job-ish links
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue

            # common job keywords
            low = href.lower()
            if any(k in low for k in ["job", "jobs", "career", "careers", "positions", "opening", "opportunity"]):
                abs_url = urljoin(careers_url, href)
                links.add(abs_url)

        # keep it small for test so you don't spam Airtable
        links = list(links)[:25]

        # 5) Write job sources to Airtable
        rows = []
        for link in links:
            rows.append(
                {
                    "company_id": company_id,
                    "job_url": link,
                    "first_seen_at": now_iso,
                    "last_seen_at": now_iso,
                    "active": True,
                    "source_type": "html",
                }
            )

        result = await airtable_create_job_sources(client, rows)

        # Optional debug info
        debug = None
        if DEBUG_SHOW_AIRTABLE:
            debug = {
                "airtable_base_id": AIRTABLE_BASE_ID,
                "companies_table": AIRTABLE_COMPANIES_TABLE,
                "job_sources_table": AIRTABLE_JOB_SOURCES_TABLE,
                "pat_prefix": (AIRTABLE_PAT[:6] + "..." if AIRTABLE_PAT else None),
            }

        return {
            "company_id": company_id,
            "company_name": company_name,
            "website": website,
            "careers_url": careers_url,
            "domain": domain,
            "robots_crawl_delay": crawl_delay,
            "jobs_found": len(rows),
            "airtable_written": result,
            "debug": debug,
        }

