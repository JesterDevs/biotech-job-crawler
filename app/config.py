import os

def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v

AIRTABLE_PAT = env("AIRTABLE_PAT")
AIRTABLE_BASE_ID = env("AIRTABLE_BASE_ID")

TBL_TARGETS = env("AIRTABLE_TABLE_TARGETS", "target_companies")
TBL_JOB_SOURCES = env("AIRTABLE_TABLE_JOB_SOURCES", "job_sources")
TBL_JOBS = env("AIRTABLE_TABLE_JOBS", "jobs")
TBL_OUTREACH = env("AIRTABLE_TABLE_OUTREACH", "bd_outreach_queue")
TBL_LOGS = env("AIRTABLE_TABLE_CRAWL_LOGS", "crawl_logs")

CRAWLER_USER_AGENT = env("CRAWLER_USER_AGENT", "JessBDJobBot/1.0 (+contact: you@yourdomain.com)")
DEFAULT_DELAY_SECONDS = float(os.getenv("DEFAULT_DELAY_SECONDS", "7.0"))  # per-domain delay
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN", "250"))            # short bursts
