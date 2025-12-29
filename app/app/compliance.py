import time
import httpx
from urllib.parse import urlparse
from dataclasses import dataclass

@dataclass
class RobotsPolicy:
    allowed: bool
    crawl_delay: float | None

class Compliance:
    def __init__(self, user_agent: str, default_delay: float, allow_domains: set[str], deny_domains: set[str]):
        self.user_agent = user_agent
        self.default_delay = default_delay
        self.allow_domains = allow_domains
        self.deny_domains = deny_domains
        self._last_request_at: dict[str, float] = {}
        self._robots_cache: dict[str, RobotsPolicy] = {}

    def _domain(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    async def robots_policy(self, client: httpx.AsyncClient, base_url: str) -> RobotsPolicy:
        domain = self._domain(base_url)
        if domain in self._robots_cache:
            return self._robots_cache[domain]

        robots_url = f"{urlparse(base_url).scheme}://{domain}/robots.txt"
        try:
            r = await client.get(robots_url, timeout=10)
            txt = r.text if r.status_code == 200 else ""
        except Exception:
            txt = ""

        # Minimal robots parsing (good starter). Upgrade later if needed.
        # If you want strict parsing, swap to: robotexclusionrulesparser or reppy.
        allowed = True
        crawl_delay = None

        ua_block = False
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, val = line.partition(":")
            key = key.strip().lower()
            val = val.strip()

            if key == "user-agent":
                ua_block = (val == "*" or val.lower() in self.user_agent.lower())
            elif ua_block and key == "disallow":
                # If they disallow everything, treat as forbidden for simplicity.
                if val == "/" or val == "/*":
                    allowed = False
            elif ua_block and key == "crawl-delay":
                try:
                    crawl_delay = float(val)
                except ValueError:
                    pass

        pol = RobotsPolicy(allowed=allowed, crawl_delay=crawl_delay)
        self._robots_cache[domain] = pol
        return pol

    async def wait_rate_limit(self, url: str, crawl_delay: float | None):
        domain = self._domain(url)
        delay = crawl_delay if crawl_delay is not None else self.default_delay
        last = self._last_request_at.get(domain)
        if last is not None:
            to_wait = delay - (time.time() - last)
            if to_wait > 0:
                time.sleep(to_wait)
        self._last_request_at[domain] = time.time()

    def domain_allowed(self, url: str) -> bool:
        domain = self._domain(url)
        if domain in self.deny_domains:
            return False
        if self.allow_domains and domain not in self.allow_domains:
            return False
        return True
