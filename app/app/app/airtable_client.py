import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

class Airtable:
    def __init__(self, pat: str, base_id: str):
        self.base_id = base_id
        self.headers = {
            "Authorization": f"Bearer {pat}",
            "Content-Type": "application/json",
        }

    def _url(self, table: str) -> str:
        return f"https://api.airtable.com/v0/{self.base_id}/{table}"

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=20))
    async def create_records(self, client: httpx.AsyncClient, table: str, records: list[dict]):
        r = await client.post(self._url(table), headers=self.headers, json={"records": records})
        r.raise_for_status()
        return r.json()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=20))
    async def update_records(self, client: httpx.AsyncClient, table: str, records: list[dict]):
        r = await client.patch(self._url(table), headers=self.headers, json={"records": records})
        r.raise_for_status()
        return r.json()

    async def list_records(self, client: httpx.AsyncClient, table: str, formula: str | None = None, page_size: int = 100):
        params = {"pageSize": page_size}
        if formula:
            params["filterByFormula"] = formula
        out = []
        offset = None
        while True:
            if offset:
                params["offset"] = offset
            r = await client.get(self._url(table), headers=self.headers, params=params)
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        return out
