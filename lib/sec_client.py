"""
Rate-limited SEC EDGAR client.

The sibling ``unstructured-data-2026`` module fetches HTTP with nothing but a
timeout — no User-Agent, no rate limiting, no retry. SEC's automated-access
rules require a descriptive User-Agent with a real contact and cap traffic at
10 requests/second, returning HTTP 429 when exceeded. This client adds:

- a token-bucket limiter (default 8 req/s, headroom under the 10 req/s cap),
- a mandatory ``User-Agent`` header,
- retry with exponential backoff + jitter on 429 / 5xx and connection errors.

It is a plain module with no Spark / Databricks imports so it can run on the
driver, inside a ``mapInPandas`` UDF on executors, and in unit tests.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field

import requests

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik10}.json"
COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
COMPANYCONCEPT_URL = (
    "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik10}/{taxonomy}/{concept}.json"
)
ARCHIVE_DOC_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_nodash}/{doc}"
)
ARCHIVE_SUBMISSION_TXT_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_nodash}.txt"
)
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

_RETRY_STATUS = {429, 500, 502, 503, 504}


def cik10(cik: str | int) -> str:
    """Zero-pad a CIK to the 10-digit form used by data.sec.gov."""
    return str(int(str(cik).lstrip("CIK").strip())).zfill(10)


def accession_nodash(accession: str) -> str:
    """0000320193-26-000064 -> 000032019326000064."""
    return accession.replace("-", "")


class _TokenBucket:
    """Simple thread-safe token bucket. ``rate`` tokens added per second, up to
    ``capacity``; ``take()`` blocks until a token is available."""

    def __init__(self, rate: float, capacity: float | None = None):
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else rate)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def take(self) -> None:
        with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self.capacity, self._tokens + (now - self._last) * self.rate
                )
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                sleep_for = (1.0 - self._tokens) / self.rate
                time.sleep(sleep_for)


@dataclass
class SecClient:
    user_agent: str
    requests_per_second: float = 8.0
    max_retries: int = 5
    timeout: int = 30
    backoff_base: float = 1.0
    backoff_cap: float = 30.0
    _bucket: _TokenBucket = field(init=False, repr=False)
    _session: requests.Session = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.user_agent or "@" not in self.user_agent:
            raise ValueError(
                "SEC requires a descriptive User-Agent containing a contact "
                "email, e.g. 'Company Name someone@example.com'."
            )
        self._bucket = _TokenBucket(self.requests_per_second)
        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        )

    # -- core ---------------------------------------------------------------
    def _get(self, url: str) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._bucket.take()
            try:
                resp = self._session.get(url, timeout=self.timeout)
            except requests.RequestException as exc:  # connection reset, DNS, ...
                last_exc = exc
                self._sleep_backoff(attempt)
                continue
            if resp.status_code in _RETRY_STATUS:
                last_exc = requests.HTTPError(f"{resp.status_code} for {url}")
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    time.sleep(min(float(retry_after), self.backoff_cap))
                else:
                    self._sleep_backoff(attempt)
                continue
            resp.raise_for_status()
            return resp
        raise RuntimeError(
            f"SEC request failed after {self.max_retries} retries: {url}"
        ) from last_exc

    def _sleep_backoff(self, attempt: int) -> None:
        delay = min(self.backoff_cap, self.backoff_base * (2 ** attempt))
        time.sleep(delay + random.uniform(0, delay * 0.25))

    # -- typed endpoints --------------------------------------------------
    def get_json(self, url: str) -> dict:
        return self._get(url).json()

    def get_bytes(self, url: str) -> bytes:
        return self._get(url).content

    def get_text(self, url: str) -> str:
        return self._get(url).text

    def submissions(self, cik: str | int) -> dict:
        return self.get_json(SUBMISSIONS_URL.format(cik10=cik10(cik)))

    def company_facts(self, cik: str | int) -> dict:
        return self.get_json(COMPANYFACTS_URL.format(cik10=cik10(cik)))

    def company_concept(
        self, cik: str | int, concept: str, taxonomy: str = "us-gaap"
    ) -> dict:
        return self.get_json(
            COMPANYCONCEPT_URL.format(
                cik10=cik10(cik), taxonomy=taxonomy, concept=concept
            )
        )

    def company_tickers(self) -> dict:
        return self.get_json(COMPANY_TICKERS_URL)

    def filing_document(self, cik: str | int, accession: str, doc: str) -> bytes:
        return self.get_bytes(
            ARCHIVE_DOC_URL.format(
                cik_int=int(cik10(cik)),
                accn_nodash=accession_nodash(accession),
                doc=doc,
            )
        )

    def submission_txt(self, cik: str | int, accession: str) -> bytes:
        return self.get_bytes(
            ARCHIVE_SUBMISSION_TXT_URL.format(
                cik_int=int(cik10(cik)), accn_nodash=accession_nodash(accession)
            )
        )


def iter_recent_filings(submissions: dict):
    """Yield dicts from the ``filings.recent`` columnar block of a submissions
    payload, one dict per filing with the column names as keys."""
    recent = submissions.get("filings", {}).get("recent", {})
    if not recent:
        return
    keys = list(recent.keys())
    n = len(recent[keys[0]]) if keys else 0
    for i in range(n):
        yield {k: recent[k][i] for k in keys}
