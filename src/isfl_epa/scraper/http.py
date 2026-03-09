"""Shared HTTP session with retry logic for all scrapers."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_session: requests.Session | None = None


def get_session() -> requests.Session:
    """Return a shared requests.Session with automatic retry on transient errors."""
    global _session
    if _session is None:
        _session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=10)
        _session.mount("https://", adapter)
        _session.mount("http://", adapter)
    return _session
