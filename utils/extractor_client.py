"""
utils/extractor_client.py
-------------------------
Resilient client for your FastAPI extractor (Ollama-backed).
- Only used by the background worker.
"""

# =========================
# Imports & Session
# =========================
import os
import logging
from typing import Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL     = (os.getenv("EXTRACTOR_URL") or "").rstrip("/")
HEALTH_PATH  = os.getenv("EXTRACTOR_HEALTH_PATH", "/health")
EXTRACT_PATH = os.getenv("EXTRACTOR_EXTRACT_PATH", "/extract")
TIMEOUT      = int(os.getenv("EXTRACTOR_TIMEOUT_SEC", "60"))

_session = requests.Session()
_retry = Retry(
    total=2,
    backoff_factor=1.0,
    status_forcelist=(408, 429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
)
_session.mount("http://", HTTPAdapter(max_retries=_retry))
_session.mount("https://", HTTPAdapter(max_retries=_retry))


# =========================
# Healthcheck (non-fatal)
# =========================
def _healthcheck(timeout: int = 5) -> bool:
    if not BASE_URL:
        logging.warning("Extractor BASE_URL not set; skipping healthcheck.")
        return False

    for path in (HEALTH_PATH, "/Health", "/"):
        url = f"{BASE_URL}{path}"
        try:
            r = _session.get(url, timeout=timeout)
            if r.status_code == 200:
                return True
            logging.warning(f"Extractor healthcheck at {url} returned {r.status_code}")
        except Exception as e:
            logging.warning(f"Extractor healthcheck failed at {url}: {e}")
    return False


# =========================
# Public: call_extractor
# =========================
def call_extractor(data: Dict[str, Any], push_to_dataverse: bool = False, timeout: int = TIMEOUT) -> Dict[str, Any]:
    """
    POST to extractor '/extract'.
    The worker builds 'data' so that ONLY 'body_text' contains content.
    """
    if not BASE_URL:
        raise RuntimeError("EXTRACTOR_URL not set")

    payload = dict(data)
    payload["push_to_dataverse"] = push_to_dataverse

    if not _healthcheck():
        logging.warning("Extractor health did not pass; attempting extract anyway.")

    url = f"{BASE_URL}{EXTRACT_PATH}"
    logging.info(f"Calling extractor at {url}")

    try:
        resp = _session.post(url, json=payload, timeout=timeout)
        if resp.status_code == 404:
            raise RuntimeError(f"Extractor 404 at {url}; check EXTRACTOR_EXTRACT_PATH.")
        resp.raise_for_status()
        return resp.json()

    except requests.Timeout:
        raise RuntimeError(f"Extractor timed out after {timeout}s at {url}.")
    except requests.ConnectionError as e:
        raise RuntimeError(f"Extractor connection error at {url}: {e}")
    except requests.RequestException as e:
        msg = getattr(e.response, "text", str(e))
        raise RuntimeError(f"Extractor request failed at {url}: {msg}") from e
