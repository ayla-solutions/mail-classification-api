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
import time
from typing import Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Base URLs and paths from environment variables.
BASE_URL     = (os.getenv("EXTRACTOR_URL") or "").rstrip("/")
HEALTH_PATH  = os.getenv("EXTRACTOR_HEALTH_PATH", "/health")
EXTRACT_PATH = os.getenv("EXTRACTOR_EXTRACT_PATH", "/extract")

# Extractor call warning threshold (in seconds).  If an extractor request
# takes longer than this many seconds, a warning will be emitted.  A value of
# 0 disables the warning.  The default can be configured via the
# EXTRACTOR_WARN_SEC environment variable.  Use EXTRACTOR_TIMEOUT_SEC to
# control client‑side timeouts; leaving it unset or set to 0 disables
# timeouts entirely.
WARN_THRESHOLD = float(os.getenv("EXTRACTOR_WARN_SEC", "30"))  # seconds
TIMEOUT_ENV    = os.getenv("EXTRACTOR_TIMEOUT_SEC")

# If no timeout is specified or EXTRACTOR_TIMEOUT_SEC is "0", disable timeouts.
TIMEOUT: Optional[int]
if not TIMEOUT_ENV or TIMEOUT_ENV == "0":
    TIMEOUT = None
else:
    try:
        TIMEOUT = int(TIMEOUT_ENV)
    except ValueError:
        TIMEOUT = None

# Configure a requests Session with retries.  total=None means unlimited
# retries; backoff_factor controls the delay between retries.
_session = requests.Session()
_retry = Retry(
    total=None,  # unlimited retries so that we don't drop requests on transient errors
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
    """
    Perform a lightweight healthcheck against the extractor service.

    This function iterates through common health endpoints and returns True
    on the first successful 200 OK response.  Failures are logged at
    warning level but are not fatal.
    """
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
def call_extractor(data: Dict[str, Any], push_to_dataverse: bool = False,
                   timeout: Optional[int] = TIMEOUT) -> Dict[str, Any]:
    """
    POST to the extractor '/extract' endpoint.

    The worker builds 'data' so that ONLY 'body_text' contains content.

    This client will not enforce a hard timeout unless one is explicitly
    provided via the 'timeout' argument or the EXTRACTOR_TIMEOUT_SEC
    environment variable.  Instead, it waits until the server responds.
    If the call duration exceeds the configured warning threshold, a log
    message is emitted to aid monitoring.
    """
    if not BASE_URL:
        raise RuntimeError("EXTRACTOR_URL not set")

    payload = dict(data)
    payload["push_to_dataverse"] = push_to_dataverse

    # Perform healthcheck before call, but do not abort on failure.
    if not _healthcheck():
        logging.warning("Extractor health did not pass; attempting extract anyway.")

    url = f"{BASE_URL}{EXTRACT_PATH}"
    logging.info(f"Calling extractor at {url}")

    # Record the start time for duration measurement.
    start = time.perf_counter()
    try:
        # Note: timeout=None disables the client‑side timeout.  If timeout
        # is an integer, requests will abort after that many seconds.
        resp = _session.post(url, json=payload, timeout=timeout)
        # Raise an error on HTTP 4xx/5xx responses.
        if resp.status_code == 404:
            raise RuntimeError(f"Extractor 404 at {url}; check EXTRACTOR_EXTRACT_PATH.")
        resp.raise_for_status()
        return resp.json()
    except requests.ConnectionError as e:
        # Connection failures propagate as RuntimeError.
        raise RuntimeError(f"Extractor connection error at {url}: {e}") from e
    except requests.RequestException as e:
        # For any other request exception, include the response text if available.
        msg = getattr(e.response, "text", str(e))
        raise RuntimeError(f"Extractor request failed at {url}: {msg}") from e
    finally:
        # Always measure call duration and emit a warning if it exceeds threshold.
        duration = time.perf_counter() - start
        if WARN_THRESHOLD and duration > WARN_THRESHOLD:
            logging.warning(
                f"Extractor call took {duration:.2f}s which exceeds the warning threshold of "
                f"{WARN_THRESHOLD}s"
            )
