"""
Unified Mail Classification API
===============================

This FastAPI application combines the functionality of the original
``mail-classification-api`` and ``mail-ollama-extractor`` into a single
service.  It exposes the following endpoints:

* ``GET /`` – Health check returning a simple confirmation string.
* ``GET /health`` – Returns configuration and tuning values for the
  service.
* ``POST /mails`` – Trigger the ingestion pipeline.  It fetches new
  messages from Microsoft Graph, performs Phase 1 idempotent
  persistence into Dataverse and schedules Phase 2 enrichment tasks.
* ``POST /classify`` – Run an in‑process LLM classification on an
  arbitrary payload.  Useful for testing and debugging the
  classification prompt in isolation.
* ``POST /extract`` – Run an in‑process LLM classification and
  extraction on an arbitrary payload.  Mirrors the behaviour of the
  former extractor API.

The implementation retains the two‑phase processing model: Phase 1
records only the minimal metadata for each mail (id, sender, subject,
body preview and attachment text) and Phase 2 augments that record
with category, priority and any invoice or customer request fields.

Logging is verbose and structured.  The root logger is configured via
``logging_setup.init_logging`` to emit either JSON or human readable
lines.  Each request is assigned a unique request ID which flows
through to the enrichment tasks.  Slow calls to Graph, Dataverse or
the LLM are flagged via warnings to aid performance tuning.
"""

from __future__ import annotations

import os
import time
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

from fastapi import FastAPI, Request, Header, HTTPException, Depends

from logging_setup import init_logging, set_request_id, set_graph_id
from utils.auth import require_aad_token             # validated multi‑tenant JWT dependency
from utils.auth_obo import get_graph_token_obo       # OBO flow for Graph delegated token
from utils.extract_attachments import fetch_messages_with_attachments
from utils.dataverse import create_basic_email_row
from utils.extractor_worker import enrich_and_patch_dataverse, set_total_mails
from utils.ollama_llm import (
    ExtractIn,
    ExtractOut,
    run_local_extraction,
    classify_text as llm_classify,
)

# Initialise logging at import time.  This configures the root logger and
# ensures that all modules share the same handlers and format.  The
# LOG_LEVEL, LOG_STYLE and other settings can be tuned via environment
# variables.
init_logging()
log = logging.getLogger("main")

# ---------------------------------------------------------------------------
# Tuning knobs and environment driven configuration
# ---------------------------------------------------------------------------
# These values control the size of the thread pool used for Phase 2
# enrichment, as well as thresholds used to emit warnings when calls
# exceed expected durations.  Adjust these via environment variables to
# suit your deployment’s performance characteristics.
WORKERS = int(os.getenv("ENRICHMENT_WORKERS", "4"))
SLOW_GRAPH_MS = int(os.getenv("SLOW_GRAPH_MS", "4000"))
SLOW_DV_MS = int(os.getenv("SLOW_DV_MS", "3000"))
SLOW_LLM_MS = int(os.getenv("SLOW_LLM_MS", "8000"))
PREVIEW_CHARS = int(os.getenv("LOG_PREVIEW_CHARS", "280"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAIL_SINCE_DAYS = int(os.getenv("MAIL_SINCE_DAYS", "7"))

_executor = ThreadPoolExecutor(max_workers=WORKERS)
app = FastAPI(title="Unified Mail Classification API")

def _preview(s: str | None, lim: int = PREVIEW_CHARS) -> Dict[str, Any]:
    """Return a safe preview of a potentially long string for logging.

    The returned dictionary contains the full length of the string and a
    prefix truncated to ``lim`` characters.  This helper is used when
    logging mail bodies and attachment text to avoid emitting large
    blobs into the logs.
    """
    if not s:
        return {"len": 0, "preview": ""}
    s = s.strip()
    return {
        "len": len(s),
        "preview": s[:lim] + ("…" if len(s) > lim else ""),
    }

# ---------------------------------------------------------------------------
# Root and health endpoints
# ---------------------------------------------------------------------------
@app.get("/")
def root() -> Dict[str, str]:
    """Basic health check for service availability."""
    return {"message": "Mail Classification API is running (unified)"}

@app.get("/health")
def health() -> Dict[str, Any]:
    """Detailed health endpoint exposing configuration values."""
    return {
        "ok": True,
        "workers": WORKERS,
        "log_level": LOG_LEVEL,
        "slow_ms": {"graph": SLOW_GRAPH_MS, "dataverse": SLOW_DV_MS, "llm": SLOW_LLM_MS},
        "preview_chars": PREVIEW_CHARS,
        "auth": "delegated-obo + AAD multi-tenant JWT validation",
    }

# Backwards compatible alias
@app.get("/Health")
def health_alias() -> Dict[str, Any]:
    return health()

# ---------------------------------------------------------------------------
# Phase 1/2 mail ingestion endpoint
# ---------------------------------------------------------------------------
@app.post("/mails")
def process_mails(
    authorization: str = Header(None),
    claims: dict = Depends(require_aad_token),  # enforce valid AAD token
) -> Dict[str, Any]:
    """Fetch mails, persist basic metadata and queue enrichment tasks.

    This endpoint performs the following actions:

    1. Validate and extract the bearer token from the incoming request.
    2. Exchange the token for a delegated Graph token using the
       On‑Behalf‑Of (OBO) flow.  This allows the API to call
       ``/me/messages`` on behalf of the caller.
    3. Fetch recent messages (including attachments) via Graph and
       extract basic plain text from the body and attachments.
    4. For each message, create a minimal Dataverse record keyed by
       the message ID (crabb_id).  The ``create_basic_email_row``
       helper ensures idempotency.
    5. Submit a background task to enrich each message using the
       in‑process LLM and patch the resulting fields back into
       Dataverse.  Progress is tracked and logged.

    The response summarises how many mails were fetched, how many
    Dataverse rows were created or skipped in Phase 1, and how many
    enrichment tasks were queued for Phase 2.  It also includes
    per‑mail details for debugging purposes.
    """
    # Step 1: Validate the Authorization header
    log.info("caller_validated", extra={"kv": {
        "tid": claims.get("tid"),
        "oid": claims.get("oid"),
        "scp": claims.get("scp"),
        "aud": claims.get("aud"),
        "azp": claims.get("azp"),
    }})
    if not authorization or not authorization.lower().startswith("bearer "):
        log.info("no_bearer_header_after_validation")
        raise HTTPException(status_code=401, detail="Missing bearer token from connector")

    req_id = uuid.uuid4().hex
    set_request_id(req_id)
    user_token = authorization.split(" ", 1)[1].strip()

    # Step 2: Exchange for Graph delegated token (OBO)
    t0 = time.perf_counter()
    try:
        graph_token = get_graph_token_obo(user_token)
    except RuntimeError as e:
        log.error("obo_error", extra={"request_id": req_id, "error": str(e)})
        raise HTTPException(status_code=401, detail=str(e))
    t1 = time.perf_counter()
    tok_ms = int((t1 - t0) * 1000)
    log.info("graph_token_obo_ok", extra={"elapsed_ms": tok_ms, "request_id": req_id})
    if tok_ms > SLOW_GRAPH_MS:
        log.warning("slow_graph_token_obo", extra={"elapsed_ms": tok_ms, "request_id": req_id})

    # Step 3: Fetch messages from Graph
    t2 = time.perf_counter()
    mails = fetch_messages_with_attachments(graph_token, since_days=MAIL_SINCE_DAYS)
    t3 = time.perf_counter()
    fetch_ms = int((t3 - t2) * 1000)
    fetched = len(mails)
    log.info("graph_fetch_messages_done", extra={"elapsed_ms": fetch_ms, "request_id": req_id, "count": fetched})
    if fetch_ms > SLOW_GRAPH_MS:
        log.warning("slow_graph_fetch", extra={"elapsed_ms": fetch_ms, "request_id": req_id, "count": fetched})

    # Inform the worker about the total number of mails for progress tracking
    set_total_mails(fetched)
    created_or_skipped = 0
    queued = 0
    details: list[Dict[str, Any]] = []

    # Step 4: Phase 1 + queue Phase 2 per message
    for m in mails:
        mid = m.get("id")
        subj = (m.get("subject") or "")[:120]
        set_graph_id(mid)
        log.info("mail_begin", extra={"kv": {"graph_id": mid, "subject": subj}})
        body_preview = _preview(m.get("mail_body_text") or m.get("mail_body") or m.get("body_preview") or "")
        att_preview = _preview(m.get("attachment_text") or "")

        # Phase 1: idempotent Dataverse insert
        c0 = time.perf_counter()
        phase1_ok = create_basic_email_row(m)
        c1 = time.perf_counter()
        c_ms = int((c1 - c0) * 1000)
        if c_ms > SLOW_DV_MS:
            log.warning("slow_dataverse_create", extra={"elapsed_ms": c_ms, "graph_id": mid})
        if phase1_ok:
            created_or_skipped += 1
            log.info("dv_create_or_skip_ok", extra={"kv": {"graph_id": mid, "elapsed_ms": c_ms}})
        else:
            log.error("dv_create_failed", extra={"kv": {"graph_id": mid, "elapsed_ms": c_ms}})

        # Phase 2: schedule enrichment in the background.  We pass a
        # simplified mail dictionary to the worker which extracts just
        # the fields it needs for the prompt.
        worker_mail = {
            "id": mid,
            "subject": m.get("subject"),
            "received_at": m.get("received_at"),
            "mail_body_text": m.get("mail_body_text"),
            "mail_body": m.get("mail_body"),
            "body_preview": m.get("body_preview"),
            "attachment_text": m.get("attachment_text"),
        }
        _executor.submit(enrich_and_patch_dataverse, worker_mail)
        queued += 1
        log.info(
            "enrichment_queued",
            extra={
                "kv": {
                    "graph_id": mid,
                    "body_text": body_preview,
                    "attachment_text": att_preview,
                    "attachments_count": len(m.get("attachments") or []),
                    "attachment_methods": m.get("attachment_methods") or [],
                }
            },
        )
        details.append({
            "graph_id": mid,
            "subject": subj,
            "body_text": body_preview,
            "attachment_text": att_preview,
            "attachments_count": len(m.get("attachments") or []),
            "attachment_methods": m.get("attachment_methods") or [],
            "created_or_skipped": phase1_ok,
            "dv_create_ms": c_ms,
        })
        set_graph_id(None)

    set_request_id(None)
    return {
        "ok": True,
        "fetched": fetched,
        "phase1_created_or_skipped": created_or_skipped,
        "phase2_queued_enrichment": queued,
        "graph_fetch_ms": fetch_ms,
        "details": details,
    }

# ---------------------------------------------------------------------------
# In‑process classification endpoint
# ---------------------------------------------------------------------------
@app.post("/classify", response_model=ExtractOut)
def classify_local(payload: ExtractIn) -> ExtractOut:
    """Classify a mail body and optional attachments using the LLM.

    This endpoint is provided to allow clients or testers to call the
    classification model directly.  It constructs a trimmed prompt and
    returns a dictionary with ``category`` and ``priority``.  No
    invoice or customer request extraction is performed here.
    """
    cls = llm_classify(payload.body_text, graph_id=payload.graph_id)
    log.info(
        "classify_endpoint_complete",
        extra={"kv": {
            "graph_id": payload.graph_id or "-",
            "category": cls.category,
            "priority": cls.priority,
        }},
    )
    return ExtractOut(ok=True, data={"category": cls.category, "priority": cls.priority})

# ---------------------------------------------------------------------------
# In‑process extraction endpoint
# ---------------------------------------------------------------------------
@app.post("/extract", response_model=ExtractOut)
def extract_local(payload: ExtractIn) -> ExtractOut:
    """Classify and extract invoice or request details using the LLM.

    This endpoint mirrors the behaviour of the former extractor API.  It
    performs classification and, depending on the category, either
    extracts invoice fields, summarises a customer request, or returns
    only the category and priority.  The response is identical to
    ``run_local_extraction``: ``ok`` with a ``data`` dictionary.
    """
    t0 = time.perf_counter()
    try:
        resp = run_local_extraction(payload.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    t1 = time.perf_counter()
    extract_ms = int((t1 - t0) * 1000)
    if extract_ms > SLOW_LLM_MS:
        log.warning(
            "slow_llm_extract", extra={"elapsed_ms": extract_ms, "graph_id": payload.graph_id or "-"}
        )
    log.info(
        "extract_endpoint_complete",
        extra={"kv": {"graph_id": payload.graph_id or "-", "elapsed_ms": extract_ms}},
    )
    return ExtractOut(**resp)
