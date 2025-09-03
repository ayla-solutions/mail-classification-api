"""
Mail Classification API (fully instrumented)
--------------------------------------------
/mails does two phases for each email:
  PHASE 1 (FAST, IDEMPOTENT): Minimal insert to Dataverse keyed by Graph message id (crabb_id)
  PHASE 2 (ASYNC): Background enrichment via Extractor API (LLM) → PATCH Dataverse
"""

import os
import time
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any
from fastapi import FastAPI, Request, Response, Header, HTTPException
from fastapi.security import HTTPBearer
import jwt

from logging_setup import init_logging, set_request_id, set_graph_id
from utils.auth_obo import get_graph_token_obo
from utils.extract_attachments import fetch_messages_with_attachments
from utils.dataverse import create_basic_email_row
from utils.extractor_worker import enrich_and_patch_dataverse

# ------------------------------------------------------------------------------

init_logging()
log = logging.getLogger("main")

WORKERS = int(os.getenv("ENRICHMENT_WORKERS", "4"))
SLOW_GRAPH_MS = int(os.getenv("SLOW_GRAPH_MS", "4000"))
SLOW_DV_MS    = int(os.getenv("SLOW_DV_MS", "3000"))
SLOW_EX_MS    = int(os.getenv("SLOW_EX_MS", "8000"))
PREVIEW_CHARS = int(os.getenv("LOG_PREVIEW_CHARS", "280"))
LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO").upper()

_executor = ThreadPoolExecutor(max_workers=WORKERS)
app = FastAPI(title="Mail Classification API (instrumented)")
bearer_security = HTTPBearer(auto_error=False)

# ------------------------------------------------------------------------------

def _preview(s: str | None, lim: int = PREVIEW_CHARS) -> Dict[str, Any]:
    """Return safe preview for logs: length + short prefix only."""
    if not s:
        return {"len": 0, "preview": ""}
    s = s.strip()
    return {"len": len(s), "preview": s[:lim] + ("…" if len(s) > lim else "")}

def _peek_claims(bearer: str) -> dict:
    try:
        token = bearer.split(" ", 1)[1]
        return jwt.decode(token, options={"verify_signature": False, "verify_aud": False})
    except Exception:
        return {}

# ------------------------------------------------------------------------------

# main.py
if __name__ == "__main__":
    from logging_setup import setup_logging
    setup_logging()
    import uvicorn, os
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        access_log=True,              # make sure this is True
    )


@app.middleware("http")
async def add_request_context(request: Request, call_next):
    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    set_request_id(rid)
    start = time.perf_counter()

    log.info(
        "http_request_start",
        extra={
            "request_id": rid,
            "method": request.method,
            "path": request.url.path,
            "query": str(request.url.query),
            "client": request.client.host if request.client else None,
        },
    )

    try:
        response: Response = await call_next(request)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        log.info(
            "http_request_end",
            extra={
                "request_id": rid,
                "status_code": response.status_code,
                "elapsed_ms": elapsed_ms,
            },
        )
        response.headers["X-Request-ID"] = rid
        return response
    finally:
        set_request_id(None)

# ------------------------------------------------------------------------------

@app.get("/")
def root():
    return {"message": "Mail Classification API is running (instrumented)"}

@app.get("/health")
def health():
    return {
        "ok": True,
        "workers": WORKERS,
        "log_level": LOG_LEVEL,
        "slow_ms": {"graph": SLOW_GRAPH_MS, "dataverse": SLOW_DV_MS, "extractor": SLOW_EX_MS},
        "preview_chars": PREVIEW_CHARS,
        "auth": "delegated-obo",
    }

@app.get("/Health")
def health_alias():
    return health()

# ------------------------------------------------------------------------------

@app.post("/mails")
def process_mails(authorization: str = Header(None)):
    """
    Fetch mails (+ attachments), Phase-1 insert, Phase-2 queue enrichment.
    Uses delegated Graph token via OBO (no mailbox param needed).
    """
    # ---- 1) Validate incoming bearer ----
    if not authorization or not authorization.lower().startswith("bearer "):
        log.info("no_bearer_header")
        raise HTTPException(status_code=401, detail="Missing bearer token from connector")

    claims = _peek_claims(authorization)
    log.info("incoming_token", extra={"kv": {
        "aud": claims.get("aud"),
        "scp": claims.get("scp"),
        "tid": claims.get("tid"),
        "azp": claims.get("azp"),
    }})

    req_id = uuid.uuid4().hex
    set_request_id(req_id)

    user_token = authorization.split(" ", 1)[1].strip()

    # ---- 2) Exchange for Graph delegated token (OBO) ----
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

    # ---- 3) Fetch messages for the signed-in user (/me) ----
    t2 = time.perf_counter()
    mails = fetch_messages_with_attachments(graph_token)
    
    t3 = time.perf_counter()
    fetch_ms = int((t3 - t2) * 1000)
    fetched = len(mails)
    log.info("graph_fetch_messages_done",
             extra={"elapsed_ms": fetch_ms, "request_id": req_id, "count": fetched})
    if fetch_ms > SLOW_GRAPH_MS:
        log.warning("slow_graph_fetch",
                    extra={"elapsed_ms": fetch_ms, "request_id": req_id, "count": fetched})

    created_or_skipped = 0
    queued = 0
    details: list[Dict[str, Any]] = []

    # ---- 4) Phase 1 + queue Phase 2 per message ----
    for m in mails:
        mid = m.get("id")
        subj = (m.get("subject") or "")[:120]
        set_graph_id(mid)
        log.info("mail_begin", extra={"kv": {"graph_id": mid, "subject": subj}})

        body_preview = _preview(m.get("mail_body_text") or m.get("mail_body") or m.get("body_preview") or "")
        att_preview  = _preview(m.get("attachment_text") or "")

        # Phase 1 (idempotent Dataverse create)
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

        # Phase 2 (background enrichment)
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

        log.info("enrichment_queued", extra={
            "kv": {
                "graph_id": mid,
                "body_text": body_preview,
                "attachment_text": att_preview,
                "attachments_count": len(m.get("attachments") or []),
                "attachment_methods": m.get("attachment_methods") or [],
            }
        })

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
