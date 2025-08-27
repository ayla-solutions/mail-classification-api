"""
main.py
-------
/mails does:
  PHASE 1 (FAST): idempotent minimal insert to Dataverse (by crabb_id)
  PHASE 2 (ASYNC): queue background enrichment (body+attachments ONLY → extractor) → PATCH all fields
"""

# =========================
# Imports & setup
# =========================
import os
import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI

from utils.auth import get_graph_token
from utils.extract_attachments import fetch_messages_with_attachments
from utils.dataverse import create_basic_email_row
from utils.extractor_worker import enrich_and_patch_dataverse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

WORKERS = int(os.getenv("ENRICHMENT_WORKERS", "4"))
_executor = ThreadPoolExecutor(max_workers=WORKERS)

app = FastAPI(title="Mail Classification API")

@app.get("/")
def root():
    return {"message": "Mail Classification API is running"}

@app.get("/mails")
def process_mails():
    """
    For each mail:
      - Ensure basic DV row exists (no dupes by crabb_id)
      - Queue enrichment worker (non-blocking)
    Returns quick counts to keep the API responsive.
    """
    token = get_graph_token()
    mails = fetch_messages_with_attachments(token)

    fetched = len(mails)
    created_or_skipped = 0
    queued = 0

    for mail in mails:
        mid = mail.get("id")
        subj = (mail.get("subject") or "")[:120]
        logging.info(f"[MAIL] {mid} | {subj}")

        # Phase 1: minimal create (idempotent)
        if create_basic_email_row(mail):
            created_or_skipped += 1

        # Phase 2: queue enrichment (only pass fields needed to build the text blob)
        worker_mail = {
            "id": mid,
            "mail_body_text": mail.get("mail_body_text"),
            "mail_body": mail.get("mail_body"),
            "body_preview": mail.get("body_preview"),
            "attachment_text": mail.get("attachment_text"),
        }
        _executor.submit(enrich_and_patch_dataverse, worker_mail)
        queued += 1

    return {
        "ok": True,
        "fetched": fetched,
        "phase1_created_or_skipped": created_or_skipped,
        "phase2_queued_enrichment": queued
    }
