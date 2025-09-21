"""
Background enrichment worker (Phase 2) for the unified Mail Classification API.

This worker runs the expensive LLM enrichment stage for each mail fetched
from Microsoft Graph.  It performs a two‑stage process:

1. Soft classification using the simple keyword heuristic defined in
   ``utils.classify``.  This stage is used only for logging and does not
   affect the final category and priority.
2. LLM extraction using the in‑process Ollama client defined in
   ``utils.ollama_llm``.  The worker builds a combined text blob
   consisting of the subject, body and attachment text, then calls
   ``run_local_extraction`` which returns category/priority and any
   invoice or customer request fields.  The returned data is flattened
   and patched back into Dataverse.

Throughout the workflow detailed log messages are emitted.  These
messages include progress updates, timings for each stage and the
Graph message ID.  Operators can follow Azure Log Stream to monitor
progress and diagnose issues.
"""

from __future__ import annotations

import logging
from threading import Lock
from typing import Dict, Any, List

from utils.classify import classify_mail
from utils.dataverse import update_email_enrichment_text
from utils.ollama_llm import run_local_extraction

# Progress tracking globals
_total_mails: int = 0
_processed_mails: int = 0
_progress_lock: Lock = Lock()

def set_total_mails(n: int) -> None:
    """Initialise the mail counters for the current batch."""
    global _total_mails, _processed_mails
    with _progress_lock:
        _total_mails = max(0, int(n))
        _processed_mails = 0
        logging.info(f"[PROGRESS] Starting enrichment of {_total_mails} mails.")

def _update_progress() -> None:
    """Increment the processed mail counter and emit a progress log."""
    global _processed_mails
    with _progress_lock:
        _processed_mails += 1
        if _total_mails:
            logging.info(f"[PROGRESS] Processed {_processed_mails}/{_total_mails} mails.")
        else:
            logging.info(f"[PROGRESS] Processed {_processed_mails} mails.")

def _combined_text(mail: Dict[str, Any]) -> str:
    """Construct a single plain‑text blob from the mail parts."""
    subj = (mail.get("subject") or "").strip()
    body = (
        mail.get("mail_body_text")
        or mail.get("mail_body")
        or mail.get("body_preview")
        or ""
    )
    att = mail.get("attachment_text") or ""
    parts: List[str] = []
    if subj:
        parts.append(f"Subject: {subj}")
    if body:
        parts.append(body)
    if att:
        parts.append(f"--- Attachment text ---\n{att}")
    return "\n\n".join(parts).strip()

def _flatten_per_rules(data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply business rules to the extractor output."""
    out: Dict[str, Any] = {}
    cat = (data.get("category") or "").strip()
    pri = (data.get("priority") or "").strip()
    out["category"] = cat
    out["priority"] = pri
    if out["category"] and out["category"].lower() == "invoices":
        out["category"] = "invoice"
    cat_l = (out.get("category") or "").lower()
    if cat_l == "invoice":
        inv = data.get("invoice") or {}
        if isinstance(inv, dict):
            out.update({
                "invoice_number":    inv.get("invoice_number"),
                "invoice_date":      inv.get("invoice_date"),
                "due_date":          inv.get("due_date"),
                "invoice_amount":    inv.get("invoice_amount"),
                "payment_link":      inv.get("payment_link"),
                "bsb":               inv.get("bsb"),
                "account_number":    inv.get("account_number"),
                "account_name":      inv.get("account_name"),
                "biller_code":       inv.get("biller_code"),
                "payment_reference": inv.get("payment_reference"),
                "description":       inv.get("description"),
            })
        return out
    if cat_l in {"customer requests", "customer request"}:
        req = data.get("request") or {}
        if isinstance(req, dict):
            out["summary"] = req.get("summary") or req.get("overview")
            out["ticket_number"] = req.get("ticket_number") or req.get("request_number")
        return out
    return out

def enrich_and_patch_dataverse(mail: Dict[str, Any]) -> None:
    """Run Phase‑2 enrichment on a single mail."""
    graph_id = mail.get("id")
    if not graph_id:
        logging.warning("enrich_and_patch_dataverse: missing mail.id; skip")
        return
    # Soft heuristic classification for early insight
    try:
        soft_res = classify_mail(mail)
        soft_cat = soft_res.get("category")
        soft_pri = soft_res.get("priority")
        logging.info(
            f"[SOFT CLASSIFICATION] graph_id={graph_id} category={soft_cat}, priority={soft_pri}"
        )
    except Exception:
        logging.exception("Soft classification failed")
    text_blob = _combined_text(mail)
    received_at = mail.get("received_at") or mail.get("receivedDateTime")
    llm_payload: Dict[str, Any] = {
        "graph_id": graph_id,
        "subject": mail.get("subject"),
        "body_text": text_blob,
        "received_at": received_at,
    }
    try:
        resp = run_local_extraction(llm_payload)
        data = resp.get("data", {}) or {}
        enrichment = _flatten_per_rules(data)
        logging.info(
            f"[LLM OK] crabb_id={graph_id} keys="
            f"{sorted(list(k for k, v in enrichment.items() if v is not None))}"
        )
    except Exception:
        logging.exception("[LLM FAIL] Fallback → keyword classifier")
        fb = classify_mail(mail)
        cat = (fb.get("category") or "").strip().lower()
        if cat == "invoices":
            cat = "invoice"
        enrichment = {
            "category": cat,
            "priority": fb.get("priority"),
        }
    ok = update_email_enrichment_text(graph_id, enrichment)
    if not ok:
        logging.error(f"[ENRICH PATCH FAIL] crabb_id={graph_id}")
    else:
        logging.info(f"[ENRICH PATCH OK] crabb_id={graph_id}")
    _update_progress()
