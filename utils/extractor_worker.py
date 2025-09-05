"""
    utils/extractor_worker.py
    -------------------------
    Background enrichment worker (Phase 2)

    Flow (strict):
      - Perform a soft classification using the heuristic classifier before
        calling the extractor.  This classification is logged and can provide
        early insight into the type of mail.
      - Build ONE plain-text blob: <body_text or fallback> + <attachment_text>.
      - Send ONLY that blob to the external Extractor API.
      - On success:
          * Always keep category & priority.
          * If category == "invoice": include full invoice fields.
          * If category == "customer requests": include ONLY summary + ticket_number.
          * If category in {"general", "misc", "miscellaneous"}: include nothing else.
      - On failure:
          * Fallback to keyword classifier → category & priority only.
      - PATCH the Dataverse row (idempotent by Graph ID).
        NOTE: Your DV helper will also set 'paid=false' when category == 'invoice'.
"""

# =========================
# Imports
# =========================
from typing import Dict, Any
import logging
from threading import Lock

from utils.classify import classify_mail
from utils.extractor_client import call_extractor
from utils.dataverse import update_email_enrichment_text

# =========================
# Progress tracking
# =========================
# These globals track how many mails are scheduled and processed.
_total_mails: int = 0
_processed_mails: int = 0
_progress_lock: Lock = Lock()

def set_total_mails(n: int) -> None:
    """
    Set the total number of mails expected for the current batch.  This should
    be called from the main thread before any enrichment tasks are submitted.
    """
    global _total_mails, _processed_mails
    with _progress_lock:
        _total_mails = max(0, int(n))
        _processed_mails = 0
        logging.info(f"[PROGRESS] Starting enrichment of {_total_mails} mails.")

def _update_progress() -> None:
    """
    Increment the processed mail count and emit a progress log message.  This
    function is safe to call from multiple threads.
    """
    global _processed_mails
    with _progress_lock:
        _processed_mails += 1
        if _total_mails:
            logging.info(
                f"[PROGRESS] Processed {_processed_mails}/{_total_mails} mails."
            )
        else:
            logging.info(f"[PROGRESS] Processed {_processed_mails} mails.")

# =========================
# Helpers
# =========================
def _combined_text(mail: Dict[str, Any]) -> str:
    """
    Build a single plain‑text blob for the LLM:
      - Prefer full 'mail_body_text' if present
      - Fallback to 'mail_body' or 'body_preview'
      - Append 'attachment_text' at the end
    """
    subj = (mail.get("subject") or "").strip()
    body = (
        mail.get("mail_body_text")
        or mail.get("mail_body")
        or mail.get("body_preview")
        or ""
    )
    att = mail.get("attachment_text") or ""
    parts = []
    if subj:
        parts.append(f"Subject: {subj}")
    if body:
        parts.append(body)
    if att:
        parts.append(f"--- Attachment text ---\n{att}")
    return "\n\n".join(parts).strip()

def _flatten_per_rules(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply business rules to extractor output and return only the fields we intend
    to PATCH into Dataverse.

    Expected extractor shape:
      {
        "category": "...",
        "priority": "...",
        "invoice": { ... },             # when invoice
        "request": { ... }              # when customer requests
      }

    Rules:
      - Always include 'category' and 'priority'
      - If category == "invoice": include full invoice fields
      - If category == "customer requests": include ONLY 'summary', 'ticket_number'
      - If category in {"general","misc","miscellaneous"}: include nothing else
    """
    out: Dict[str, Any] = {}

    # ---- carry top‑level labels (if present) ----
    cat = (data.get("category") or "").strip()
    pri = (data.get("priority") or "").strip()
    out["category"] = cat
    out["priority"] = pri

    # ---- normalize "invoices" → "invoice" for consistency downstream ----
    if out["category"] and out["category"].lower() == "invoices":
        out["category"] = "invoice"

    cat_l = (out.get("category") or "").lower()

    # ---- INVOICE: pass through the known invoice fields ----
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
                # if your extractor also returns these, they'll flow through:
                "biller_code":       inv.get("biller_code"),
                "payment_reference": inv.get("payment_reference"),
                "description":       inv.get("description"),
            })
        return out  # nothing else for invoices

    # ---- CUSTOMER REQUESTS: ONLY summary + ticket_number ----
    # accept singular/plural just in case
    if cat_l in {"customer requests", "customer request"}:
        req = data.get("request") or {}
        if isinstance(req, dict):
            # canonical keys you want to persist:
            out["summary"] = req.get("summary") or req.get("overview")
            out["ticket_number"] = req.get("ticket_number") or req.get("request_number")
        return out

    # ---- GENERAL / MISC: no additional fields ----
    if cat_l in {"general", "misc", "miscellaneous"}:
        return out

    # For any unexpected category values, keep it minimal:
    return out

# =========================
# Worker
# =========================
def enrich_and_patch_dataverse(mail: Dict[str, Any]) -> None:
    """
    Phase‑2 enrichment worker:

      - Perform a soft classification using the heuristic classifier prior to
        invoking the extractor.  This classification is logged but does not
        alter the extractor flow.
      - Build minimal payload for extractor (ONLY text blob).
      - Try extractor → flatten per rules.
      - On exception → fallback to keyword classifier.
      - PATCH the Dataverse row by Graph ID.
      - Update progress counters.
    """
    graph_id = mail.get("id")
    if not graph_id:
        logging.warning("enrich_and_patch_dataverse: missing mail.id; skip")
        return

    # ---- Soft (heuristic) classification ----
    try:
        soft_res = classify_mail(mail)
        soft_cat = soft_res.get("category")
        soft_pri = soft_res.get("priority")
        logging.info(
            f"[SOFT CLASSIFICATION] graph_id={graph_id} category={soft_cat}, priority={soft_pri}"
        )
    except Exception:
        logging.exception("Soft classification failed")

    # ---- Build payload with ONLY the text we want the LLM to see ----
    text_blob = _combined_text(mail)
    received_at = mail.get("received_at") or mail.get("receivedDateTime")
    payload = {
        "graph_id":    graph_id,   # passthrough; extractor may ignore
        "subject":     mail.get("subject"),
        "sender":      "",
        "body_html":   "",
        "body_text":   text_blob,  # ← THE ONLY CONTENT WE SEND
        "received_at": received_at,
        "attachments": [],
    }
    logging.info(
        "worker_payload_built",
        extra={
            "graph_id": graph_id,
            "received_at": received_at,
            "body_len": len(text_blob),
        },
    )

    # ---- Call extractor, fallback to keyword classify on failure ----
    try:
        resp = call_extractor(payload, push_to_dataverse=False)
        data = resp.get("data", {}) or {}
        enrichment = _flatten_per_rules(data)

        logging.info(
            f"[EXTRACTOR OK] crabb_id={graph_id} keys="
            f"{sorted(list(k for k, v in enrichment.items() if v is not None))}"
        )
    except Exception:
        logging.exception("[EXTRACTOR FAIL] Fallback → keyword classifier")
        fb = classify_mail(mail)  # {"category": "...", "priority": "..."}
        cat = (fb.get("category") or "").strip().lower()
        if cat == "invoices":
            cat = "invoice"
        enrichment = {
            "category": cat,
            "priority": fb.get("priority"),
        }

    # ---- PATCH back to Dataverse (idempotent by Graph ID) ----
    ok = update_email_enrichment_text(graph_id, enrichment)
    if not ok:
        logging.error(f"[ENRICH PATCH FAIL] crabb_id={graph_id}")
    else:
        logging.info(f"[ENRICH PATCH OK] crabb_id={graph_id}")

    # ---- Update progress counters ----
    _update_progress()
