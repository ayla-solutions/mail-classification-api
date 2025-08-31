"""
utils/classify.py
-----------------
Very simple keyword fallback classifier (used only if extractor fails).
Returns: {"category": <str>, "priority": <str>}
"""

# =========================
# Public: classify_mail
# =========================
import logging
from logging_setup import init_logging
init_logging()
log = logging.getLogger("utils.classify")

def classify_mail(mail: dict) -> dict:
    """Heuristic fallback."""
    subject = (mail.get("subject") or "").lower()
    body    = (mail.get("body_preview") or "").lower()
    attn    = " ".join(mail.get("attachments", [])).lower()
    text = f"{subject} {body} {attn}"

    categories = {
        "Invoice":              ["invoice", "bill", "statement"],
        "Customer Requests":     ["client", "customer", "enquiry", "inquiry"],
    }

    priority = "Low"
    for word in ("urgent", "asap", "immediate", "important", "high priority"):
        if word in text:
            priority = "High"
            break

    for cat, kws in categories.items():
        if any(kw in text for kw in kws):
            log.debug("Fallback classify hit", extra={"category": cat, "priority": priority})
            return {"category": cat, "priority": priority}

    log.debug("Fallback classify default general")
    return {"category": "general", "priority": priority}
