"""
utils/classify.py
-----------------
Very simple keyword fallback classifier (used only if extractor fails).
Returns: {"category": <str>, "priority": <str>}
"""

# =========================
# Public: classify_mail
# =========================
def classify_mail(mail: dict) -> dict:
    """
    Cheap keyword heuristic:
      - Category uses lower-case canonical names (e.g., 'invoice')
      - Priority 'High' if urgency words are present, else 'Low'
    """
    subject = (mail.get("subject") or "").lower()
    body    = (mail.get("body_preview") or "").lower()
    attn    = " ".join(mail.get("attachments", [])).lower()
    text = f"{subject} {body} {attn}"

    categories = {
        "invoice":              ["invoice", "bill", "statement"],
        "service request":      ["issue", "support", "ticket"],
        "team member request":  ["access", "permission", "request"],
        "customer request":     ["client", "customer", "enquiry", "inquiry"],
        "meeting":              ["meeting", "calendar", "invite"],
        "timesheets":           ["timesheet", "approval", "work hours"],
    }

    priority = "Low"
    for word in ("urgent", "asap", "immediate", "important", "high priority"):
        if word in text:
            priority = "High"
            break

    for cat, kws in categories.items():
        if any(kw in text for kw in kws):
            return {"category": cat, "priority": priority}

    return {"category": "general", "priority": priority}
