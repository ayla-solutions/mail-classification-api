def classify_mail(mail):
    subject = mail.get("subject", "").lower()
    body = mail.get("body_preview", "").lower()
    attachment_names = " ".join(mail.get("attachments", [])).lower()

    text = f"{subject} {body} {attachment_names}"

    categories = {
        "Invoices": ["invoice", "bill", "statement"],
        "Service Requests": ["issue", "support", "ticket"],
        "Team Member Requests": ["access", "request", "permission"],
        "Customer Requests": ["client", "customer", "enquiry"],
        "Meeting Requests": ["meeting", "calendar", "invite"],
        "Timesheet Approvals": ["timesheet", "approval", "work hours"],
    }

    priority = "Low"
    for word in ["urgent", "asap", "immediate", "important"]:
        if word in text:
            priority = "High"

    for category, keywords in categories.items():
        if any(kw in text for kw in keywords):
            return {"category": category, "priority": priority}

    return {"category": "General", "priority": priority}
