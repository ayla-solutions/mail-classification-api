import requests
import os
from PyPDF2 import PdfReader
from docx import Document
import pandas as pd

def extract_text_from_attachment(file_bytes, name):
    name = name.lower()
    try:
        if name.endswith(".pdf"):
            reader = PdfReader(file_bytes)
            return "\n".join([p.extract_text() for p in reader.pages if p.extract_text()])
        elif name.endswith(".docx"):
            doc = Document(file_bytes)
            return "\n".join([p.text for p in doc.paragraphs])
        elif name.endswith(".xlsx"):
            df = pd.read_excel(file_bytes)
            return df.to_string()
    except Exception as e:
        return f"[ERROR reading attachment: {e}]"
    return "[Unsupported File]"

def fetch_messages_with_attachments(token):
    headers = {"Authorization": f"Bearer {token}"}
    messages_url = f"https://graph.microsoft.com/v1.0/users/{os.getenv('MAILBOX_USER')}/messages?$top=10"

    res = requests.get(messages_url, headers=headers)
    data = res.json()

    results = []
    for msg in data.get("value", []):
        msg_id = msg["id"]
        attachments_url = f"https://graph.microsoft.com/v1.0/users/{os.getenv('MAILBOX_USER')}/messages/{msg_id}/attachments"

        atts = requests.get(attachments_url, headers=headers).json().get("value", [])
        attachment_names = []
        extracted_content = []

        for att in atts:
            name = att.get("name", "")
            if att.get("@odata.mediaContentType"):
                attachment_url = att.get("@odata.mediaReadLink")
                if attachment_url and attachment_url.startswith("https"):
                    content = requests.get(attachment_url, headers=headers).content
                else:
                    content = b''  # or fallback to empty or skip
                text = extract_text_from_attachment(content, name)
                extracted_content.append(text)
            attachment_names.append(name)

        results.append({
            "subject": msg.get("subject"),
            "body_preview": msg.get("bodyPreview"),
            "received_at": msg.get("receivedDateTime"),
            "attachments": attachment_names,
            "attachment_text": "\n\n".join(extracted_content),
            "sender": msg.get("from", {}).get("emailAddress", {}).get("address", "Unknown")
        })

    return results
