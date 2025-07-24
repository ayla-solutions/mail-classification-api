import requests
import os
from PyPDF2 import PdfReader
from docx import Document
import pandas as pd
import pdfplumber
from pdf2image import convert_from_bytes
from PIL import ImageEnhance, Image
import pytesseract
from bs4 import BeautifulSoup

def extract_text_from_attachment(file_bytes, name):
    name = name.lower()
    try:
        if name.endswith(".pdf"):
            text = ""
            # Try extracting using pdfplumber
            try:
                with pdfplumber.open(file_bytes) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n"
            except Exception as e:
                print(f"[pdfplumber failed] {e}")

            # If no text found, fallback to OCR
            if not text.strip():
                print("[INFO] Falling back to OCR...")
                images = convert_from_bytes(file_bytes, dpi=300)
                for img in images:
                    img = img.convert("L")  # grayscale
                    img = ImageEnhance.Contrast(img).enhance(2.0)
                    text += pytesseract.image_to_string(img) + "\n"

            return text.strip()
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
    messages_url = f"https://graph.microsoft.com/v1.0/users/{os.getenv('MAILBOX_USER')}/messages?$top=100"

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


        body = msg.get("body", {})
        raw_content = body.get("content", "")
        content_type = body.get("contentType", "")

        if content_type == "html":
            # Convert HTML to plain text
            soup = BeautifulSoup(raw_content, "html.parser")
            mail_body_text = soup.get_text(separator="\n").strip()
        else:
            # Already plain text
            mail_body_text = raw_content.strip()

        results.append({
            "id":  msg.get("id"),
            "sender": msg.get("from", {}).get("emailAddress", {}).get("name", "Unknown"),
            "received_from": msg.get("from", {}).get("emailAddress", {}).get("address", "Unknown"),
            "subject": msg.get("subject"),
            "body_preview": msg.get("bodyPreview"),
            "mail_body": msg.get("body", {}).get("content", ""),
            "received_at": msg.get("receivedDateTime"),
            "attachments": attachment_names,
            "attachment_text": "\n\n".join(extracted_content),
        })

    return results
