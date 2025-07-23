import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

def get_dataverse_token():
    url = f"https://login.microsoftonline.com/{os.getenv('DATAVERSE_TENANT_ID')}/oauth2/v2.0/token"
    data = {
        "client_id": os.getenv("DATAVERSE_CLIENT_ID"),
        "client_secret": os.getenv("DATAVERSE_CLIENT_SECRET"),
        "grant_type": "client_credentials",
        "scope": f"{os.getenv('DATAVERSE_RESOURCE')}/.default"
    }
    response = requests.post(url, data=data)
    if response.status_code != 200:
        print(f"[ERROR] Failed to get token: {response.status_code} - {response.text}")
        return None
    return response.json().get("access_token")

def push_to_dataverse(data):
    token = get_dataverse_token()
    if not token:
        print("[ERROR] No access token received. Skipping write.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0"
    }

    url = f"{os.getenv('DATAVERSE_RESOURCE')}/api/data/v9.2/{os.getenv('DATAVERSE_TABLE')}"
    print(f"[DEBUG] Writing to Dataverse: {data['subject']}")

    payload = {
        "crabb_subject": data["subject"],
        "crabb_category": data["category"],
        "crabb_priority": data["priority"],
        "crabb_email_body": data["body_preview"],
        "crabb_received_at": data["received_at"],
        "crabb_received_from": data["sender"],
        "crabb_attachments": ", ".join(data["attachments"]),
        "crabb_attachment_content": data["attachment_text"]
    }

    print("[DEBUG] Payload to be sent:")
    print(json.dumps(payload, indent=2))

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code >= 300:
        print(f"[ERROR] Failed to write to Dataverse: {response.status_code}")
        print(f"[ERROR] Response: {response.text}")
    else:
        print(f"[SUCCESS] Entry written to Dataverse: {response.status_code}")
        print(f"[SUCCESS] Response: {response.text}")

