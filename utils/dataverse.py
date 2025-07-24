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

def email_exists_in_dataverse(graph_id, headers):
    base_url = os.getenv("DATAVERSE_RESOURCE")
    table = os.getenv("DATAVERSE_TABLE")
    query_url = f"{base_url}/api/data/v9.2/{table}?$filter=crabb_id eq '{graph_id}'"
    response = requests.get(query_url, headers=headers)
    if response.status_code != 200:
        print(f"[ERROR] Failed to query Dataverse: {response.status_code} - {response.text}")
        return False
    return len(response.json().get("value", [])) > 0

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

    # ✅ Check if email already exists based on crabb_id (Graph email ID)
    if email_exists_in_dataverse(data["id"], headers):
        print(f"[SKIPPED] Email with ID {data['id']} already exists in Dataverse.")
        return

    payload = {
        "crabb_id": data["id"],

        "crabb_sender": data["sender"],
        "crabb_received_from": data["received_from"],
        "crabb_received_at": data["received_at"],

        "crabb_subject": data["subject"],

        "crabb_email_body": data["mail_body"],
        
        "crabb_attachments": ", ".join(data["attachments"]),
        "crabb_attachment_content": data["attachment_text"],

        "crabb_category": data["category"],
        "crabb_priority": data["priority"],
    }

    #print("[DEBUG] Payload to be sent:")
    #print(json.dumps(payload, indent=2))

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code >= 300:
        print(f"[ERROR] Failed to write to Dataverse: {response.status_code}")
        print(f"[ERROR] Response: {response.text}")
    else:
        print(f"[SUCCESS] Entry written to Dataverse: {response.status_code}")
        print(f"[SUCCESS] Response: {response.text}")

