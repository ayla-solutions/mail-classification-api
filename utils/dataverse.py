"""
utils/dataverse.py
--------------------------------
Dataverse client for the Mail Classification API.

Business rules (v2):
- Phase 1: create_basic_email_row()  -> minimal, idempotent insert keyed by crabb_id
- Phase 2: update_email_enrichment_text() -> PATCH extracted fields as *TEXT* only

Category-specific behavior:
  • invoice            → write full invoice fields + set crabb_paid = False
  • customer requests  → write ONLY: summary, ticket_number
  • general/misc       → write ONLY: category, priority

Guarantees:
  • No duplicates (lookups by crabb_id, the Graph message ID)
  • Values persisted as strings (Dataverse columns are text), booleans remain booleans
"""

# =========================
# Imports & Setup
# =========================
import os
import json
import requests
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# =========================
# Environment / Constants
# =========================
DV_RESOURCE = (os.getenv("DATAVERSE_RESOURCE") or "").rstrip("/")
DV_TABLE    = os.getenv("DATAVERSE_TABLE")                 # e.g., crabb_arth_main1s
DV_TENANT   = os.getenv("DATAVERSE_TENANT_ID")
DV_CLIENT   = os.getenv("DATAVERSE_CLIENT_ID")
DV_SECRET   = os.getenv("DATAVERSE_CLIENT_SECRET")

# Primary key logical name (adjust to your table if different)
PK_LOGICAL  = os.getenv("DATAVERSE_PRIMARY_ID", "crabb_arth_main1id")

# Column logical names
COL_GRAPH_ID          = "crabb_id"
COL_CATEGORY          = "crabb_category"
COL_PRIORITY          = "crabb_priority"
COL_PAID              = "crabb_paid"

# Invoice columns (TEXT)
COL_INV_NUMBER        = "crabb_invoice_number"
COL_INV_DATE          = "crabb_invoice_date"
COL_INV_DUE_DATE      = "crabb_due_date"
COL_INV_AMOUNT        = "crabb_invoice_amount"
COL_INV_PAYMENT_LINK  = "crabb_payment_link"
COL_INV_BSB           = "crabb_bsb"
COL_INV_ACC_NO        = "crabb_acnt_number"
COL_INV_ACC_NAME      = "crabb_acnt_name"
COL_INV_DESCRIPTION   = "crabb_inv_desc"

# Optional extras (only used if env vars provided)
COL_BILLER_CODE       = "crabb_biller_code"
COL_PAYMENT_REFERENCE = "crabb_payment_reference"

# Request columns (TEXT)
COL_REQ_OVERVIEW      = "crabb_cr_overview" 
COL_REQ_NUMBER        = "crabb_cr_number"    

# NOTE: Meeting / Timesheet columns intentionally unused in the new flow.


# =========================
# Auth & HTTP helpers
# =========================
def _token() -> Optional[str]:
    """Acquire an application token for Dataverse (client credentials)."""
    url = f"https://login.microsoftonline.com/{DV_TENANT}/oauth2/v2.0/token"
    data = {
        "client_id": DV_CLIENT,
        "client_secret": DV_SECRET,
        "grant_type": "client_credentials",
        "scope": f"{DV_RESOURCE}/.default",
    }
    r = requests.post(url, data=data)
    if r.status_code != 200:
        print(f"[DV AUTH ERROR] {r.status_code} - {r.text}")
        return None
    return r.json().get("access_token")


def _headers(tok: str) -> Dict[str, str]:
    """Standard Dataverse headers for Web API."""
    return {
        "Authorization": f"Bearer {tok}",
        "Content-Type": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
    }


def _base() -> str:
    return f"{DV_RESOURCE}/api/data/v9.2"


# =========================
# Lookups (NO DUPES by Graph ID)
# =========================
def get_row_id_by_graph_id(graph_id: str) -> Optional[str]:
    """Return row GUID for crabb_id == graph_id; None if not found."""
    tok = _token()
    if not tok:
        return None
    url = f"{_base()}/{DV_TABLE}?$select={PK_LOGICAL}&$filter={COL_GRAPH_ID} eq '{graph_id}'"
    r = requests.get(url, headers=_headers(tok))
    if r.status_code != 200:
        print(f"[DV QUERY ERROR] {r.status_code} - {r.text}")
        return None
    vals = r.json().get("value", [])
    return vals[0].get(PK_LOGICAL) if vals else None


# =========================
# Phase 1: BASIC CREATE (idempotent)
# =========================
def create_basic_email_row(mail: Dict[str, Any]) -> bool:
    """
    Create a minimal record once (idempotent by crabb_id).
    Writes only obvious, fast fields we already have from Graph.
    """
    if not mail or not mail.get("id"):
        print("[DV CREATE SKIP] mail or mail['id'] missing")
        return False

    # Skip if already exists
    if get_row_id_by_graph_id(mail["id"]):
        print(f"[DV SKIP] exists (crabb_id={mail['id']})")
        return True

    tok = _token()
    if not tok:
        return False

    payload = {
        COL_GRAPH_ID:                mail.get("id"),
        "crabb_sender":              mail.get("sender"),
        "crabb_received_from":       mail.get("received_from"),
        "crabb_received_at":         mail.get("received_at"),
        "crabb_subject":             mail.get("subject"),
        "crabb_email_body":          (mail.get("mail_body_text") or mail.get("mail_body") or mail.get("body_preview") or ""),
        "crabb_attachments":         ", ".join(mail.get("attachments", [])),
        "crabb_attachment_content":  mail.get("attachment_text", ""),
    }

    url = f"{_base()}/{DV_TABLE}"
    r = requests.post(url, headers=_headers(tok), data=json.dumps(payload))
    if r.status_code >= 300:
        print(f"[DV CREATE ERROR] {r.status_code} - {r.text}")
        return False

    print(f"[DV CREATE OK] crabb_id={mail['id']}")
    return True


# =========================
# Phase 2: ENRICHMENT PATCH (TEXT ONLY)
# =========================
def update_email_enrichment_text(graph_id: str, fields: Dict[str, Any]) -> bool:
    """
    PATCH extracted items as TEXT (strings), with category-specific behavior:

      - Normalize "invoices" → "invoice"
      - Always patch category/priority if provided
      - If category == "invoice":
            write all invoice fields (as text) and set crabb_paid = False
      - If category == "customer requests":
            write ONLY summary -> COL_REQ_OVERVIEW, ticket_number -> COL_REQ_NUMBER
      - If category in {"general","misc","miscellaneous"}:
            write nothing else

    Any missing values are simply omitted from the PATCH.
    """
    if not graph_id:
        print("[DV PATCH SKIP] Missing graph_id")
        return False

    row_id = get_row_id_by_graph_id(graph_id)
    if not row_id:
        print(f"[DV PATCH SKIP] No row for crabb_id={graph_id}")
        return False

    tok = _token()
    if not tok:
        return False

    def _s(v: Any) -> Any:
        """Convert non-None values to string; leave booleans alone."""
        if isinstance(v, bool) or v is None:
            return v
        return str(v)

    # --- base labels ---
    category = (fields.get("category") or "").strip()
    if category.lower() == "invoices":
        category = "invoice"

    payload: Dict[str, Any] = {}
    if category:
        payload[COL_CATEGORY] = _s(category)
    if "priority" in fields:
        payload[COL_PRIORITY] = _s(fields.get("priority"))

    cat_l = (category or "").lower()

    # --- invoice: full invoice fields + paid=false ---
    if cat_l == "invoice":
        payload[COL_PAID] = False  # default unpaid for invoices
        if "invoice_number" in fields:   payload[COL_INV_NUMBER] = _s(fields.get("invoice_number"))
        if "invoice_date" in fields:     payload[COL_INV_DATE] = _s(fields.get("invoice_date"))
        if "due_date" in fields:         payload[COL_INV_DUE_DATE] = _s(fields.get("due_date"))
        if "invoice_amount" in fields:   payload[COL_INV_AMOUNT] = _s(fields.get("invoice_amount"))
        if "payment_link" in fields:     payload[COL_INV_PAYMENT_LINK] = _s(fields.get("payment_link"))
        if "bsb" in fields:              payload[COL_INV_BSB] = _s(fields.get("bsb"))
        if "account_number" in fields:   payload[COL_INV_ACC_NO] = _s(fields.get("account_number"))
        if "account_name" in fields:     payload[COL_INV_ACC_NAME] = _s(fields.get("account_name"))
        # Optional extras (env-controlled)
        if COL_BILLER_CODE and ("biller_code" in fields):
            payload[COL_BILLER_CODE] = _s(fields.get("biller_code"))
        if COL_PAYMENT_REFERENCE and ("payment_reference" in fields):
            payload[COL_PAYMENT_REFERENCE] = _s(fields.get("payment_reference"))
        if COL_INV_DESCRIPTION and ("description" in fields):
            payload[COL_INV_DESCRIPTION] = _s(fields.get("description"))

    # --- customer requests: ONLY summary + ticket_number ---
    elif cat_l in {"customer requests", "customer request"}:
        # accept either key names from extractor; we store only these two
        if ("summary" in fields) or ("overview" in fields):
            payload[COL_REQ_OVERVIEW] = _s(fields.get("summary") or fields.get("overview"))
        if ("ticket_number" in fields) or ("request_number" in fields):
            payload[COL_REQ_NUMBER] = _s(fields.get("ticket_number") or fields.get("request_number"))

    # --- general / misc: nothing else (category/priority already handled) ---
    # (no-op)

    if not payload:
        print(f"[DV PATCH NOOP] Nothing to update for crabb_id={graph_id}")
        return True

    url = f"{_base()}/{DV_TABLE}({row_id})"
    r = requests.patch(url, headers=_headers(tok), data=json.dumps(payload))
    if r.status_code not in (204, 1223):  # 1223 seen by some clients for 204
        print(f"[DV PATCH ERROR] {r.status_code} - {r.text}")
        return False

    print(f"[DV PATCH OK] crabb_id={graph_id} fields={list(payload.keys())}")
    return True
