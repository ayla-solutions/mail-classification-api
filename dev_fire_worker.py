# dev_fire_worker.py
# ----------------------------------------------
# Drives end-to-end LOCAL tests:
#   - Phase 1: create_basic_email_row (Dataverse row by crabb_id)
#   - Phase 2: enrich_and_patch_dataverse (calls local extractor @ 8010)
#
# Make sure your PowerShell has:
#   EXTRACTOR_URL=http://localhost:8010
#   EXTRACTOR_EXTRACT_PATH=/extract
#   EXTRACTOR_HEALTH_PATH=/health
#   EXTRACTOR_TIMEOUT_SEC=300
# And real Dataverse creds set if you want to persist results.
# ----------------------------------------------

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from utils.dataverse import create_basic_email_row
from utils.extractor_worker import enrich_and_patch_dataverse

def fire(mail):
    print("\n=== TEST MAIL:", mail.get("id"), "===")
    # Phase 1: ensure DV row exists (idempotent by crabb_id)
    create_basic_email_row(mail)
    # Phase 2: call extractor → flatten → PATCH DV
    enrich_and_patch_dataverse(mail)

# ===============================
# Complex TEST CASES
# ===============================
tests = [

    # ---------- CUSTOMER REQUESTS ----------

    # CR-High: Explicit urgency + hard deadline same-day
    {
        "id": "CRQ-offboard-urgent-001",
        "subject": "URGENT — Terminate access TODAY: Sandeep Rao (contract ended)",
        "received_at": "2025-08-27T06:50:00Z",
        "mail_body_text": """Hi IT,

Please remove ALL access for contractor Sandeep Rao IMMEDIATELY, before 17:00 AEST today.
Systems: Okta, VPN, GitHub (ayla-org ALL TEAMS), Azure DevOps (all projects), GDrive shared folders.

Reason: contract ended early.
""",
        "attachment_text": """OffboardingChecklist.pdf (text):
Name: Sandeep Rao
Email: sandeep.rao@contractor.co
End Date: 27-08-2025
Manager: Anita Sen
"""
    },

    # CR-Medium: Due-by mention, no "urgent"
    {
        "id": "CRQ-export-medium-002",
        "subject": "Vendor payments export — due by 29/08/2025 COB",
        "received_at": "2025-08-27T02:15:00Z",
        "mail_body_text": """Hello Support,

We need a CSV export of vendor payments between 01/07/2024 and 30/06/2025.
Columns: Invoice No, Payment Date, Amount, Vendor, Reference.

Please provide by 29/08/2025 (COB). No escalation expected.
""",
        "attachment_text": """example-format.csv:
invoice_number,payment_date,amount,vendor,reference
INV-1001,2024-07-14,560.00,ACME PTY LTD,AC-560
"""
    },

    # CR-Low: Multi-ask onboarding (no urgency)
    {
        "id": "CRQ-onboard-low-003",
        "subject": "Access request — BI contractor onboarding (start 04-09-2025)",
        "received_at": "2025-08-26T23:12:00Z",
        "mail_body_text": """Hi team,

Please onboard BI contractor (Jamie Lee) starting 04-09-2025.
Requests:
 - VPN
 - Okta (SSO)
 - Snowflake (read on FINANCE_DB, read/write on SANDBOX_DB)
 - GitHub: ayla-org/analytics (read + issue create)

Not urgent; anytime this week is fine.
""",
        "attachment_text": """onboarding-details.docx (extracted):
Name: Jamie Lee
Manager: Priya Sharma
Role: BI Contractor
Start: 04-09-2025
"""
    },

    # CR-Quoted thread: Ensure top request is summarized, not the quoted history
    {
        "id": "CRQ-thread-004",
        "subject": "Follow-up: data retention policy exceptions",
        "received_at": "2025-08-25T10:42:00Z",
        "mail_body_text": """Hi Legal/IT,

Can we approve a 90-day exception to retain S3 logs for the customer audit?
Business justification: investigation window overlaps financial close.

Please advise approver and next steps.

-----Original Message-----
From: Audit Team
We might need longer retention...
""",
        "attachment_text": ""
    },

    # ---------- INVOICES ----------

    # INV: Attachment vs Body conflict → prefer attachment text
    {
        "id": "INV-conflict-001",
        "subject": "Invoice INV-9001 — Please process",
        "received_at": "2025-08-20T11:00:00Z",
        "mail_body_text": """Hi,
Invoice #INV-9002 total AUD 1,300.00 (this is WRONG in body; attachment has correct values).
""",
        "attachment_text": """ACME PTY LTD
Tax Invoice  INV-9001
Invoice Date  19-08-2025
Due Date      28-08-2025
TOTAL (AUD)   1,250.00

Bank Transfer:
  Account Name: ACME PTY LTD
  BSB: 123-456
  Account Number: 00112233

BPAY:
  Biller Code: 123456
  Reference: 987654
"""
    },

    # INV: BPAY only (no bank), payment link with query params
    {
        "id": "INV-bpay-link-002",
        "subject": "Harbour IT Services – Invoice HITS-2025-0815",
        "received_at": "2025-08-18T07:15:00Z",
        "mail_body_text": "Hello, attached invoice for managed services.",
        "attachment_text": """Harbour IT Services
Invoice No: HITS-2025-0815
Invoice Date: 15-08-2025
Due Date: 10-09-2025
Total (AUD): 2,849.90

Pay online:
  https://pay.harbourit.com/invoice?id=HITS-2025-0815&src=email

BPAY:
  Biller Code: 654321
  Reference: AYLA8899
"""
    },

    # INV: OCR-ish noise & spaced BSB
    {
        "id": "INV-ocr-noise-003",
        "subject": "Payment Reminder — Inv—777A (Overdue)",
        "received_at": "2025-08-22T03:01:00Z",
        "mail_body_text": "Our records indicate the attached invoice remains unpaid.",
        "attachment_text": """NORTH SHORE ENERGY
TAX INVOICE

Invo1ce No : INV—777A
Invoice   Date : 08/15/2025
Due   Date : 05/09/2025
TOTAL DUE: AUD 1,987.35

Payment Options:
Bank:
  Acc Name: North Shore Energy Pty Ltd
  B S B : 11 22 33
  Account: 0045 6678

Online:
  Payment Link : http ://pay.nse.com / xyz

BPAY:
  BillerCode: 654321
  Ref: AYLA-INV777A
"""
    },

    # INV: Missing due date (should return null for due_date)
    {
        "id": "INV-missing-due-004",
        "subject": "Tax Invoice INV-2044",
        "received_at": "2025-08-19T11:22:00Z",
        "mail_body_text": "Please see attached invoice.",
        "attachment_text": """ACME CONSULTING
Tax Invoice: INV-2044
Invoice Date: 18-08-2025
TOTAL (AUD): 990.00

Bank Transfer:
  Account Name: ACME CONSULTING
  BSB: 062-000
  Account Number: 12345678
"""
    },

    # INV: Two totals in the table vs grand total line
    {
        "id": "INV-two-totals-005",
        "subject": "Invoice 2025-00088 (Services)",
        "received_at": "2025-08-21T08:00:00Z",
        "mail_body_text": "Hi finance, as discussed.",
        "attachment_text": """Vendor XYZ Pty Ltd
Invoice No: 2025-00088
Invoice Date: 20-08-2025
Due Date: 03-09-2025

Items:
  Subscription (Aug)              1    900.00      900.00
  Overage usage                   -    -           350.00
Subtotal:                                      1,250.00
GST (0%):                                          0.00
TOTAL AUD:                                     1,250.00

Payment:
  Account Name: XYZ PTY LTD
  BSB: 013-333
  Account Number: 99887766
"""
    },

    # ---------- GENERAL ----------

    # GEN: Meeting minutes – FYI only
    {
        "id": "GEN-minutes-001",
        "subject": "Minutes — Architecture Guild (25 Aug)",
        "received_at": "2025-08-25T09:30:00Z",
        "mail_body_text": """Hi all,
Minutes attached for your information. No actions assigned outside the guild.""",
        "attachment_text": """arch-guild-minutes.pdf (text):
Agenda:
- Event-driven patterns in billing
- Decomp of monolith service 'Ledger'
- Observability baselines
Action items: (guild-internal only)
"""
    },

    # GEN: Newsletter w/ roadmap – Non-actionable
    {
        "id": "GEN-news-aug-002",
        "subject": "August Engineering Newsletter — platform roadmap & observability wins",
        "received_at": "2025-08-16T09:00:00Z",
        "mail_body_text": """Team,
Highlights: rolled out meshes, reduced P95 by 27%. Roadmap attached. No action required.""",
        "attachment_text": """Roadmap_Summary_Aug.pdf (text):
Q4 Targets:
- Reduce infra cost by 8-10%
- Decommission legacy namespace
- Multi-tenant isolation improvements
"""
    },

    # ---------- MISC ----------

    # MISC: Automated CI bot (system notification)
    {
        "id": "MISC-ci-001",
        "subject": "Build failure — Pipeline #342 on main (Commit 8b1e3c7)",
        "received_at": "2025-08-21T05:00:00Z",
        "mail_body_text": """This is an automated notification.
Pipeline #342 failed on step 'integration-tests'. Logs attached.""",
        "attachment_text": """ci_logs.txt:
[ERROR] Timeout while waiting for container readiness on port 8080
[INFO] Retrying… attempt 3/3
[FAIL] Exceeded retries
"""
    },
]

if __name__ == "__main__":
    for m in tests:
        fire(m)
