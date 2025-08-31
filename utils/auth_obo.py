"""
OBO (On-Behalf-Of) helper for Microsoft Graph delegated tokens.
- Exchanges the user's API bearer (from your Custom Connector OAuth) for a Graph delegated token.
"""

# utils/auth_obo.py
import os, msal

def _pick(*names):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None

TENANT_ID = _pick("TENANT_ID", "AZURE_TENANT_ID", "DIRECTORY_TENANT_ID")
API_CLIENT_ID = _pick("API_CLIENT_ID", "CLIENT_ID")
API_CLIENT_SECRET = _pick("API_CLIENT_SECRET", "CLIENT_SECRET")

missing = [k for k, v in {
    "TENANT_ID": TENANT_ID,
    "API_CLIENT_ID": API_CLIENT_ID,
    "API_CLIENT_SECRET": API_CLIENT_SECRET,
}.items() if not v]
if missing:
    raise RuntimeError(
        f"Missing required environment variables: {', '.join(missing)}. "
        "Set them in App Service → Configuration using values from the Mail API app registration."
    )

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPH_SCOPES = ["https://graph.microsoft.com/Mail.Read"]

_cca = msal.ConfidentialClientApplication(
    API_CLIENT_ID, authority=AUTHORITY, client_credential=API_CLIENT_SECRET
)

def get_graph_token_obo(user_access_token: str) -> str:
    result = _cca.acquire_token_on_behalf_of(
        user_assertion=user_access_token, scopes=GRAPH_SCOPES
    )
    if "access_token" not in result:
        raise RuntimeError(f"OBO failed: {result.get('error_description') or result}")
    return result["access_token"]

