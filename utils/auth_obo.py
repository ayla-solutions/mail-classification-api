"""
OBO (On-Behalf-Of) helper for Microsoft Graph delegated tokens.
- Exchanges the user's API bearer (from your Custom Connector OAuth) for a Graph delegated token.
"""

import os
import msal

TENANT_ID = os.getenv("TENANT_ID")
API_CLIENT_ID = os.getenv("API_CLIENT_ID") or os.getenv("CLIENT_ID")   # prefer dedicated vars
API_CLIENT_SECRET = os.getenv("API_CLIENT_SECRET") or os.getenv("CLIENT_SECRET")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPH_SCOPES = ["https://graph.microsoft.com/Mail.Read"]

_cca = msal.ConfidentialClientApplication(
    API_CLIENT_ID,
    authority=AUTHORITY,
    client_credential=API_CLIENT_SECRET,
)

def get_graph_token_obo(user_access_token: str) -> str:
    """
    Exchange the user's API bearer for a Graph delegated token (OBO).
    Raises RuntimeError with MSAL response on failure.
    """
    result = _cca.acquire_token_on_behalf_of(
        user_assertion=user_access_token,
        scopes=GRAPH_SCOPES,
    )
    if "access_token" not in result:
        raise RuntimeError(f"OBO failed: {result.get('error_description') or result}")
    return result["access_token"]
