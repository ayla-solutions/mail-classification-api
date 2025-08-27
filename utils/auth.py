"""
utils/auth.py
-------------
Graph (Microsoft 365) authentication helpers.
"""

# =========================
# Imports & Config
# =========================
import os
import requests
from dotenv import load_dotenv

# Load .env for local runs; in Docker you can pass env vars directly
load_dotenv()

TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"


# =========================
# Public: App Token for Graph
# =========================
def get_graph_token() -> str:
    """
    Client-credentials flow → app-only Graph access token.
    """
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": GRAPH_SCOPE,
    }
    res = requests.post(AUTH_URL, data=data)
    res.raise_for_status()
    return res.json()["access_token"]
