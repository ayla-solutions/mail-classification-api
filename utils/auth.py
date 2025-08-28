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
import logging
from dotenv import load_dotenv
from logging_setup import init_logging

load_dotenv()
init_logging()
log = logging.getLogger("utils.auth")

TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

def get_graph_token() -> str:
    """Client-credentials flow → app-only Graph access token."""
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": GRAPH_SCOPE,
    }
    log.debug("Requesting Graph token", extra={"tenant": TENANT_ID})
    res = requests.post(AUTH_URL, data=data)
    try:
        res.raise_for_status()
        log.debug("Graph token ok")
        return res.json()["access_token"]
    except Exception:
        log.exception("Graph auth failed: %s", res.text[:500])
        raise
