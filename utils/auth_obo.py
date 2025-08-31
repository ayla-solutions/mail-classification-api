# utils/auth_obo.py
"""
On-Behalf-Of (OBO) helper for Microsoft Graph.

Key fixes:
- No more import-time crash: env vars are validated lazily (when a token is requested).
- Clear, actionable error messages bubble up to the API layer.
"""

from __future__ import annotations

import os
import msal
from typing import Optional


# ---- Internal lazy singleton -------------------------------------------------

_CCA: Optional[msal.ConfidentialClientApplication] = None
_TENANT_ID: Optional[str] = None
_API_CLIENT_ID: Optional[str] = None
_API_CLIENT_SECRET: Optional[str] = None


def _ensure_config() -> None:
    """Create the MSAL ConfidentialClientApplication lazily and validate env."""
    global _CCA, _TENANT_ID, _API_CLIENT_ID, _API_CLIENT_SECRET
    if _CCA is not None:
        return

    # Read without throwing at import time
    _TENANT_ID = os.getenv("TENANT_ID")
    _API_CLIENT_ID = os.getenv("API_CLIENT_ID") or os.getenv("CLIENT_ID")
    _API_CLIENT_SECRET = os.getenv("API_CLIENT_SECRET") or os.getenv("CLIENT_SECRET")

    missing = [k for k, v in {
        "TENANT_ID": _TENANT_ID,
        "API_CLIENT_ID": _API_CLIENT_ID,
        "API_CLIENT_SECRET": _API_CLIENT_SECRET,
    }.items() if not v]

    if missing:
        # Raise here (only when actually needed), not during module import
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
            + ". Set them in App Service → Configuration with values from the Mail API app registration."
        )

    authority = f"https://login.microsoftonline.com/{_TENANT_ID}"
    try:
        _CCA = msal.ConfidentialClientApplication(
            client_id=_API_CLIENT_ID,
            client_credential=_API_CLIENT_SECRET,
            authority=authority,
        )
    except ValueError as e:
        # Surface invalid tenant/authority early
        raise RuntimeError(
            f"MSAL configuration error for authority '{authority}': {e}"
        ) from e


# ---- Public API --------------------------------------------------------------

_GRAPH_SCOPES = ["https://graph.microsoft.com/.default"]


def get_graph_token_obo(user_assertion: str) -> str:
    """
    Exchange a user token (from Authorization header or Swagger OAuth) for a Graph token
    using the OAuth2 On-Behalf-Of flow.

    Returns:
        access token (str)

    Raises:
        RuntimeError with a descriptive message on configuration or token failures.
    """
    if not user_assertion:
        raise RuntimeError("No user assertion provided for OBO exchange.")

    _ensure_config()

    assert _CCA is not None
    result = _CCA.acquire_token_on_behalf_of(
        user_assertion=user_assertion,
        scopes=_GRAPH_SCOPES,
    )

    if "access_token" in result:
        return result["access_token"]

    # Compose a helpful error
    err = result.get("error")
    desc = result.get("error_description")
    corr = result.get("correlation_id")
    raise RuntimeError(
        f"OBO failed: {err}: {desc} (correlation_id={corr})"
    )
