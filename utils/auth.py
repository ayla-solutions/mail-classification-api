"""
utils/auth.py
-------------
Graph (Microsoft 365) authentication helpers + Multi-tenant AAD JWT validation.
"""

# =========================
# Imports & Config
# =========================
import os
import logging
import requests
import httpx
from jose import jwt
from cachetools import TTLCache
from dotenv import load_dotenv
from logging_setup import init_logging
from fastapi import Request, HTTPException, status

load_dotenv()
init_logging()
log = logging.getLogger("utils.auth")

# ----- App-only (client credentials) for Graph -----
TENANT_ID      = os.getenv("TENANT_ID")   # tenant to acquire app-only token against
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
AUTH_URL       = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_SCOPE    = "https://graph.microsoft.com/.default"

def get_graph_token() -> str:
    """Client-credentials flow → app-only Graph access token (for backend to call Graph)."""
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

# ----- Inbound multi-tenant JWT validation (for protecting your API) -----
API_AUDIENCE       = os.getenv("API_AUDIENCE", "api://837228e1-de28-4329-8c48-c4374694437c")
API_ALLOWED_SCOPE  = os.getenv("API_ALLOWED_SCOPE", "access_as_user")
# Optional: allowlist of tenant IDs (comma-separated). Leave empty to allow any AAD tenant.
ALLOWED_TENANTS    = {t.strip() for t in os.getenv("ALLOWED_TENANTS", "").split(",") if t.strip()}

# Cache per-tenant OpenID config and JWKS (24h)
_cfg_cache: TTLCache = TTLCache(maxsize=1024, ttl=24 * 3600)
_jwks_cache: TTLCache = TTLCache(maxsize=1024, ttl=24 * 3600)
_http = httpx.AsyncClient(timeout=10.0)

def _extract_bearer(auth_header: str | None) -> str:
    if not auth_header or not auth_header.lower().startswith("bearer "):
        raise ValueError("Missing or invalid Authorization header")
    return auth_header.split(" ", 1)[1].strip()

async def _openid_config_for(tid: str) -> dict:
    key = f"cfg:{tid}"
    if key in _cfg_cache:
        return _cfg_cache[key]
    url = f"https://login.microsoftonline.com/{tid}/v2.0/.well-known/openid-configuration"
    r = await _http.get(url)
    r.raise_for_status()
    cfg = r.json()
    _cfg_cache[key] = cfg
    return cfg

async def _jwks_for(tid: str) -> dict:
    key = f"jwks:{tid}"
    if key in _jwks_cache:
        return _jwks_cache[key]
    cfg = await _openid_config_for(tid)
    r = await _http.get(cfg["jwks_uri"])
    r.raise_for_status()
    jwks = r.json()
    _jwks_cache[key] = jwks
    return jwks

async def validate_aad_bearer(auth_header: str) -> dict:
    """
    Validate Authorization: Bearer <token> from ANY AAD tenant (multi-tenant).
    Enforces:
      - issuer  = https://login.microsoftonline.com/{tid}/v2.0
      - audience in { api://<app-id-guid>, <app-id-guid> }
      - scope   contains API_ALLOWED_SCOPE
    Returns decoded claims on success.
    """
    token = _extract_bearer(auth_header)

    # Peek unverified claims to learn tenant (tid) and issuer
    unverified = jwt.get_unverified_claims(token)
    tid = unverified.get("tid")
    iss = unverified.get("iss")
    if not tid or not iss or not iss.startswith(f"https://login.microsoftonline.com/{tid}/v2.0"):
        raise ValueError("Invalid issuer/tenant in token")

    # Optional per-tenant allowlist
    if ALLOWED_TENANTS and tid not in ALLOWED_TENANTS:
        raise ValueError("Tenant not allowed")

    # Fetch tenant-specific signing keys (JWKS)
    jwks = await _jwks_for(tid)
    header = jwt.get_unverified_header(token)
    kid = header.get("kid")
    key = next((k for k in jwks["keys"] if k.get("kid") == kid), None)
    if not key:
        _jwks_cache.pop(f"jwks:{tid}", None)  # refresh once
        jwks = await _jwks_for(tid)
        key = next((k for k in jwks["keys"] if k.get("kid") == kid), None)
        if not key:
            raise ValueError("Signing key not found")

    # Build accepted audience forms
    aud_uri = API_AUDIENCE                               # e.g., "api://8372...4437c"
    aud_guid = aud_uri.removeprefix("api://") if aud_uri.startswith("api://") else aud_uri
    accepted_aud = {aud_uri, aud_guid}

    # Decode with issuer/exp but WITHOUT built-in audience verification
    claims = jwt.decode(
        token,
        key,
        algorithms=[key.get("alg", "RS256"), "RS256"],
        issuer=f"https://login.microsoftonline.com/{tid}/v2.0",
        options={"verify_exp": True, "verify_aud": False},   # <-- important change
    )

    # Manual audience check supporting both forms + list-or-str shapes
    aud_claim = claims.get("aud")
    if isinstance(aud_claim, (list, tuple)):
        if not any(a in accepted_aud for a in aud_claim):
            raise ValueError("Invalid audience")
    else:
        if aud_claim not in accepted_aud:
            raise ValueError("Invalid audience")

    # Scope check
    scopes = (claims.get("scp") or "").split(" ")
    if API_ALLOWED_SCOPE not in scopes:
        raise ValueError("Insufficient scope")

    log.debug(
        "AAD token validated",
        extra={"caller_tid": tid, "caller_oid": claims.get("oid"), "scp": scopes, "aud": aud_claim},
    )

    # ------------------------------------------------------------------
    # 🔒 Optional: Tenant allowlist (how to enable later)
    #
    # By default, this API accepts tokens from ANY Azure AD tenant
    # (multi-tenant setup). If you want to restrict which tenants
    # are allowed to call your API, you can enable the allowlist check.
    #
    # 1) In Azure App Service → Configuration (or .env), set:
    #       ALLOWED_TENANTS=<tenant-guid-1>,<tenant-guid-2>,...
    #
    #    Example:
    #       ALLOWED_TENANTS=11111111-1111-1111-1111-111111111111,22222222-2222-2222-2222-222222222222
    #
    # 2) (Already active above) This code checks the tid against ALLOWED_TENANTS.
    #    If not present, any tenant is accepted. If present, only those tenants work.
    #
    # 3) Restart the API after adding/updating the env var.
    #
    # This is useful for piloting with specific customers before opening broadly.
    # ------------------------------------------------------------------
    return claims

# ---------- FastAPI dependency wrapper (use in routes) ----------
async def require_aad_token(request: Request):
    """
    FastAPI dependency wrapper around validate_aad_bearer.
    Use this in your routes with:  claims: dict = Depends(require_aad_token)
    """
    try:
        return await validate_aad_bearer(request.headers.get("Authorization"))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )
