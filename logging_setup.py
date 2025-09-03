"""
Shared logging for the whole service (Mail Classification API).

Features
- Context vars: service, request_id, graph_id on every record
- Styles:
    LOG_STYLE=json   -> newline-delimited JSON (default; best for Azure ingestion)
    LOG_STYLE=human  -> compact human-readable lines
    LOG_STYLE=both   -> emit both handlers
- Tuning:
    LOG_LEVEL=INFO|DEBUG|...
    SERVICE_NAME=mail-classifier (default)
- Helpers:
    human_kv(dict) to format short key=val lists (with safe truncation)
"""

from __future__ import annotations

import os
import logging
import contextvars
from typing import Any, Mapping, Iterable

try:
    # Keep optional: only needed for JSON style
    from pythonjsonlogger import jsonlogger  # type: ignore
except Exception:  # pragma: no cover
    jsonlogger = None  # json mode won't be available

# ----------------------------
# Context (settable from any module)
# ----------------------------
request_id_var = contextvars.ContextVar("request_id", default=None)
graph_id_var   = contextvars.ContextVar("graph_id", default=None)

def set_graph_id(gid: str | None) -> None:
    graph_id_var.set(gid)

def set_request_id(rid: str | None) -> None:
    request_id_var.set(rid)


import logging, sys, os
def setup_logging():
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        handlers=[logging.StreamHandler(sys.stdout)],
        format="%(message)s",
        force=True,  # override any prior config
    )
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "gunicorn.error", "gunicorn.access"):
        lg = logging.getLogger(name)
        lg.setLevel(level)
        lg.propagate = True

# ----------------------------
# Pretty key/value helper
# ----------------------------
def _short(s: Any, limit: int = 140) -> str:
    """Safely stringify & truncate for single-line logs."""
    if s is None:
        return "-"
    try:
        t = str(s)
    except Exception:
        t = repr(s)
    t = t.replace("\n", " ").replace("\r", " ").strip()
    return t if len(t) <= limit else (t[:limit] + "…")

def human_kv(items: Mapping[str, Any] | Iterable[tuple[str, Any]], sep: str = " ") -> str:
    """Render mapping/iterable as 'k=v' tokens with truncation."""
    try:
        pairs = items.items() if isinstance(items, Mapping) else items
    except Exception:
        return _short(items)
    return sep.join(f"{k}={_short(v)}" for k, v in pairs)

# ----------------------------
# Filters & Formatters
# ----------------------------
class _CtxFilter(logging.Filter):
    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        record.service    = self.service
        record.request_id = request_id_var.get()
        record.graph_id   = graph_id_var.get()
        return True

class _HumanFormatter(logging.Formatter):
    default_msec_format = "%s.%03d"

    def format(self, record: logging.LogRecord) -> str:
        # Base prefix: 2025-08-28 10:36:28,047 INFO mail-classifier main:
        prefix = f"{self.formatTime(record)} {record.levelname} {getattr(record, 'service', '-')}" \
                 f" {record.name}:"
        msg = str(record.getMessage())

        # If message is a simple 'event' tag (e.g., "mail_begin"), append kvs from known attrs
        # so we don’t rely on json extras to be visible.
        extras = []
        for key in ("request_id", "graph_id"):
            val = getattr(record, key, None)
            if val:
                extras.append((key, val))
        # Include user-provided k=v if message payload was a dict-like string; otherwise
        # rely on modules to pass an 'extra' dict under 'kv' key.
        kv = getattr(record, "kv", None)
        if isinstance(kv, Mapping) and kv:
            extras.extend(kv.items())

        line = f"{prefix} {msg}"
        if extras:
            line += " | " + human_kv(extras)
        return line

# ----------------------------
# Init
# ----------------------------
def init_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    service = os.getenv("SERVICE_NAME", "mail-classifier")
    style = os.getenv("LOG_STYLE", "json").lower()  # json | human | both

    root = logging.getLogger()
    # Avoid duplicate handlers on reloads
    if getattr(root, "_initialized_by_app", False):
        return

    root.handlers.clear()
    root.setLevel(level)

    ctx_filter = _CtxFilter(service)

    # Human handler (pretty one-liners)
    if style in ("human", "both"):
        h = logging.StreamHandler()
        h.setFormatter(_HumanFormatter())
        h.addFilter(ctx_filter)
        root.addHandler(h)

    # JSON handler (for Azure ingestion)
    if style in ("json", "both"):
        if jsonlogger is None:
            # Fallback to human if python-json-logger is missing
            h = logging.StreamHandler()
            h.setFormatter(_HumanFormatter())
            h.addFilter(ctx_filter)
            root.addHandler(h)
        else:
            j = logging.StreamHandler()
            fmt = jsonlogger.JsonFormatter(
                "%(asctime)s %(levelname)s %(service)s %(name)s %(message)s %(request_id)s %(graph_id)s"
            )
            j.setFormatter(fmt)
            j.addFilter(ctx_filter)
            root.addHandler(j)

    root._initialized_by_app = True  # type: ignore[attr-defined]
