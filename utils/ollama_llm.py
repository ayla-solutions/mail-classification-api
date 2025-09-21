"""
Local LLM classification and extraction utilities
=================================================

This module encapsulates the core logic formerly housed in the
``mail‑ollama‑extractor`` service.  It exposes a set of Pydantic
models and helper functions to perform in‑process classification and
structured extraction of business emails using an Ollama large
language model (LLM).

Key features:
* Self‑contained (no HTTP calls) and configurable via environment vars.
* Deterministic seeding for reproducibility.
* Regex fallback for invoices when few fields are extracted.
* Verbose JSON‑style logging for every operation (invocation, completion,
  fallbacks), including elapsed time and hashes for correlation.
"""

from __future__ import annotations

import os
import json
import re
import hashlib
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any, Iterable, Type, Literal

from pydantic import BaseModel, Field
from dotenv import load_dotenv
import ollama

# Load environment variables from a .env file if present
load_dotenv()

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
log = logging.getLogger("utils.ollama_llm")

# ---------------------------------------------------------------------------
# Model and Ollama client configuration
# ---------------------------------------------------------------------------
OLLAMA_HOST: str = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
CLASSIFIER_MODEL: str = os.getenv("CLASSIFIER_MODEL", "mail-classifier-small")
INVOICE_MODEL: str    = os.getenv("INVOICE_MODEL",   "invoice-extractor-small")
REQUEST_MODEL: str    = os.getenv("REQUEST_MODEL",   "request-summarizer-small")

def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

OLLAMA_TEMPERATURE: float = _get_float("OLLAMA_TEMPERATURE", 0.0)
OLLAMA_NUM_PREDICT: int   = _get_int("OLLAMA_NUM_PREDICT", 200)
OLLAMA_NUM_CTX: int       = _get_int("OLLAMA_NUM_CTX", 3072)
OLLAMA_KEEP_ALIVE: str    = os.getenv("OLLAMA_KEEP_ALIVE", "30m")
INVOICE_NUM_PREDICT: int = _get_int("INVOICE_NUM_PREDICT", 400)

# Instantiate the Ollama client once per module
client = ollama.Client(host=OLLAMA_HOST)

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
Category = Literal["General", "Invoice", "Customer Requests", "Misc"]
Priority = Literal["High", "Medium", "Low"]

class ExtractIn(BaseModel):
    """Schema for inbound classification or extraction requests.

    Only ``body_text`` is required.  When provided, ``attachments_text``
    should be a list of plain text strings derived from file
    attachments.  ``graph_id`` and ``subject`` are included for
    correlation and deterministic seeding but are optional.
    """
    body_text: str = Field(..., description="Plain text of the email body")
    attachments_text: Optional[List[str]] = Field(
        default=None,
        description="List of plain‑text contents from attachments",
    )
    graph_id: Optional[str] = Field(
        default=None,
        description="Microsoft Graph message id (used for deterministic seeding)",
    )
    subject: Optional[str] = Field(default=None, description="Email subject")
    received_at: Optional[str] = Field(
        default=None,
        description="ISO‑8601 timestamp used for ticket date generation",
    )

class Classification(BaseModel):
    """Result of the classification stage."""
    category: Category
    priority: Priority

class InvoiceFields(BaseModel):
    """Structured invoice extraction result."""
    invoice_number: Optional[str] = None
    invoice_date:   Optional[str] = None
    due_date:       Optional[str] = None
    invoice_amount: Optional[str] = None
    payment_link:   Optional[str] = None
    bsb:            Optional[str] = None
    account_number: Optional[str] = None
    account_name:   Optional[str] = None
    biller_code:    Optional[str] = None
    payment_reference: Optional[str] = None
    description:    Optional[str] = None

class RequestFields(BaseModel):
    """Structured customer request summary."""
    summary: str
    ticket_number: Optional[str] = None

class ExtractOut(BaseModel):
    """Wrapper for the response returned by ``run_local_extraction``."""
    ok: bool = True
    data: Dict[str, Any]

# ---------------------------------------------------------------------------
# Helpers for text manipulation and deterministic behaviours
# ---------------------------------------------------------------------------
def trim_text(s: str, max_chars: int) -> str:
    """Return the first ``max_chars`` characters of ``s`` or an empty string."""
    if not s:
        return ""
    return s.strip()[:max_chars]

MAX_CHARS_EXTRACT: int = _get_int("EXTRACTOR_MAX_CHARS", 12000)
MAX_CHARS_CLASSIFY: int = _get_int("CLASSIFY_MAX_CHARS", 4000)

def title_case(s: Optional[str]) -> str:
    """Convert a string to title case (capitalise each word)."""
    s = (s or "").strip()
    if not s:
        return s
    return " ".join(w.capitalize() for w in s.split())

def compose_email_text(
    subject: Optional[str],
    body_text: str,
    attachments_text: Optional[Iterable[str]],
) -> str:
    """Construct a prompt by concatenating subject, body and attachments."""
    parts: List[str] = []
    if subject:
        parts.append(f"Subject: {subject.strip()}")
    parts.append("Email Body:")
    parts.append(body_text.strip())
    att = attachments_text or []
    if att:
        parts.append("\nAttachments:")
        for i, a in enumerate(att, start=1):
            snippet = (a or "").strip()
            if snippet:
                parts.append(f"--- Attachment {i} ---\n{snippet}")
    return "\n\n".join(parts)

def yyyymmdd_from_iso(iso: Optional[str]) -> str:
    """Convert an ISO 8601 timestamp to YYYYMMDD, falling back to today."""
    try:
        dt = datetime.fromisoformat((iso or "").replace("Z", "+00:00"))
    except Exception:
        dt = datetime.utcnow()
    return dt.strftime("%Y%m%d")

def compute_ticket(date_yyyymmdd: str, seed: str, prefix: Optional[str] = None) -> str:
    """Generate a deterministic ticket number from a date and seed."""
    env_prefix = os.getenv("TICKET_PREFIX")
    prefix = env_prefix if env_prefix is not None else (prefix or "REQ-")
    alnums = "".join(ch for ch in (seed or "") if ch.isalnum()).upper()
    last6 = alnums[-6:] if len(alnums) >= 6 else alnums
    return f"{prefix}{date_yyyymmdd}-{last6}"

def sha256_8(s: str) -> str:
    """Compute a short SHA256 digest for correlation logging."""
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:8]

# ---------------------------------------------------------------------------
# JSON extraction helpers
# ---------------------------------------------------------------------------
def _strip_code_fences(s: str) -> str:
    """Remove leading/trailing triple backtick fences from a response."""
    s = s.strip()
    if s.startswith("```"):
        lines = s.splitlines()
        lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return s

def _first_complete_json(s: str) -> Optional[str]:
    """Return the first valid JSON object/array found in ``s`` or None."""
    start_idx: Optional[int] = None
    stack: List[str] = []
    in_str = False
    esc = False
    for i, ch in enumerate(s):
        if start_idx is None:
            if ch in "{[":
                start_idx = i
                stack = [ch]
                in_str = False
                esc = False
            continue
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch in "{[":
            stack.append(ch)
        elif ch in "}]":
            if not stack:
                return None
            open_ch = stack.pop()
            if (open_ch, ch) not in (("{", "}"), ("[", "]")):
                return None
            if not stack:
                return s[start_idx : i + 1]
    return None

def _det_seed(graph_id: Optional[str], text: str) -> int:
    """Derive a stable seed from a Graph ID and text digest."""
    h = hashlib.sha256((graph_id or "").encode("utf-8") + text.encode("utf-8")).hexdigest()
    return int(h[:8], 16)

def _gen(
    model: str,
    prompt: str,
    schema_model: Type[BaseModel],
    seed: Optional[int] = None,
    num_predict_override: Optional[int] = None,
) -> Dict[str, Any]:
    """Core generator function enforcing strict JSON outputs."""
    schema = schema_model.model_json_schema()
    options: Dict[str, Any] = {
        "temperature": OLLAMA_TEMPERATURE,
        "num_predict": OLLAMA_NUM_PREDICT,
        "num_ctx": OLLAMA_NUM_CTX,
        "top_p": 1,
        "mirostat": 0,
    }
    if num_predict_override is not None:
        options["num_predict"] = num_predict_override
    if seed is not None:
        options["seed"] = seed
    # First attempt: pass JSON schema as the format
    try:
        resp = client.generate(
            model=model,
            prompt=prompt,
            format=schema,
            options=options,
            keep_alive=OLLAMA_KEEP_ALIVE,
        )
        raw = _strip_code_fences((resp.get("response") or "").strip())
        return json.loads(raw)
    except Exception:
        # Second attempt: plain JSON format
        resp = client.generate(
            model=model,
            prompt=prompt,
            format="json",
            options=options,
            keep_alive=OLLAMA_KEEP_ALIVE,
        )
        raw = _strip_code_fences((resp.get("response") or "").strip())
        try:
            return json.loads(raw)
        except Exception:
            extracted = _first_complete_json(raw)
            if extracted:
                return json.loads(extracted)
            raise

# ---------------------------------------------------------------------------
# Prompts and instructions
# ---------------------------------------------------------------------------
_ALLOWED_CATS_LIST: List[str] = ["General", "Invoice", "Customer Requests", "Misc"]
_ALLOWED_PRIOS_LIST: List[str] = ["High", "Medium", "Low"]

_CLASSIFY_INSTRUCTIONS: str = f"""
You are a STRICT JSON classifier for business emails.
… (classification instructions omitted for brevity; see file for full text) …
"""
_INVOICE_INSTRUCTIONS: str = """
Extract invoice details strictly from the text provided (email + attachments). …
… (invoice instructions omitted for brevity) …
"""
_REQUEST_INSTRUCTIONS: str = """
Summarise the customer's request in 2–3 sentences … (see file for full text) …
"""

# ---------------------------------------------------------------------------
# Public LLM tasks
# ---------------------------------------------------------------------------
def classify_text(text: str, graph_id: Optional[str] = None) -> Classification:
    """Classify free‑form text into a strict category and priority."""
    start = datetime.utcnow()
    log.info(
        "llm_classify_invoked",
        extra={
            "graph_id": graph_id or "-",
            "input_chars": len(text or ""),
            "hash": sha256_8(text or ""),
        },
    )
    # Trim text to the configured maximum for classification
    text_small = trim_text(text, MAX_CHARS_CLASSIFY)
    prompt = f"{text_small}\n\n{_CLASSIFY_INSTRUCTIONS}"
    seed = _det_seed(graph_id, text_small)
    data = _gen(CLASSIFIER_MODEL, prompt, Classification, seed=seed)
    category = title_case(data.get("category"))
    priority = title_case(data.get("priority"))
    if category not in _ALLOWED_CATS_LIST:
        low = (category or "").lower()
        if low.startswith("invoice"):
            category = "Invoice"
        elif low.startswith("customer request"):
            category = "Customer Requests"
        elif "misc" in low:
            category = "Misc"
        else:
            category = "General"
    if priority not in _ALLOWED_PRIOS_LIST:
        priority = "Low"
    elapsed_ms = int((datetime.utcnow() - start).total_seconds() * 1000)
    log.info(
        "llm_classify_complete",
        extra={
            "graph_id": graph_id or "-",
            "category": category,
            "priority": priority,
            "elapsed_ms": elapsed_ms,
        },
    )
    return Classification(category=category, priority=priority)

def extract_invoice(text: str, graph_id: Optional[str] = None) -> InvoiceFields:
    """Extract structured invoice data from text using the configured model."""
    start = datetime.utcnow()
    text_big = trim_text(text, MAX_CHARS_EXTRACT)
    prompt = f"{text_big}\n\n{_INVOICE_INSTRUCTIONS}"
    seed = _det_seed(graph_id, text_big)
    data = _gen(
        INVOICE_MODEL,
        prompt,
        InvoiceFields,
        seed=seed,
        num_predict_override=INVOICE_NUM_PREDICT,
    )
    inv = InvoiceFields(**data)
    present = [k for k, v in inv.model_dump().items() if v]
    if len(present) <= 2:
        fallback = _fallback_invoice_parse(text_big)
        merged = inv.model_dump()
        for k, v in fallback.items():
            if not merged.get(k) and v:
                merged[k] = v
        inv = InvoiceFields(**merged)
        log.info(
            "llm_invoice_fallback_applied",
            extra={
                "graph_id": graph_id or "-",
                "fields_detected": len([k for k, v in inv.model_dump().items() if v]),
            },
        )
    elapsed_ms = int((datetime.utcnow() - start).total_seconds() * 1000)
    log.info(
        "llm_extract_invoice_complete",
        extra={
            "graph_id": graph_id or "-",
            "elapsed_ms": elapsed_ms,
            "fields_detected": len([k for k, v in inv.model_dump().items() if v]),
        },
    )
    return inv

def extract_customer_request_summary(text: str, graph_id: Optional[str] = None) -> RequestFields:
    """Summarise a customer request and compute a ticket number."""
    start = datetime.utcnow()
    text_big = trim_text(text, MAX_CHARS_EXTRACT)
    prompt = f"{text_big}\n\n{_REQUEST_INSTRUCTIONS}"
    seed = _det_seed(graph_id, text_big)
    data = _gen(REQUEST_MODEL, prompt, RequestFields, seed=seed)
    summary = (data.get("summary") or "").strip()
    elapsed_ms = int((datetime.utcnow() - start).total_seconds() * 1000)
    log.info(
        "llm_extract_request_complete",
        extra={
            "graph_id": graph_id or "-",
            "elapsed_ms": elapsed_ms,
            "summary_chars": len(summary),
        },
    )
    return RequestFields(summary=summary)

# ---------------------------------------------------------------------------
# Regex fallback for invoices
# ---------------------------------------------------------------------------
… (regex helpers unchanged) …

# ---------------------------------------------------------------------------
# High‑level extraction wrapper
# ---------------------------------------------------------------------------
def run_local_extraction(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Perform classification and conditional extraction on a mail payload."""
    body_text_raw = (payload.get("body_text") or "").strip()
    if not body_text_raw:
        raise ValueError("body_text is required for extraction")
    subject = (payload.get("subject") or "").strip()
    graph_id = (payload.get("graph_id") or "").strip()
    received_at_iso = (payload.get("received_at") or "").strip()
    att_texts: List[str] = payload.get("attachments_text") or []

    # Quick classification using a trimmed view of the body only
    text_small = compose_email_text(subject, trim_text(body_text_raw, MAX_CHARS_CLASSIFY), [])
    t0 = datetime.utcnow()
    cls = classify_text(text_small, graph_id=graph_id)
    classify_ms = int((datetime.utcnow() - t0).total_seconds() * 1000)
    log.info(
        "llm_classification_done",
        extra={
            "graph_id": graph_id or "-",
            "body_sha": sha256_8(body_text_raw),
            "elapsed_ms": classify_ms,
            "category": cls.category,
            "priority": cls.priority,
            "attachments": len(att_texts),
        },
    )
    out: Dict[str, Any] = {"category": cls.category, "priority": cls.priority}
    cat = cls.category
    if cat.lower() in {"invoice", "invoices"}:
        text_big = compose_email_text(
            subject,
            trim_text(body_text_raw, MAX_CHARS_EXTRACT),
            [trim_text(a, MAX_CHARS_EXTRACT) for a in att_texts if a],
        )
        t1 = datetime.utcnow()
        inv = extract_invoice(text_big, graph_id=graph_id)
        invoice_ms = int((datetime.utcnow() - t1).total_seconds() * 1000)
        log.info(
            "llm_invoice_extraction_done",
            extra={
                "graph_id": graph_id or "-",
                "elapsed_ms": invoice_ms,
                "fields_present": len([k for k, v in inv.model_dump().items() if v is not None]),
            },
        )
        out["invoice"] = inv.model_dump()
    elif cat.lower() in {"customer requests", "customer request"}:
        text_big = compose_email_text(
            subject,
            trim_text(body_text_raw, MAX_CHARS_EXTRACT),
            [trim_text(a, MAX_CHARS_EXTRACT) for a in att_texts if a],
        )
        t2 = datetime.utcnow()
        summary = extract_customer_request_summary(text_big, graph_id=graph_id)
        request_ms = int((datetime.utcnow() - t2).total_seconds() * 1000)
        date_yyyymmdd = yyyymmdd_from_iso(received_at_iso)
        ticket = compute_ticket(date_yyyymmdd, seed=graph_id or "")
        log.info(
            "llm_request_extraction_done",
            extra={
                "graph_id": graph_id or "-",
                "elapsed_ms": request_ms,
                "ticket": ticket,
            },
        )
        out["request"] = {"summary": summary.summary, "ticket_number": ticket}
    else:
        log.info(
            "llm_no_extraction_needed",
            extra={"graph_id": graph_id or "-", "category": cat},
        )
    log.info(
        "llm_extraction_complete",
        extra={"graph_id": graph_id or "-", "category": out.get("category"), "priority": out.get("priority")},
    )
    return {"ok": True, "data": out}
