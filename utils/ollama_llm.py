"""
Local LLM classification and extraction utilities
=================================================

This module encapsulates the core logic formerly housed in the
``mail‑ollama‑extractor`` service.  It exposes a set of Pydantic
models and helper functions to perform in‑process classification and
structured extraction of business emails using an Ollama large
language model (LLM).

The design goals for this module are:

* **Self‑contained** – no HTTP calls or external services are
  required.  All LLM requests are issued via the ``ollama`` Python
  client running on the same host.
* **Transparent logging** – every step emits informative log lines
  so that operators can trace classification and extraction times,
  seeds and any fallback logic.  Logs are structured to aid
  ingestion into Azure’s Log Stream.
* **Compatibility** – the functions mirror the behaviour of the
  separate extractor API: a fast classification stage followed by
  conditional invoice or customer request parsing.  Should a field
  be missing from the model output, sensible defaults and regex
  fallbacks are applied.

Classes
-------

``ExtractIn``
    Request schema for classification/extraction.  Carries the
    plaintext body of the message, optional attachments (as plain
    text), and metadata such as ``graph_id`` and ``received_at`` for
    deterministic ticket generation.

``InvoiceFields``
    Structured representation of invoice data extracted from a mail.

``RequestFields``
    Representation of a customer request summary and ticket number.

``ExtractOut``
    Wrapper for extraction results, containing a top‑level ``data``
    field holding the category, priority and any invoice or request
    sub‑structures.

Functions
---------

``classify_text(text: str, graph_id: Optional[str] = None) -> Classification``
    Classify an email’s content into one of the allowed categories
    (General, Invoice, Customer Requests, Misc) and assign a priority
    (High, Medium, Low).  Uses a deterministic seed based on the
    ``graph_id`` and the input text to ensure reproducible results.

``extract_invoice(text: str, graph_id: Optional[str] = None) -> InvoiceFields``
    Perform structured invoice extraction against the provided text.
    If too few fields are returned, a regex fallback is used to
    opportunistically fill in common invoice details.

``extract_customer_request_summary(text: str, graph_id: Optional[str] = None) -> RequestFields``
    Summarise a customer request into a concise summary and compute
    a deterministic ticket number.

``run_local_extraction(payload: Dict[str, Any]) -> Dict[str, Any]``
    High level wrapper that accepts a dictionary matching the payload
    shape expected by the original extractor service.  It performs
    classification followed by conditional invoice or request
    extraction and returns a dictionary with the same ``data``
    structure as the remote service.

Environment
-----------

Several environment variables may be set to tune the behaviour of the
models:

``OLLAMA_HOST``
    URL of the local Ollama server (defaults to ``http://localhost:11434``).

``CLASSIFIER_MODEL``, ``INVOICE_MODEL``, ``REQUEST_MODEL``
    Names of the Ollama models to use for classification, invoice
    extraction and request summarisation respectively.

``OLLAMA_TEMPERATURE``, ``OLLAMA_NUM_PREDICT``, ``OLLAMA_NUM_CTX``
    Generation parameters controlling sampling, number of predicted
    tokens and context size.  See the Ollama documentation for
    details.

``INVOICE_NUM_PREDICT``
    Overrides the ``num_predict`` setting specifically for invoice
    extraction.

``TICKET_PREFIX``
    Prefix for generated customer request ticket numbers.  Defaults to
    ``REQ-`` if unset.
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

# Load environment variables from a .env file if present.  This is
# intentionally called at import time to allow model configuration to
# happen early.  If a .env file is not present, this is a no‑op.
load_dotenv()

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
# This module uses a dedicated logger so that its output can be easily
# differentiated from other components in the unified API.  The main
# application (main.py) calls ``init_logging`` which will configure
# root logging handlers and propagate to this logger.
log = logging.getLogger("utils.ollama_llm")

# ---------------------------------------------------------------------------
# Model and Ollama client configuration
# ---------------------------------------------------------------------------
# Allow overriding the Ollama server via an environment variable.  The
# trailing slash is stripped so that URL concatenation works as
# expected.
OLLAMA_HOST: str = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")

# Names of the models used for each task.  These default to small
# variants for local development but can be swapped for larger models
# (e.g. ``mail-classifier-large``) without code changes.
CLASSIFIER_MODEL: str = os.getenv("CLASSIFIER_MODEL", "mail-classifier-small")
INVOICE_MODEL: str    = os.getenv("INVOICE_MODEL",   "invoice-extractor-small")
REQUEST_MODEL: str    = os.getenv("REQUEST_MODEL",   "request-summarizer-small")

# Generation parameters.  Temperature of zero disables sampling for
# deterministic responses.  ``num_predict`` controls the maximum
# number of tokens to generate (applies to classification and
# customer request).  ``num_ctx`` sets the context window; if your
# emails and attachments are very large, consider increasing this.
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

# Invoice specific override; set this to 0 to allow unlimited token
# generation for invoices.
INVOICE_NUM_PREDICT: int = _get_int("INVOICE_NUM_PREDICT", 400)

# Instantiate the Ollama client.  This object manages connections
# behind the scenes.  Should the host be unavailable, client
# operations will raise exceptions which are caught by callers.
client = ollama.Client(host=OLLAMA_HOST)

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
# Define literal types for strict validation of categories and priorities.
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
        description="List of plain-text contents from attachments",
    )
    graph_id: Optional[str] = Field(
        default=None,
        description="Microsoft Graph message id (used for deterministic seeding)",
    )
    subject: Optional[str] = Field(default=None, description="Email subject")
    received_at: Optional[str] = Field(
        default=None,
        description="ISO-8601 timestamp used for ticket date generation",
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
    """Construct a prompt by concatenating subject, body and attachments.

    The remote service built prompts with distinct sections.  This helper
    replicates that behaviour for parity with the original extractor.
    """
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
    """Convert an ISO 8601 timestamp to YYYYMMDD, falling back to UTC today."""
    try:
        dt = datetime.fromisoformat((iso or "").replace("Z", "+00:00"))
    except Exception:
        dt = datetime.utcnow()
    return dt.strftime("%Y%m%d")

def compute_ticket(date_yyyymmdd: str, seed: str, prefix: Optional[str] = None) -> str:
    """Generate a deterministic ticket number from a date and seed.

    The ticket format is ``<PREFIX><YYYYMMDD>-<LAST6>`` where ``<PREFIX>``
    defaults to ``REQ-`` but can be overridden via the ``TICKET_PREFIX``
    environment variable.  The seed is any string; all non‑alphanumeric
    characters are stripped and the last six characters are kept (or the
    entire string if shorter).
    """
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
        lines = lines[1:]  # drop the opening fence line
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
    """Core generator function enforcing strict JSON outputs.

    This helper attempts two generation strategies.  First, it asks
    Ollama to emit JSON matching the provided Pydantic schema.  If the
    returned text cannot be parsed, it retries using plain ``format="json"``
    which instructs the model to output a JSON object.  Should both
    attempts yield invalid JSON, an exception will propagate back to
    the caller.
    """
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

Allowed categories (exactly one, case‑sensitive):
- General
- Invoice
- Customer Requests
- Misc

Allowed priorities:
- High
- Medium
- Low

Rules:
- Read the ENTIRE email body AND any attachment text included in the prompt.
- Use the exact casing shown above; do not use plurals.
- Decide by context and content; do not rely on isolated keywords like “invoice” or “bill”.
- Never invent information not present in the text.

Category guidelines:
- “Invoice”: choose this category only if the email clearly contains **at least three** invoice‑specific cues.  Examples of cues include: (a) an attached invoice or bill document; (b) an explicit invoice number or reference; (c) an invoice date or due date; (d) a total amount or amount due; (e) payment instructions or bank/account details.  If fewer than three of these cues are present—even if the words “invoice” or “payment” appear—do **not** choose “Invoice”; instead classify as “General” or “Misc” as appropriate.
- “Customer Requests”: the sender is asking for help, information or action that is not a bill; e.g., support tickets, queries, complaints or account changes.
- “Misc”: automated notifications, marketing emails or system alerts that are unrelated to customer service or billing.
- Otherwise → “General”.

Priority guidelines:
- Use “High” **ONLY** if the email explicitly expresses urgency (e.g., “urgent”, “asap”, “immediately”, “critical”) or there is a due date that is imminent.
- Use “Medium” when there is a due date/time but no explicit urgency words.
- Otherwise → “Low”.

Return only this JSON object:
{{"category": <one of {_ALLOWED_CATS_LIST}>, "priority": <one of {_ALLOWED_PRIOS_LIST}>}}
"""

_INVOICE_INSTRUCTIONS: str = """
Extract invoice details strictly from the text provided (email + attachments). Do not guess.
If a field isn't present, set it to null.
Keep numbers/strings exactly as they appear (no reformatting).

Also produce a concise "description" of what the invoice is for:
- 1–2 sentences max.
- Base ONLY on items/particulars/services visible in the text.
- Name the key item(s) or service(s) if listed.
- Do NOT include totals or payment details.
- If unclear, set description to null.

Return JSON with these keys ONLY:
- invoice_number
- invoice_date
- due_date
- invoice_amount
- payment_link
- bsb
- account_number
- account_name
- biller_code
- payment_reference
- description
"""

_REQUEST_INSTRUCTIONS: str = """
Summarise the customer's request in 2–3 sentences (plain English, no assumptions).
Return JSON with only:
{ "summary": string }
"""

# ---------------------------------------------------------------------------
# Public LLM tasks
# ---------------------------------------------------------------------------
def classify_text(text: str, graph_id: Optional[str] = None) -> Classification:
    """Classify free‑form text into a strict category and priority.

    This function caps the input length for latency control, composes
    the classification prompt and invokes the Ollama model.  It then
    performs hard validation of the returned values to coerce common
    variants (e.g. plurals or casing differences) into one of the
    allowed categories and priorities.
    """
    # Trim text to the configured maximum for classification
    start = datetime.utcnow()
    # Emit a log before classification begins.  We do not include the
    # full text in logs to avoid leaking sensitive information.  Instead
    # we log a hash and the length of the trimmed prompt.
    log.info(
        "llm_classify_invoked",
        extra={
            "graph_id": graph_id or "-",
            "input_chars": len(text or ""),
            "hash": sha256_8(text or ""),
        },
    )
    text_small = trim_text(text, MAX_CHARS_CLASSIFY)
    prompt = f"{text_small}\n\n{_CLASSIFY_INSTRUCTIONS}"
    # Derive deterministic seed for reproducibility
    seed = _det_seed(graph_id, text_small)
    data = _gen(CLASSIFIER_MODEL, prompt, Classification, seed=seed)
    # Coerce to title case
    category = title_case(data.get("category"))
    priority = title_case(data.get("priority"))
    # Normalise category
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
    # Normalise priority
    if priority not in _ALLOWED_PRIOS_LIST:
        priority = "Low"
    elapsed_ms = int((datetime.utcnow() - start).total_seconds() * 1000)
    # Final log for classification completion
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
    """Extract structured invoice data from text using the configured model.

    The extraction is capped to ``MAX_CHARS_EXTRACT`` characters.  If the
    resulting model contains two or fewer non‑empty fields, a regex
    fallback is applied to opportunistically harvest additional
    details.  The merged result is returned as an ``InvoiceFields``
    instance.
    """
    # Capture start time for performance logging
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
    # Count how many fields are populated
    present = [k for k, v in inv.model_dump().items() if v]
    if len(present) <= 2:
        fallback = _fallback_invoice_parse(text_big)
        merged = inv.model_dump()
        for k, v in fallback.items():
            if not merged.get(k) and v:
                merged[k] = v
        inv = InvoiceFields(**merged)
        # Log that a regex fallback was applied
        log.info(
            "llm_invoice_fallback_applied",
            extra={
                "graph_id": graph_id or "-",
                "fields_detected": len([k for k, v in inv.model_dump().items() if v]),
            },
        )
    # Log completion and elapsed time for invoice extraction
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
    """Summarise a customer request and compute a ticket number.

    The summary extraction is capped to ``MAX_CHARS_EXTRACT``.
    """
    # Capture start time for performance logging
    start = datetime.utcnow()
    text_big = trim_text(text, MAX_CHARS_EXTRACT)
    prompt = f"{text_big}\n\n{_REQUEST_INSTRUCTIONS}"
    seed = _det_seed(graph_id, text_big)
    data = _gen(REQUEST_MODEL, prompt, RequestFields, seed=seed)
    # Ensure the summary is clean
    summary = (data.get("summary") or "").strip()
    # Log completion and elapsed time for request summary extraction
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
_NUM_PATTERN = r"[0-9][0-9,]*"
_AMT_PATTERN = rf"(?:{_NUM_PATTERN}(?:\.\d{{2}})?)"

def _search(pattern: str, text: str, flags=re.I) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None

def _fallback_invoice_parse(text: str) -> Dict[str, Optional[str]]:
    """Attempt to extract invoice fields using regular expressions.

    This parser is intentionally liberal and may produce false positives.
    It is only used when the LLM extraction yields too few fields.
    """
    fields: Dict[str, Optional[str]] = {
        "invoice_number": _search(r"\bInvoice(?:\s*(?:No\.?|#|Number))?[:\-\s]*([A-Za-z0-9\-/]+)", text),
        "invoice_date":   _search(r"\bInvoice\s*Date[:\-\s]*([0-9]{1,2}[\/\-.][0-9]{1,2}[\/\-.][0-9]{2,4})", text)
                          or _search(r"\bDate[:\-\s]*([0-9]{1,2}[\/\-.][0-9]{1,2}[\/\-.][0-9]{2,4})", text),
        "due_date":       _search(r"\b(?:Due\s*Date|Payment\s*Due)[:\-\s]*([0-9]{1,2}[\/\-.][0-9]{1,2}[\/\-.][0-9]{2,4})", text),
        "invoice_amount": _search(r"\b(?:Total\s*Due|Total\s*Amount|Amount\s*Due)[:\-\s]*(\$?\s*" + _AMT_PATTERN + ")", text),
        "payment_link":   _search(r"(https?://\S+)", text),
        "bsb":            _search(r"\bBSB[:\-\s]*([0-9]{3}[- ]?[0-9]{3})", text),
        "account_number": _search(r"\bAccount\s*Number[:\-\s]*([0-9]{5,})", text),
        "account_name":   _search(r"\bAccount\s*Name[:\-\s]*([^\n]+)", text),
        "biller_code":    _search(r"\bBiller\s*Code[:\-\s]*([0-9]+)", text),
        "payment_reference": _search(r"\bPayment\s*Reference[:\-\s]*([A-Za-z0-9\-]+)", text),
        "description":    None,  # description must come from the LLM
    }
    return fields

# ---------------------------------------------------------------------------
# High‑level extraction wrapper
# ---------------------------------------------------------------------------
def run_local_extraction(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Perform classification and conditional extraction on a mail payload.

    ``payload`` should at minimum contain a ``body_text`` field.  Optional
    fields include ``subject``, ``received_at``, ``graph_id`` and
    ``attachments_text``.  The resulting dictionary contains
    ``ok`` (bool) and ``data`` (dict) keys mirroring the remote
    extractor’s response format.  The ``data`` key always contains
    ``category`` and ``priority``, and may contain an ``invoice`` or
    ``request`` sub-dictionary if applicable.
    """
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
    # Emit classification log
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
    # Conditional extraction for invoices
    cat = cls.category
    # Lowercase comparisons to handle variants
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
        # Deterministic ticket number
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
        # General / Misc → nothing else to extract
        log.info(
            "llm_no_extraction_needed",
            extra={"graph_id": graph_id or "-", "category": cat},
        )
    # Final log and return
    log.info(
        "llm_extraction_complete",
        extra={"graph_id": graph_id or "-", "category": out.get("category"), "priority": out.get("priority")},
    )
    return {"ok": True, "data": out}
