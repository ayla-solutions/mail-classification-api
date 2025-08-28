"""
Shared JSON logging for the whole service.
- Adds service name, request_id, graph_id to every record
- Emits newline-delimited JSON to stdout (great for Azure logs)
"""
import os
import logging
import contextvars
from pythonjsonlogger import jsonlogger

# Context that any module can set/get
request_id_var = contextvars.ContextVar("request_id", default=None)
graph_id_var   = contextvars.ContextVar("graph_id", default=None)

class _CtxFilter(logging.Filter):
    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def filter(self, record: logging.LogRecord) -> bool:
        record.service    = self.service
        record.request_id = request_id_var.get()
        record.graph_id   = graph_id_var.get()
        return True

def init_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    service = os.getenv("SERVICE_NAME", "mail-classifier")

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    handler = logging.StreamHandler()
    fmt = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(service)s %(name)s %(message)s %(request_id)s %(graph_id)s"
    )
    handler.setFormatter(fmt)
    handler.addFilter(_CtxFilter(service))
    root.addHandler(handler)

def set_graph_id(gid: str | None):
    graph_id_var.set(gid)

def set_request_id(rid: str | None):
    request_id_var.set(rid)
