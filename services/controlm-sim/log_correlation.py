"""Logging helpers for Datadog trace/log correlation."""

import json
import logging
import os
from datetime import datetime, timezone

from ddtrace import tracer


def datadog_correlation_fields():
    fields = tracer.get_log_correlation_context()
    return {key: str(value) for key, value in fields.items() if value is not None}


class DatadogJsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
            "env": os.environ.get("DD_ENV", "demo"),
            "service": os.environ.get("DD_SERVICE", "controlm-sim"),
            "version": os.environ.get("DD_VERSION", "2.0.0"),
        }
        payload.update(datadog_correlation_fields())
        if record.exc_info:
            payload["error.stack"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def install_file_logging(path="/data/logs/controlm-sim.log"):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        root = logging.getLogger()
        if any(getattr(h, "baseFilename", None) == path for h in root.handlers):
            return
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(DatadogJsonFormatter())
        root.addHandler(handler)
    except Exception:
        pass
