"""Strukturált naplózás beállítása (structlog).

Dupla kimenet:
- Console (stdout): rich/json/console formátum → docker-compose logs
- File (/app/logs/bot.log): mindig JSON → tartós napló
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog


def _json_serializer(obj, **kwargs) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _millis_timestamper(logger, method, event_dict):
    now = datetime.now(timezone.utc)
    event_dict["timestamp"] = now.strftime("%H:%M:%S.") + f"{now.microsecond // 1000:03d}"
    return event_dict


# ANSI escape kódok
_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_GREEN = "\033[32m"
_RED = "\033[31m"
_CYAN = "\033[36m"
_YELLOW = "\033[33m"
_MAGENTA = "\033[35m"
_RED_BOLD = "\033[1;31m"
_YELLOW_BOLD = "\033[1;33m"
_MAGENTA_BOLD = "\033[1;35m"


def _grid_event_colorizer(logger, method, event_dict):
    event = event_dict.get("event", "")

    if event == "FILL":
        side = event_dict.get("side", "")
        color = _GREEN if side == "BUY" else _RED
        event_dict["event"] = f"{color}■ {side} FILL{_RESET}"
    elif event == "COUNTER":
        event_dict["event"] = f"{_CYAN}→ COUNTER{_RESET}"
    elif event == "CYCLE":
        event_dict["event"] = f"{_YELLOW_BOLD}✓ CYCLE{_RESET}"
    elif event == "PROFIT":
        event_dict["event"] = f"{_MAGENTA_BOLD}$ PROFIT{_RESET}"
    elif event == "Order bekülve":
        event_dict["event"] = f"{_DIM}Order bekülve{_RESET}"
    elif method in ("error", "critical"):
        event_dict["event"] = f"{_RED_BOLD}{event}{_RESET}"

    return event_dict


def _setup_file_handler(level: str) -> None:
    """JSON log fájl beállítása RotatingFileHandler-rel."""
    log_dir = os.environ.get("LOG_DIR", "/app/logs")
    path = Path(log_dir)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError:
        return

    handler = RotatingFileHandler(
        path / "bot.log",
        maxBytes=50 * 1024 * 1024,  # 50 MB
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler.setFormatter(logging.Formatter("%(message)s"))

    file_logger = logging.getLogger("structlog_file")
    file_logger.handlers.clear()
    file_logger.addHandler(handler)
    file_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    file_logger.propagate = False


def _file_writer_processor(logger, method, event_dict):
    """Minden log sort JSON-ként ír a file logger-be (mellékhatás processor)."""
    try:
        file_logger = logging.getLogger("structlog_file")
        if file_logger.handlers:
            clean = {k: v for k, v in event_dict.items()}
            for k in ("_record", "_from_structlog"):
                clean.pop(k, None)
            # ANSI kódok eltávolítása a fájlból
            ev = str(clean.get("event", ""))
            if "\033[" in ev:
                import re
                clean["event"] = re.sub(r"\033\[[0-9;]*m", "", ev).strip()
            now = datetime.now(timezone.utc)
            clean["ts"] = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
            line = json.dumps(clean, ensure_ascii=False, default=str)
            file_logger.info(line)
    except Exception:
        pass
    return event_dict


def setup_logging(level: str = "INFO", fmt: str = "rich") -> None:
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )

    _setup_file_handler(level)

    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
    ]

    if fmt == "json":
        processors.extend([
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.ExceptionRenderer(),
            _file_writer_processor,
            structlog.processors.JSONRenderer(serializer=_json_serializer),
        ])
    elif fmt == "rich":
        processors.extend([
            _millis_timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.ExceptionRenderer(),
            _grid_event_colorizer,
            _file_writer_processor,
            structlog.dev.ConsoleRenderer(colors=True, pad_event=50),
        ])
    else:
        processors.extend([
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.ExceptionRenderer(),
            _file_writer_processor,
            structlog.dev.ConsoleRenderer(),
        ])

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)
