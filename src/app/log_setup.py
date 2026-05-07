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


_LINE = "─" * 60


def _grid_event_colorizer(logger, method, event_dict):
    event = event_dict.get("event", "")

    if event == "FILL":
        side = event_dict.pop("side", "")
        lvl = event_dict.pop("lvl", "")
        price = event_dict.pop("price", "")
        qty = event_dict.pop("qty", "")
        quote = event_dict.pop("quote", "")
        fee = event_dict.pop("fee", "")
        filled_at = event_dict.pop("filled_at", "")
        pair = event_dict.pop("pair", "")
        color = _GREEN if side == "BUY" else _RED
        event_dict["event"] = (
            f"\n{_DIM}{_LINE}{_RESET}\n"
            f"{color}{_BOLD}■ {side} FILL{_RESET}       "
            f"{_DIM}{pair}{_RESET}  lvl {_BOLD}{lvl}{_RESET}\n"
            f"  price {_BOLD}{price}{_RESET}   qty {qty}   quote {quote}\n"
            f"  fee {fee}   filled {_DIM}{filled_at}{_RESET}"
        )

    elif event == "COUNTER":
        side = event_dict.pop("side", "")
        lvl = event_dict.pop("lvl", "")
        price = event_dict.pop("price", "")
        qty = event_dict.pop("qty", "")
        value = event_dict.pop("value", "")
        sent_at = event_dict.pop("sent_at", "")
        latency_ms = event_dict.pop("latency_ms", "")
        event_dict.pop("pair", None)
        event_dict["event"] = (
            f"  {_CYAN}→ {side} COUNTER{_RESET}   lvl {_BOLD}{lvl}{_RESET}\n"
            f"    price {_BOLD}{price}{_RESET}   qty {qty}   value {value}\n"
            f"    sent {_DIM}{sent_at}{_RESET}   latency {_DIM}{latency_ms}ms{_RESET}\n"
            f"{_DIM}{_LINE}{_RESET}"
        )

    elif event == "PROFIT":
        realized = event_dict.pop("realized", "")
        matches = event_dict.pop("matches", "")
        per_hour = event_dict.pop("per_hour", "")
        portfolio = event_dict.pop("portfolio", "")
        total_pnl = event_dict.pop("total_pnl", "")
        open_base = event_dict.pop("open_base", "")
        open_quote = event_dict.pop("open_quote", "")
        hours = event_dict.pop("hours", "")
        pnl_color = _RED if str(total_pnl).startswith("-") else _GREEN
        event_dict["event"] = (
            f"\n{_DIM}{_LINE}{_RESET}\n"
            f"{_MAGENTA_BOLD}$ PROFIT{_RESET}  {_DIM}{hours}h{_RESET}\n"
            f"  realized {_BOLD}{realized}{_RESET}   ({matches} matches, {per_hour}/h)\n"
            f"  portfolio {_BOLD}{portfolio}{_RESET} USDT   total_pnl {pnl_color}{_BOLD}{total_pnl}{_RESET} USDT\n"
            f"  open base {open_base}   open quote {open_quote}\n"
            f"{_DIM}{_LINE}{_RESET}"
        )

    elif event == "GRID_TABLE":
        symbol = event_dict.pop("symbol", "")
        gtype = event_dict.pop("type", "")
        anchor = event_dict.pop("anchor", "")
        step_pct = event_dict.pop("step_pct", "")
        break_even_pct = event_dict.pop("break_even_pct", "")
        grid_low = event_dict.pop("grid_low", "")
        grid_high = event_dict.pop("grid_high", "")
        grid_range_pct = event_dict.pop("grid_range_pct", "")
        k_buy = event_dict.pop("k_buy", 0)
        k_sell = event_dict.pop("k_sell", 0)
        order_quote_value = event_dict.pop("order_quote_value", "")
        profit_per_cycle = event_dict.pop("profit_per_cycle", "")
        quote_needed = event_dict.pop("quote_needed", "")
        base_needed = event_dict.pop("base_needed", "")
        rows = event_dict.pop("rows", [])
        base_asset = event_dict.pop("base_asset", "")
        quote_asset = event_dict.pop("quote_asset", "")

        lines = [
            "",
            f"{_DIM}{_LINE}{_RESET}",
            f"{_YELLOW_BOLD}🎯 GRID GENERÁLVA{_RESET}  {_BOLD}{symbol}{_RESET}  {_DIM}{gtype}{_RESET}",
            f"  anchor {_BOLD}{anchor}{_RESET}   step {_BOLD}{step_pct}{_RESET}   "
            f"break-even {_DIM}{break_even_pct}{_RESET}",
            f"  range {grid_low} → {grid_high}  ({grid_range_pct})   "
            f"szintek: {k_buy} BUY / {k_sell} SELL",
            f"  per-line {order_quote_value}   profit/cycle {_GREEN}{profit_per_cycle}{_RESET}",
            f"  igényel: {quote_needed}  +  {base_needed}",
            f"{_DIM}  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─{_RESET}",
        ]
        for idx, side, price, qty, notional in rows:
            if side == "ANCHOR":
                lines.append(
                    f"{_DIM}  ━━━━━ anchor {_BOLD}{price}{_RESET}{_DIM} ━━━━━{_RESET}"
                )
            else:
                color = _RED if side == "SELL" else _GREEN
                sign = "+" if idx > 0 else ""
                lines.append(
                    f"  {color}{side:4s}{_RESET} {sign}{idx:>3}  "
                    f"price {_BOLD}{price}{_RESET}  "
                    f"qty {qty.normalize() if hasattr(qty, 'normalize') else qty}  "
                    f"notional {notional:.2f} {quote_asset}"
                )
        lines.append(f"{_DIM}{_LINE}{_RESET}")
        event_dict["event"] = "\n".join(lines)

    elif event == "INIT_ORDER":
        side = event_dict.pop("side", "")
        lvl = event_dict.pop("lvl", "")
        price = event_dict.pop("price", "")
        qty = event_dict.pop("qty", "")
        notional = event_dict.pop("notional", "")
        quote_asset = event_dict.pop("quote_asset", "")
        event_dict.pop("cid", None)
        color = _GREEN if side == "BUY" else _RED
        sign = "+" if isinstance(lvl, int) and lvl > 0 else ""
        event_dict["event"] = (
            f"  {color}→ {side:4s}{_RESET} {sign}{lvl:>3}  "
            f"price {_BOLD}{price}{_RESET}  qty {qty}  "
            f"notional {notional} {quote_asset}"
        )

    elif event == "MISSED":
        side = event_dict.pop("side", "")
        lvl = event_dict.pop("lvl", "")
        price = event_dict.pop("price", "")
        reason = event_dict.pop("reason", "")
        event_dict["event"] = (
            f"{_YELLOW_BOLD}⚠ MISSED {side}{_RESET}  "
            f"lvl {_BOLD}{lvl}{_RESET}  price {_BOLD}{price}{_RESET}  "
            f"{_DIM}({reason}){_RESET}"
        )

    elif event == "MISSED_RETRY":
        side = event_dict.pop("side", "")
        lvl = event_dict.pop("lvl", "")
        price = event_dict.pop("price", "")
        retry = event_dict.pop("retry", "")
        mid = event_dict.pop("mid", "")
        event_dict["event"] = (
            f"{_CYAN}↻ MISSED RETRY {side}{_RESET}  "
            f"lvl {_BOLD}{lvl}{_RESET}  price {_BOLD}{price}{_RESET}  "
            f"retry #{retry}  {_DIM}mid={mid}{_RESET}"
        )

    elif event == "CYCLE":
        event_dict["event"] = f"{_YELLOW_BOLD}✓ CYCLE{_RESET}"
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
            structlog.dev.ConsoleRenderer(colors=True, pad_event=0),
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
