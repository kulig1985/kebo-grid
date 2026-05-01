"""Strukturált naplózás beállítása (structlog)."""
import json
import logging
import structlog


def _json_serializer(obj, **kwargs) -> str:
    """JSON szerializáló, ami olvasható karaktereket ír (nem \\uXXXX escape)."""
    return json.dumps(obj, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO", json_output: bool = True) -> None:
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )

    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.ExceptionRenderer(),
    ]

    if json_output:
        # ensure_ascii=False: ékezetes karakterek olvashatóak maradnak a logban
        processors.append(structlog.processors.JSONRenderer(serializer=_json_serializer))
    else:
        processors.append(structlog.dev.ConsoleRenderer())

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
