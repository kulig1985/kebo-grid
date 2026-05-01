"""
Order állapotgép.

Helyi állapotok és az executionReport eseményekből való átmenetek kezelése.
"""
from enum import Enum
from typing import Optional
from exchange.models import ExecutionReport


class LocalOrderState(str, Enum):
    PLANNED = "PLANNED"
    SUBMIT_QUEUED = "SUBMIT_QUEUED"
    SUBMITTED_UNKNOWN = "SUBMITTED_UNKNOWN"
    EXCHANGE_NEW = "EXCHANGE_NEW"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_QUEUED = "CANCEL_QUEUED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    EXTERNAL_CANCELED = "EXTERNAL_CANCELED"
    EXTERNAL_FILLED = "EXTERNAL_FILLED"
    UNKNOWN = "UNKNOWN"
    ORPHANED = "ORPHANED"
    ERROR = "ERROR"


# Terminal állapotok – ezekből nem lehet tovább haladni
TERMINAL_STATES = {
    LocalOrderState.FILLED,
    LocalOrderState.CANCELED,
    LocalOrderState.REJECTED,
    LocalOrderState.EXPIRED,
    LocalOrderState.EXTERNAL_CANCELED,
    LocalOrderState.EXTERNAL_FILLED,
    LocalOrderState.ERROR,
}

# Aktív állapotok – az order még nyitott az exchange-en
ACTIVE_STATES = {
    LocalOrderState.EXCHANGE_NEW,
    LocalOrderState.WORKING,
    LocalOrderState.PARTIALLY_FILLED,
    LocalOrderState.CANCEL_QUEUED,
}


def transition_from_execution_report(
    current_state: LocalOrderState,
    report: ExecutionReport,
    has_local_cancel_request: bool = False,
) -> tuple[LocalOrderState, bool]:
    """
    Állapot meghatározása egy executionReport alapján.

    Visszatér: (új_állapot, külső_beavatkozás_detektálva)
    """
    x = report.execution_type  # NEW | TRADE | CANCELED | REJECTED | EXPIRED
    X = report.order_status    # NEW | PARTIALLY_FILLED | FILLED | CANCELED | REJECTED | EXPIRED
    r = report.reject_reason

    # Terminal állapotból nem jövünk ki (idempotencia)
    if current_state in TERMINAL_STATES:
        return current_state, False

    external_intervention = False

    match x:
        case "NEW":
            new_state = LocalOrderState.WORKING
            if X == "NEW":
                new_state = LocalOrderState.WORKING

        case "TRADE":
            if X == "FILLED":
                new_state = LocalOrderState.FILLED
            else:
                new_state = LocalOrderState.PARTIALLY_FILLED

        case "CANCELED":
            if has_local_cancel_request:
                new_state = LocalOrderState.CANCELED
            else:
                new_state = LocalOrderState.EXTERNAL_CANCELED
                external_intervention = True

        case "REJECTED":
            new_state = LocalOrderState.REJECTED

        case "EXPIRED":
            new_state = LocalOrderState.EXPIRED

        case "REPLACED":
            # LIMIT_MAKER amennyiből visszautasításra kerül, EXPIRED-ként érkezik
            new_state = LocalOrderState.EXPIRED

        case _:
            new_state = LocalOrderState.UNKNOWN

    return new_state, external_intervention


def is_terminal(state: LocalOrderState) -> bool:
    return state in TERMINAL_STATES


def is_active(state: LocalOrderState) -> bool:
    return state in ACTIVE_STATES
