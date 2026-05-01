"""
Idempotencia teszt: duplikált executionReport nem dupláz fill-t.
"""
import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, call
from exchange.models import ExecutionReport
from grid.state_machine import LocalOrderState, transition_from_execution_report


def make_fill_report(trade_id: int = 999) -> dict:
    return {
        "e": "executionReport",
        "E": 1700000001000,
        "s": "SOLUSDT",
        "c": "G-abc123-B-001-0001-01",
        "S": "BUY",
        "o": "LIMIT_MAKER",
        "f": "GTC",
        "q": "0.060",
        "p": "80.00",
        "x": "TRADE",
        "X": "FILLED",
        "r": "NONE",
        "i": 123456789,
        "l": "0.060",
        "z": "0.060",
        "L": "80.00",
        "n": "0.000060",
        "N": "SOL",
        "T": 1700000001000,
        "t": trade_id,
        "I": 111222333,
        "w": False,
        "m": True,
        "O": 1700000000000,
        "Z": "4.800000",
        "Y": "4.800000",
    }


def test_dupla_fill_event_parse():
    """Ugyanaz az executionReport kétszer parse-olva ugyanazt adja."""
    raw = make_fill_report(trade_id=999)
    r1 = ExecutionReport.from_dict(raw)
    r2 = ExecutionReport.from_dict(raw)

    assert r1.trade_id == r2.trade_id == 999
    assert r1.execution_id == r2.execution_id == 111222333
    assert r1.cumulative_filled_qty == r2.cumulative_filled_qty


def test_state_machine_terminal_idempotens():
    """
    Ha az order már FILLED állapotban van,
    egy újabb event nem változtatja az állapotot.
    """
    raw = make_fill_report()
    report = ExecutionReport.from_dict(raw)

    # Első feldolgozás
    state1, _ = transition_from_execution_report(LocalOrderState.WORKING, report)
    assert state1 == LocalOrderState.FILLED

    # Dupla feldolgozás – állapot nem változik
    state2, _ = transition_from_execution_report(state1, report)
    assert state2 == LocalOrderState.FILLED  # terminal, nem változik


def test_különböző_trade_id_különböző_fill():
    """Különböző trade_id-vel érkező event-ek különböző fill-nek számítanak."""
    raw1 = make_fill_report(trade_id=100)
    raw2 = make_fill_report(trade_id=101)

    r1 = ExecutionReport.from_dict(raw1)
    r2 = ExecutionReport.from_dict(raw2)

    assert r1.trade_id != r2.trade_id
