"""Order állapotgép tesztek."""
import pytest
from decimal import Decimal
from exchange.models import ExecutionReport
from grid.state_machine import (
    LocalOrderState, transition_from_execution_report,
    is_terminal, is_active,
)


def make_report(**kwargs) -> ExecutionReport:
    defaults = dict(
        event_time=1700000000000,
        symbol="SOLUSDT",
        client_order_id="G-abc123-B-001-0001-01",
        side="BUY",
        order_type="LIMIT_MAKER",
        time_in_force="GTC",
        original_qty=Decimal("0.06"),
        price=Decimal("80.00"),
        execution_type="NEW",
        order_status="NEW",
        reject_reason="NONE",
        order_id=123456789,
        last_executed_qty=Decimal("0"),
        cumulative_filled_qty=Decimal("0"),
        last_executed_price=Decimal("0"),
        commission_amount=Decimal("0"),
        commission_asset=None,
        transaction_time=1700000000000,
        trade_id=-1,
        execution_id=None,
        is_on_book=True,
        is_maker=False,
        order_creation_time=1700000000000,
        cumulative_quote_qty=Decimal("0"),
        last_quote_qty=Decimal("0"),
    )
    defaults.update(kwargs)
    return ExecutionReport(**defaults)


def test_new_event_working_állapotra_vált():
    report = make_report(execution_type="NEW", order_status="NEW", is_on_book=True)
    state, external = transition_from_execution_report(LocalOrderState.SUBMITTED_UNKNOWN, report)
    assert state == LocalOrderState.WORKING
    assert not external


def test_trade_filled_végállapot():
    report = make_report(
        execution_type="TRADE",
        order_status="FILLED",
        cumulative_filled_qty=Decimal("0.06"),
        last_executed_qty=Decimal("0.06"),
    )
    state, external = transition_from_execution_report(LocalOrderState.WORKING, report)
    assert state == LocalOrderState.FILLED
    assert is_terminal(state)


def test_trade_partially_filled():
    report = make_report(
        execution_type="TRADE",
        order_status="PARTIALLY_FILLED",
        cumulative_filled_qty=Decimal("0.03"),
        last_executed_qty=Decimal("0.03"),
    )
    state, _ = transition_from_execution_report(LocalOrderState.WORKING, report)
    assert state == LocalOrderState.PARTIALLY_FILLED
    assert not is_terminal(state)


def test_canceled_helyi_cancel_kérelemmel():
    report = make_report(execution_type="CANCELED", order_status="CANCELED")
    state, external = transition_from_execution_report(
        LocalOrderState.CANCEL_QUEUED, report, has_local_cancel_request=True
    )
    assert state == LocalOrderState.CANCELED
    assert not external


def test_canceled_külső_beavatkozás():
    """Ha nincs helyi cancel kérelem, EXTERNAL_CANCELED kell."""
    report = make_report(execution_type="CANCELED", order_status="CANCELED")
    state, external = transition_from_execution_report(
        LocalOrderState.WORKING, report, has_local_cancel_request=False
    )
    assert state == LocalOrderState.EXTERNAL_CANCELED
    assert external  # külső beavatkozás detektálva


def test_rejected_állapot():
    report = make_report(execution_type="REJECTED", order_status="REJECTED", reject_reason="PRICE_FILTER")
    state, _ = transition_from_execution_report(LocalOrderState.SUBMITTED_UNKNOWN, report)
    assert state == LocalOrderState.REJECTED
    assert is_terminal(state)


def test_terminal_állapotból_nem_lehet_továbblépni():
    """Terminal állapotból érkező event nem változtat állapotot."""
    report = make_report(execution_type="NEW", order_status="NEW")
    state, _ = transition_from_execution_report(LocalOrderState.FILLED, report)
    assert state == LocalOrderState.FILLED  # nem változik


def test_is_active():
    assert is_active(LocalOrderState.WORKING)
    assert is_active(LocalOrderState.PARTIALLY_FILLED)
    assert not is_active(LocalOrderState.FILLED)
    assert not is_active(LocalOrderState.CANCELED)
