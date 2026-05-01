"""
Manuális törlés teszt.

Acceptance criteria:
1. Order WORKING állapotban van
2. executionReport érkezik X=CANCELED-del
3. Nincs helyi cancel kérelem
4. EXTERNAL_CANCELED állapotba kerül
5. external_intervention_policy=pause -> PAUSED_EXTERNAL_INTERVENTION
6. Automatikus counter order NEM kerül küldésre
"""
import asyncio
import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from exchange.models import ExecutionReport
from grid.engine import BotStatus, GridEngine
from grid.state_machine import LocalOrderState, transition_from_execution_report


def make_cancel_report(client_order_id: str = "G-abc123-B-001-0001-01") -> ExecutionReport:
    return ExecutionReport(
        event_time=1700000002000,
        symbol="SOLUSDT",
        client_order_id=client_order_id,
        side="BUY",
        order_type="LIMIT_MAKER",
        time_in_force="GTC",
        original_qty=Decimal("0.06"),
        price=Decimal("80.00"),
        execution_type="CANCELED",
        order_status="CANCELED",
        reject_reason="NONE",
        order_id=123456789,
        last_executed_qty=Decimal("0"),
        cumulative_filled_qty=Decimal("0"),
        last_executed_price=Decimal("0"),
        commission_amount=Decimal("0"),
        commission_asset=None,
        transaction_time=1700000002000,
        trade_id=-1,
        execution_id=None,
        is_on_book=False,
        is_maker=False,
        order_creation_time=1700000000000,
        cumulative_quote_qty=Decimal("0"),
        last_quote_qty=Decimal("0"),
    )


def test_külső_cancel_detektálás():
    """Ha nincs helyi cancel, EXTERNAL_CANCELED kell."""
    report = make_cancel_report()
    state, external = transition_from_execution_report(
        LocalOrderState.WORKING,
        report,
        has_local_cancel_request=False,
    )
    assert state == LocalOrderState.EXTERNAL_CANCELED
    assert external is True


def test_helyi_cancel_nem_külső():
    """Ha van helyi cancel kérelem, CANCELED (nem EXTERNAL)."""
    report = make_cancel_report()
    state, external = transition_from_execution_report(
        LocalOrderState.CANCEL_QUEUED,
        report,
        has_local_cancel_request=True,
    )
    assert state == LocalOrderState.CANCELED
    assert external is False


@pytest.mark.asyncio
async def test_pause_policy_alkalmazása():
    """
    external_intervention_policy=pause esetén
    a bot PAUSED_EXTERNAL_INTERVENTION állapotba kerül.
    """
    from app.config import Settings, BotConfig, SafetyConfig
    from unittest.mock import AsyncMock, MagicMock

    settings = MagicMock(spec=Settings)
    settings.bot = MagicMock(spec=BotConfig)
    settings.bot.symbol = "SOLUSDT"
    settings.safety = MagicMock(spec=SafetyConfig)
    settings.safety.external_intervention_policy = "pause"

    ws_api = MagicMock()
    db_queue = asyncio.Queue()
    event_queue = asyncio.Queue()

    engine = GridEngine(settings, ws_api, db_queue, event_queue)
    engine.status = BotStatus.RUNNING
    engine.bot_run_id = 1

    report = make_cancel_report()
    await engine.on_external_cancel(report)

    assert engine.status == BotStatus.PAUSED_EXTERNAL_INTERVENTION


@pytest.mark.asyncio
async def test_pause_state_megakadályoz_counter_ordert():
    """PAUSED_EXTERNAL_INTERVENTION állapotban nincs counter order küldés."""
    from app.config import Settings, BotConfig, SafetyConfig
    from unittest.mock import AsyncMock, MagicMock, patch

    settings = MagicMock(spec=Settings)
    settings.bot = MagicMock(spec=BotConfig)
    settings.bot.symbol = "SOLUSDT"
    settings.bot.order_type = "LIMIT_MAKER"
    settings.bot.time_in_force = "GTC"
    settings.safety = MagicMock(spec=SafetyConfig)
    settings.safety.external_intervention_policy = "pause"

    ws_api = MagicMock()
    ws_api.enqueue_order = MagicMock()
    db_queue = asyncio.Queue()
    event_queue = asyncio.Queue()

    engine = GridEngine(settings, ws_api, db_queue, event_queue)
    engine.status = BotStatus.PAUSED_EXTERNAL_INTERVENTION
    engine.bot_run_id = 1

    # Fill report küldése – counter order NEM szabad
    fill_report = ExecutionReport(
        event_time=1700000003000, symbol="SOLUSDT",
        client_order_id="G-abc123-B-001-0001-01", side="BUY",
        order_type="LIMIT_MAKER", time_in_force="GTC",
        original_qty=Decimal("0.06"), price=Decimal("80"),
        execution_type="TRADE", order_status="FILLED",
        reject_reason="NONE", order_id=123456789,
        last_executed_qty=Decimal("0.06"), cumulative_filled_qty=Decimal("0.06"),
        last_executed_price=Decimal("80"), commission_amount=Decimal("0"),
        commission_asset=None, transaction_time=1700000003000,
        trade_id=999, execution_id=111, is_on_book=False, is_maker=True,
        order_creation_time=1700000000000, cumulative_quote_qty=Decimal("4.8"),
        last_quote_qty=Decimal("4.8"),
    )

    await engine.on_execution_report(fill_report)

    # Nem küldött order-t
    ws_api.enqueue_order.assert_not_called()
