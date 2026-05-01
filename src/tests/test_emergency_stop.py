"""
Vészleállítás teszt.

Acceptance criteria:
1. Bot 10 WORKING order-rel rendelkezik
2. emergency_stop parancs érkezik API-n
3. Grid motor azonnal leáll (nem generál új order-t)
4. openOrders.cancelAll bekülve WS-en
5. API azonnal visszatér (nem vár)
6. User stream CANCELED event-ek -> order-ek törölve
7. Ha event-ek nem jönnek -> reconciliation ellenőriz
8. Csak akkor EMERGENCY_STOPPED ha nincs nyitott order
"""
import asyncio
import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from grid.engine import BotStatus, GridEngine
from supervisor.emergency import EmergencyStop


@pytest.fixture
def mock_settings():
    from app.config import Settings, BotConfig, SafetyConfig
    s = MagicMock(spec=Settings)
    s.bot = MagicMock(spec=BotConfig)
    s.bot.symbol = "SOLUSDT"
    s.safety = MagicMock(spec=SafetyConfig)
    s.safety.external_intervention_policy = "pause"
    s.safety.cancel_retry_interval_sec = 0.1
    s.safety.cancel_retry_max = 3
    return s


@pytest.fixture
def mock_engine(mock_settings):
    ws_api = MagicMock()
    db_queue = asyncio.Queue()
    event_queue = asyncio.Queue()
    engine = GridEngine(mock_settings, ws_api, db_queue, event_queue)
    engine.status = BotStatus.RUNNING
    engine.bot_run_id = 1
    engine._stop_event = asyncio.Event()
    return engine


@pytest.mark.asyncio
async def test_emergency_stop_állapot_változás(mock_engine, mock_settings):
    """Emergency stop után az engine EMERGENCY_STOPPING állapotba kerül."""
    ws_api = MagicMock()
    ws_api.enqueue_cancel_all = MagicMock()
    ws_api.get_open_orders = AsyncMock(return_value=[])
    db_queue = asyncio.Queue()

    emergency = EmergencyStop(mock_engine, ws_api, db_queue, mock_settings.safety)
    await emergency.execute("teszt")

    assert mock_engine.status == BotStatus.EMERGENCY_STOPPING


@pytest.mark.asyncio
async def test_cancel_all_beküldve(mock_engine, mock_settings):
    """Emergency stop beküldi a cancelAll parancsot."""
    ws_api = MagicMock()
    ws_api.enqueue_cancel_all = MagicMock()
    ws_api.get_open_orders = AsyncMock(return_value=[])
    db_queue = asyncio.Queue()

    emergency = EmergencyStop(mock_engine, ws_api, db_queue, mock_settings.safety)
    await emergency.execute("teszt")

    ws_api.enqueue_cancel_all.assert_called_once_with("SOLUSDT")


@pytest.mark.asyncio
async def test_emergency_stop_gyors_visszatérés(mock_engine, mock_settings):
    """Az execute() gyorsan visszatér, nem vár az összes cancel-re."""
    ws_api = MagicMock()
    ws_api.enqueue_cancel_all = MagicMock()
    ws_api.get_open_orders = AsyncMock(return_value=[], side_effect=asyncio.sleep(5))
    db_queue = asyncio.Queue()

    emergency = EmergencyStop(mock_engine, ws_api, db_queue, mock_settings.safety)

    import time
    start = time.monotonic()
    await emergency.execute("teszt")
    elapsed = time.monotonic() - start

    assert elapsed < 0.5, f"Emergency stop túl sokáig tartott: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_emergency_stop_újrapróbálkozás(mock_engine, mock_settings):
    """Ha maradnak nyitott order-ek, újra beküldi a cancelAll-t."""
    ws_api = MagicMock()
    ws_api.enqueue_cancel_all = MagicMock()
    # Első hívásra van még order, másodiknál nincs
    ws_api.get_open_orders = AsyncMock(side_effect=[
        [{"orderId": 1}],  # még van order
        [],                 # már nincs
    ])
    db_queue = asyncio.Queue()

    emergency = EmergencyStop(mock_engine, ws_api, db_queue, mock_settings.safety)
    await emergency.execute("teszt")

    # Várunk a háttér task befejezésére
    await asyncio.sleep(0.5)

    # cancelAll-t kétszer kellett hívni
    assert ws_api.enqueue_cancel_all.call_count >= 2


@pytest.mark.asyncio
async def test_motor_nem_generál_order_t_emergency_stopnál(mock_engine, mock_settings):
    """EMERGENCY_STOPPING állapotban az engine nem generál counter order-t."""
    from exchange.models import ExecutionReport

    mock_engine.status = BotStatus.EMERGENCY_STOPPING
    ws_api = mock_engine.ws_api
    ws_api.enqueue_order = MagicMock()

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

    await mock_engine.on_execution_report(fill_report)
    ws_api.enqueue_order.assert_not_called()
