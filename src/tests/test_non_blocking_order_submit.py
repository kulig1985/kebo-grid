"""
Non-blocking order submission teszt.

Acceptance criteria:
- grid_engine N order intent-et generál
- order_router N üzenetet tesz a queue-ba
- grid_engine NEM vár ACK-ra
- ACK/error érkez later – nincs blokkolás
- executionReport frissíti az állapotot later
- Egyetlen task sem vár egy specifikus order válaszra
"""
import asyncio
import pytest
import time
from decimal import Decimal
from unittest.mock import MagicMock

from grid.order_router import OrderIntent, OrderRouter, make_client_order_id


def make_mock_ws_api():
    ws = MagicMock()
    ws.enqueue_order = MagicMock()
    ws.send_queue = asyncio.Queue()
    return ws


def make_intent(i: int) -> OrderIntent:
    return OrderIntent(
        bot_run_id=1,
        client_order_id=make_client_order_id("abc123", "BUY", i, 0, i),
        symbol="SOLUSDT",
        side="BUY",
        order_type="LIMIT_MAKER",
        time_in_force="GTC",
        price=Decimal("80.00"),
        quantity=Decimal("0.06"),
        grid_level_index=-i,
        quote_value_estimate=Decimal("4.8"),
    )


@pytest.mark.asyncio
async def test_n_order_beküldés_non_blocking():
    """N order beküldi az összes intent-et és azonnal visszatér."""
    N = 10
    ws_api = make_mock_ws_api()
    db_queue: asyncio.Queue = asyncio.Queue()

    from app.config import BotConfig
    config = MagicMock(spec=BotConfig)

    router = OrderRouter(ws_api, db_queue, bot_run_id=1, config=config)

    start = time.monotonic()
    for i in range(1, N + 1):
        router.submit_order(make_intent(i))
    elapsed = time.monotonic() - start

    # Összes N order beküldve
    assert ws_api.enqueue_order.call_count == N

    # Nem kellett várni (< 100ms N=10 intent-re)
    assert elapsed < 0.1, f"submit_order() túl lassú: {elapsed:.3f}s"

    # DB queue-ban is N esemény
    assert db_queue.qsize() == N


@pytest.mark.asyncio
async def test_dupla_submit_idempotens():
    """Ugyanaz a clientOrderId kétszer nem kerül beküldésre."""
    ws_api = make_mock_ws_api()
    db_queue: asyncio.Queue = asyncio.Queue()
    from app.config import BotConfig
    config = MagicMock(spec=BotConfig)
    router = OrderRouter(ws_api, db_queue, bot_run_id=1, config=config)

    intent = make_intent(1)
    router.submit_order(intent)
    router.submit_order(intent)  # dupla

    assert ws_api.enqueue_order.call_count == 1  # csak egyszer


@pytest.mark.asyncio
async def test_ack_nélkül_folytatódik():
    """
    A submit_order() nem vár WS ACK-ra.
    Szimuláljuk: N order intent beküldve, WS reader soha nem válaszol.
    Az N order mégis bekerül a queue-ba.
    """
    ws_api = make_mock_ws_api()
    db_queue: asyncio.Queue = asyncio.Queue()
    from app.config import BotConfig
    config = MagicMock(spec=BotConfig)
    router = OrderRouter(ws_api, db_queue, bot_run_id=1, config=config)

    # Beküldünk 5 order-t
    for i in range(1, 6):
        router.submit_order(make_intent(i))

    # Nem vártunk semmire
    assert ws_api.enqueue_order.call_count == 5

    # A WS send queue-ba kerültek (az enqueue_order hívta)
    # Ez bizonyítja, hogy nem vár ACK-ra


def test_client_order_id_format():
    """clientOrderId helyes formátuma és hossza."""
    cid = make_client_order_id("abc123", "BUY", 5, 42, 7)
    assert len(cid) <= 36
    assert cid.startswith("G-")
    assert "B" in cid

    cid_sell = make_client_order_id("xyz999", "SELL", 10, 1, 99)
    assert "S" in cid_sell
    assert len(cid_sell) <= 36


def test_client_order_id_level_index_negatív():
    """Negatív level index (buy szintek) helyesen kódolódik."""
    cid = make_client_order_id("abc123", "BUY", -3, 0, 1)
    assert "003" in cid  # abs(level_index) formázva
