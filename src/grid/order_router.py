"""
Non-blocking order router.

Az OrderRouter az egyetlen belépési pont az új megbízások létrehozásához.
submit_order() AZONNAL visszatér – soha nem vár ACK-ra vagy DB-re.
"""
import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from app.config import BotConfig
from app.log_setup import get_logger
from exchange.ws_api import BinanceWsApi
from persistence.writer import DbEvent

log = get_logger(__name__)

# clientOrderId formátum: G-{run6}-{S1}-{lvl3}-{cyc4}-{sq2}
# max 36 karakter, pl.: G-a1b2c3-B-005-0001-00 = 22 karakter


def make_client_order_id(
    bot_run_short_id: str,
    side: str,
    level_index: int,
    cycle_id: int,
    seq: int,
) -> str:
    """Determinisztikus, maximum 36 karakteres clientOrderId."""
    s = "B" if side == "BUY" else "S"
    cid = f"G-{bot_run_short_id}-{s}-{abs(level_index):03d}-{cycle_id:04d}-{seq:02d}"
    if len(cid) > 36:
        raise ValueError(f"clientOrderId túl hosszú: {cid}")
    return cid


@dataclass
class OrderIntent:
    """Egy megbízási szándék – a grid motor ezt hozza létre."""
    bot_run_id: int
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    time_in_force: str
    price: Decimal
    quantity: Decimal
    grid_level_index: int
    quote_value_estimate: Optional[Decimal] = None
    pair_id: Optional[str] = None
    cycle_id: Optional[str] = None


class OrderRouter:
    """
    Non-blocking order submission.

    Folyamat:
    1. OrderIntent fogadás
    2. DB queue-ba: SUBMIT_QUEUED állapot mentés
    3. WS send queue-ba: order.place parancs
    4. AZONNAL visszatér

    Az order státusa a user data stream executionReport-ból lesz ismert.
    """

    def __init__(
        self,
        ws_api: BinanceWsApi,
        db_queue: asyncio.Queue[DbEvent],
        bot_run_id: int,
        config: BotConfig,
    ):
        self.ws_api = ws_api
        self.db_queue = db_queue
        self.bot_run_id = bot_run_id
        self.config = config
        self._submitted: set[str] = set()  # dupla submit védelem

    def submit_order(self, intent: OrderIntent) -> None:
        """
        Order beküldése – AZONNAL visszatér, nem vár semmire.

        Idempotens: ugyanaz a clientOrderId kétszer nem kerül beküldésre.
        """
        if intent.client_order_id in self._submitted:
            log.warning("Order már beküldve, kihagyás", cid=intent.client_order_id)
            return

        self._submitted.add(intent.client_order_id)

        # 1. DB queue: intent mentés SUBMIT_QUEUED állapotban
        self.db_queue.put_nowait(DbEvent(
            type="upsert_order_from_intent",
            data={
                "bot_run_id": intent.bot_run_id,
                "client_order_id": intent.client_order_id,
                "symbol": intent.symbol,
                "side": intent.side,
                "order_type": intent.order_type,
                "time_in_force": intent.time_in_force,
                "price": intent.price,
                "original_quantity": intent.quantity,
                "executed_quantity": Decimal("0"),
                "cumulative_quote_quantity": Decimal("0"),
                "status_local": "SUBMIT_QUEUED",
                "grid_level_index": intent.grid_level_index,
                "pair_id": intent.pair_id,
                "cycle_id": intent.cycle_id,
                "quote_value_estimate": intent.quote_value_estimate,
                "created_monotonic_ns": time.monotonic_ns(),
            },
        ))

        # 2. WS send queue: order.place parancs (non-blocking)
        self.ws_api.enqueue_order(
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            price=intent.price,
            quantity=intent.quantity,
            client_order_id=intent.client_order_id,
            time_in_force=intent.time_in_force,
        )

        log.debug("Order bekülve",
                  cid=intent.client_order_id, side=intent.side,
                  level=intent.grid_level_index,
                  price=str(intent.price), qty=str(intent.quantity),
                  notional=f"{intent.quote_value_estimate:.2f}")

    def submit_cancel(self, symbol: str, client_order_id: str) -> None:
        """Cancel küldése queue-ba – non-blocking."""
        self.db_queue.put_nowait(DbEvent(
            type="update_intent_state",
            data={"client_order_id": client_order_id, "state": "CANCEL_QUEUED"},
        ))
        self.ws_api.enqueue_cancel(symbol=symbol, client_order_id=client_order_id)
        log.debug("Cancel bekülve", cid=client_order_id)

    def submit_cancel_all(self, symbol: str) -> None:
        """Összes nyitott order törlése – non-blocking."""
        self.ws_api.enqueue_cancel_all(symbol=symbol)
        log.info("CancelAll bekülve", symbol=symbol)
