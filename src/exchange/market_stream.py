"""
Opcionális Binance Market Data Stream kliens.

Cél:
- Kezdeti anchor price meghatározás
- Live ár monitorozás (hard stop határok)
- Csak telemetria – a kereskedési motor NEM függ tőle!
"""
import asyncio
import json
import time
from decimal import Decimal
from typing import Optional

import websockets

from app.config import ExchangeConfig
from app.log_setup import get_logger
from .models import BookTicker

log = get_logger(__name__)


class MarketStream:
    """
    Legjobb bid/ask stream (bookTicker) és utolsó kereskedési ár.

    A kereskedési motor nem blokkolóan kérdezi le az árat.
    """

    def __init__(self, config: ExchangeConfig, symbol: str):
        self.config = config
        self.symbol = symbol.lower()
        self._latest_ticker: Optional[BookTicker] = None
        self._last_trade_price: Optional[Decimal] = None
        self._running = False
        self._last_update_time: float = 0.0

    async def start(self) -> None:
        self._running = True
        base_url = self.config.user_stream_url  # stream URL-t használjuk
        stream = f"{self.symbol}@bookTicker"
        url = f"{base_url}/{stream}"

        while self._running:
            try:
                log.info("Market stream csatlakozás", url=url)
                async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
                    async for message in ws:
                        self._last_update_time = time.monotonic()
                        try:
                            data = json.loads(message)
                            self._latest_ticker = BookTicker(
                                symbol=data["s"],
                                bid_price=Decimal(data["b"]),
                                ask_price=Decimal(data["a"]),
                            )
                        except Exception as e:
                            log.debug("Market stream parse hiba", error=str(e))
            except Exception as e:
                if self._running:
                    log.warning("Market stream megszakadt, újracsatlakozás", error=str(e))
                    await asyncio.sleep(2)

    async def stop(self) -> None:
        self._running = False

    @property
    def mid_price(self) -> Optional[Decimal]:
        if self._latest_ticker:
            return self._latest_ticker.mid_price
        return None

    @property
    def best_bid(self) -> Optional[Decimal]:
        return self._latest_ticker.bid_price if self._latest_ticker else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        return self._latest_ticker.ask_price if self._latest_ticker else None

    async def wait_for_price(self, timeout: float = 10.0) -> Decimal:
        """Vár az első ár megérkezéséig (startup-hoz)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._latest_ticker is not None:
                return self._latest_ticker.mid_price
            await asyncio.sleep(0.1)
        raise TimeoutError(f"Market stream timeout – nincs ár {timeout}s alatt ({self.symbol})")
