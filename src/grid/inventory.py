"""
Inventory és egyenleg kezelés.

Nyomon követi az elérhető base és quote eszközt,
figyelembe véve a zárolásokat és a tartalékokat.
"""
from decimal import Decimal
from typing import Optional
from app.config import BotConfig
from app.log_setup import get_logger

log = get_logger(__name__)


class InventoryManager:
    """
    In-memory egyenleg állapot.

    A user data stream outboundAccountPosition és balanceUpdate
    eseményeiből frissül.
    """

    def __init__(self, config: BotConfig):
        self.config = config
        self._free: dict[str, Decimal] = {}
        self._locked: dict[str, Decimal] = {}

    def update_from_account(self, balances: list[dict]) -> None:
        """Account lekérdezés eredményéből frissítés."""
        for b in balances:
            asset = b["asset"]
            self._free[asset] = Decimal(str(b.get("free", "0")))
            self._locked[asset] = Decimal(str(b.get("locked", "0")))
        log.debug("Egyenleg frissítve (account)", assets=list(self._free.keys()))

    def update_from_account_position(self, balances: list[dict]) -> None:
        """outboundAccountPosition eseményből frissítés."""
        for b in balances:
            asset = b["a"]
            self._free[asset] = Decimal(b["f"])
            self._locked[asset] = Decimal(b["l"])

    def update_from_balance_update(self, asset: str, delta: Decimal) -> None:
        """balanceUpdate eseményből frissítés."""
        current = self._free.get(asset, Decimal("0"))
        self._free[asset] = current + delta

    def free(self, asset: str) -> Decimal:
        return self._free.get(asset, Decimal("0"))

    def locked(self, asset: str) -> Decimal:
        return self._locked.get(asset, Decimal("0"))

    def available_quote(self) -> Decimal:
        """Elérhető quote eszköz tartalék nélkül."""
        total = self._free.get(self.config.quote_asset, Decimal("0"))
        reserve = total * self.config.quote_reserve_pct
        return max(total - reserve, Decimal("0"))

    def available_base(self) -> Decimal:
        """Elérhető base eszköz tartalék nélkül."""
        total = self._free.get(self.config.base_asset, Decimal("0"))
        reserve = total * self.config.base_reserve_pct
        return max(total - reserve, Decimal("0"))

    def has_base_for_sell(self, quantity: Decimal) -> bool:
        return self.available_base() >= quantity

    def has_quote_for_buy(self, notional: Decimal) -> bool:
        return self.available_quote() >= notional

    def snapshot(self) -> dict[str, dict[str, str]]:
        assets = set(self._free.keys()) | set(self._locked.keys())
        return {
            asset: {
                "free": str(self._free.get(asset, Decimal("0"))),
                "locked": str(self._locked.get(asset, Decimal("0"))),
            }
            for asset in sorted(assets)
        }
