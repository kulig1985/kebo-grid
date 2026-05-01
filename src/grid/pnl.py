"""P&L számítás grid ciklusonként."""
from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class CyclePnl:
    """Egy buy-sell ciklus P&L adata."""
    buy_client_order_id: str
    sell_client_order_id: str
    buy_price: Decimal
    sell_price: Decimal
    quantity: Decimal
    buy_commission: Decimal
    sell_commission: Decimal
    buy_commission_asset: str
    sell_commission_asset: str

    @property
    def gross_profit_quote(self) -> Decimal:
        return self.quantity * (self.sell_price - self.buy_price)

    @property
    def net_profit_quote(self) -> Decimal:
        """Nettó profit – csak ha a jutalék quote eszközben volt."""
        commission = Decimal("0")
        if self.buy_commission_asset == "USDT":
            commission += self.buy_commission
        if self.sell_commission_asset == "USDT":
            commission += self.sell_commission
        return self.gross_profit_quote - commission


@dataclass
class PnlTracker:
    """Összesített P&L nyomkövető."""
    total_realized_quote: Decimal = Decimal("0")
    total_commission_quote: Decimal = Decimal("0")
    completed_cycles: int = 0
    cycles: list[CyclePnl] = field(default_factory=list)

    def record_cycle(self, cycle: CyclePnl) -> None:
        self.cycles.append(cycle)
        self.total_realized_quote += cycle.net_profit_quote
        self.completed_cycles += 1

    def summary(self) -> dict:
        return {
            "total_realized_quote": str(self.total_realized_quote),
            "total_commission_quote": str(self.total_commission_quote),
            "completed_cycles": self.completed_cycles,
            "avg_profit_per_cycle": str(
                self.total_realized_quote / self.completed_cycles
                if self.completed_cycles > 0 else Decimal("0")
            ),
        }
