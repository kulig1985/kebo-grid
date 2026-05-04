"""Half-match pool alapú P&L számítás.

Minden fill (BUY/SELL, beleértve bootstrap-et) egy half-match.
Párosítás: greedy closest-price — legkisebb árkülönbségű (buy, sell) pár elsőként.
"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


@dataclass
class HalfMatch:
    fill_id: int
    side: str
    price: Decimal
    quantity: Decimal
    quote_qty: Decimal
    commission: Decimal
    commission_asset: str
    remaining: Decimal


@dataclass
class MatchedPair:
    buy_fill_id: int
    sell_fill_id: int
    buy_price: Decimal
    sell_price: Decimal
    quantity: Decimal
    gross_profit: Decimal
    buy_fee: Decimal
    sell_fee: Decimal

    @property
    def net_profit(self) -> Decimal:
        return self.gross_profit - self.buy_fee - self.sell_fee


@dataclass
class ProfitReport:
    grid_profit: Decimal
    total_buy_fees: Decimal
    total_sell_fees: Decimal
    completed_matches: int
    unmatched_buy_qty: Decimal
    unmatched_sell_qty: Decimal
    unrealized_base_value: Decimal
    total_pnl: Decimal
    matches: list[MatchedPair] = field(default_factory=list)


def compute_half_match_profit(
    fills: list[HalfMatch],
    current_price: Decimal = Decimal("0"),
    initial_investment: Decimal = Decimal("0"),
) -> ProfitReport:
    buys = [HalfMatch(
        fill_id=f.fill_id, side=f.side, price=f.price,
        quantity=f.quantity, quote_qty=f.quote_qty,
        commission=f.commission, commission_asset=f.commission_asset,
        remaining=f.quantity,
    ) for f in fills if f.side == "BUY"]

    sells = [HalfMatch(
        fill_id=f.fill_id, side=f.side, price=f.price,
        quantity=f.quantity, quote_qty=f.quote_qty,
        commission=f.commission, commission_asset=f.commission_asset,
        remaining=f.quantity,
    ) for f in fills if f.side == "SELL"]

    matches: list[MatchedPair] = []

    while True:
        best_pair: Optional[tuple[HalfMatch, HalfMatch]] = None
        best_diff: Optional[Decimal] = None

        for b in buys:
            if b.remaining <= 0:
                continue
            for s in sells:
                if s.remaining <= 0:
                    continue
                if s.price <= b.price:
                    continue
                diff = s.price - b.price
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_pair = (b, s)

        if best_pair is None:
            break

        b, s = best_pair
        match_qty = min(b.remaining, s.remaining)
        gross = match_qty * (s.price - b.price)

        buy_fee = b.commission * (match_qty / b.quantity) if b.quantity > 0 else Decimal("0")
        sell_fee = s.commission * (match_qty / s.quantity) if s.quantity > 0 else Decimal("0")

        matches.append(MatchedPair(
            buy_fill_id=b.fill_id, sell_fill_id=s.fill_id,
            buy_price=b.price, sell_price=s.price,
            quantity=match_qty, gross_profit=gross,
            buy_fee=buy_fee, sell_fee=sell_fee,
        ))
        b.remaining -= match_qty
        s.remaining -= match_qty

    grid_profit = sum(m.net_profit for m in matches)
    total_buy_fees = sum(m.buy_fee for m in matches)
    total_sell_fees = sum(m.sell_fee for m in matches)
    unmatched_buy_qty = sum(b.remaining for b in buys if b.remaining > 0)
    unmatched_sell_qty = sum(s.remaining for s in sells if s.remaining > 0)
    unrealized = unmatched_buy_qty * current_price
    total_pnl = grid_profit + unrealized - initial_investment

    return ProfitReport(
        grid_profit=grid_profit,
        total_buy_fees=total_buy_fees,
        total_sell_fees=total_sell_fees,
        completed_matches=len(matches),
        unmatched_buy_qty=unmatched_buy_qty,
        unmatched_sell_qty=unmatched_sell_qty,
        unrealized_base_value=unrealized,
        total_pnl=total_pnl,
        matches=matches,
    )


class PnlTracker:
    """Backward compat wrapper — az engine egyenlőre hivatkozik rá."""

    def __init__(self):
        self.total_realized_quote = Decimal("0")
        self.completed_cycles = 0

    def summary(self) -> dict:
        return {
            "total_realized_quote": str(self.total_realized_quote),
            "completed_cycles": self.completed_cycles,
        }
