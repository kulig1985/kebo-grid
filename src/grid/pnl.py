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
    grid_profit: Decimal              # realized: half-match párokból (BUY-SELL párok nettó profitja)
    total_buy_fees: Decimal
    total_sell_fees: Decimal
    completed_matches: int
    unmatched_buy_qty: Decimal
    unmatched_sell_qty: Decimal
    portfolio_quote: Decimal          # wallet_base × current_price + wallet_quote
    total_pnl: Decimal                # portfolio_quote − initial_capital_quote
    # Inventory mark-to-market mezők (volatilitás profit):
    avg_buy_vwap: Decimal             # össz BUY quote / össz BUY base (volume-weighted avg buy ár)
    inventory_value_at_vwap: Decimal  # wallet_base × avg_buy_vwap
    inventory_value_at_current: Decimal  # wallet_base × current_price
    unrealized_pnl: Decimal           # inv_value_current − inv_value_at_vwap
    matches: list[MatchedPair] = field(default_factory=list)


def compute_half_match_profit(
    fills: list[HalfMatch],
    current_price: Decimal = Decimal("0"),
    initial_capital_quote: Decimal = Decimal("0"),
    wallet_base: Decimal = Decimal("0"),
    wallet_quote: Decimal = Decimal("0"),
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

    grid_profit = sum((m.net_profit for m in matches), Decimal("0"))
    total_buy_fees = sum((m.buy_fee for m in matches), Decimal("0"))
    total_sell_fees = sum((m.sell_fee for m in matches), Decimal("0"))
    unmatched_buy_qty = sum((b.remaining for b in buys if b.remaining > 0), Decimal("0"))
    unmatched_sell_qty = sum((s.remaining for s in sells if s.remaining > 0), Decimal("0"))
    portfolio_quote = wallet_base * current_price + wallet_quote
    total_pnl = portfolio_quote - initial_capital_quote

    # Inventory mark-to-market: a wallet-ben lévő open base mennyit ér jelenleg
    # vs. amennyibe TÉNYLEGESEN került (volume-weighted avg buy price az ÖSSZES BUY-ra)
    total_buy_quote_orig = sum((f.quote_qty for f in fills if f.side == "BUY"), Decimal("0"))
    total_buy_base_orig = sum((f.quantity for f in fills if f.side == "BUY"), Decimal("0"))
    if total_buy_base_orig > 0:
        avg_buy_vwap = total_buy_quote_orig / total_buy_base_orig
    else:
        avg_buy_vwap = Decimal("0")

    inventory_value_at_vwap = wallet_base * avg_buy_vwap
    inventory_value_at_current = wallet_base * current_price
    unrealized_pnl = inventory_value_at_current - inventory_value_at_vwap

    return ProfitReport(
        grid_profit=grid_profit,
        total_buy_fees=total_buy_fees,
        total_sell_fees=total_sell_fees,
        completed_matches=len(matches),
        unmatched_buy_qty=unmatched_buy_qty,
        unmatched_sell_qty=unmatched_sell_qty,
        portfolio_quote=portfolio_quote,
        total_pnl=total_pnl,
        avg_buy_vwap=avg_buy_vwap,
        inventory_value_at_vwap=inventory_value_at_vwap,
        inventory_value_at_current=inventory_value_at_current,
        unrealized_pnl=unrealized_pnl,
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
