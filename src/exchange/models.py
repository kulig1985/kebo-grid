"""Exchange adatmodellek (exchangeInfo, filterek, executionReport)."""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


@dataclass
class PriceFilter:
    min_price: Decimal
    max_price: Decimal
    tick_size: Decimal


@dataclass
class LotSizeFilter:
    min_qty: Decimal
    max_qty: Decimal
    step_size: Decimal


@dataclass
class NotionalFilter:
    min_notional: Decimal
    max_notional: Optional[Decimal] = None
    apply_min_to_market: bool = False


@dataclass
class MaxNumOrdersFilter:
    max_num_orders: int


@dataclass
class SymbolInfo:
    symbol: str
    base_asset: str
    quote_asset: str
    price_filter: PriceFilter
    lot_size: LotSizeFilter
    notional: NotionalFilter
    max_num_orders: Optional[MaxNumOrdersFilter] = None
    base_asset_precision: int = 8
    quote_precision: int = 8
    is_spot_trading_allowed: bool = True

    @classmethod
    def from_exchange_info(cls, symbol_data: dict) -> "SymbolInfo":
        filters = {f["filterType"]: f for f in symbol_data.get("filters", [])}

        pf = filters.get("PRICE_FILTER", {})
        price_filter = PriceFilter(
            min_price=Decimal(pf.get("minPrice", "0")),
            max_price=Decimal(pf.get("maxPrice", "999999999")),
            tick_size=Decimal(pf.get("tickSize", "0.01")),
        )

        lf = filters.get("LOT_SIZE", {})
        lot_size = LotSizeFilter(
            min_qty=Decimal(lf.get("minQty", "0.00001")),
            max_qty=Decimal(lf.get("maxQty", "999999")),
            step_size=Decimal(lf.get("stepSize", "0.00001")),
        )

        # NOTIONAL (újabb) vagy MIN_NOTIONAL (régebbi)
        nf = filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {}))
        notional = NotionalFilter(
            min_notional=Decimal(nf.get("minNotional", "1")),
            max_notional=Decimal(nf["maxNotional"]) if nf.get("maxNotional") else None,
            apply_min_to_market=nf.get("applyMinToMarket", False),
        )

        mno = filters.get("MAX_NUM_ORDERS")
        max_orders = MaxNumOrdersFilter(int(mno["maxNumOrders"])) if mno else None

        return cls(
            symbol=symbol_data["symbol"],
            base_asset=symbol_data["baseAsset"],
            quote_asset=symbol_data["quoteAsset"],
            price_filter=price_filter,
            lot_size=lot_size,
            notional=notional,
            max_num_orders=max_orders,
            base_asset_precision=symbol_data.get("baseAssetPrecision", 8),
            quote_precision=symbol_data.get("quotePrecision", 8),
            is_spot_trading_allowed=symbol_data.get("isSpotTradingAllowed", True),
        )


@dataclass
class ExecutionReport:
    """User data stream executionReport esemény."""
    event_time: int           # E
    symbol: str               # s
    client_order_id: str      # c
    side: str                 # S  BUY | SELL
    order_type: str           # o
    time_in_force: str        # f
    original_qty: Decimal     # q
    price: Decimal            # p
    execution_type: str       # x  NEW|TRADE|CANCELED|REJECTED|EXPIRED
    order_status: str         # X  NEW|PARTIALLY_FILLED|FILLED|CANCELED|REJECTED|EXPIRED
    reject_reason: str        # r
    order_id: int             # i
    last_executed_qty: Decimal  # l
    cumulative_filled_qty: Decimal  # z
    last_executed_price: Decimal   # L
    commission_amount: Decimal     # n
    commission_asset: Optional[str]  # N
    transaction_time: int      # T
    trade_id: int              # t
    execution_id: Optional[int]  # I
    is_on_book: bool           # w
    is_maker: bool             # m
    order_creation_time: int   # O
    cumulative_quote_qty: Decimal  # Z
    last_quote_qty: Decimal    # Y

    @classmethod
    def from_dict(cls, d: dict) -> "ExecutionReport":
        return cls(
            event_time=d["E"],
            symbol=d["s"],
            client_order_id=d["c"],
            side=d["S"],
            order_type=d["o"],
            time_in_force=d["f"],
            original_qty=Decimal(d["q"]),
            price=Decimal(d["p"]),
            execution_type=d["x"],
            order_status=d["X"],
            reject_reason=d.get("r", "NONE"),
            order_id=int(d["i"]),
            last_executed_qty=Decimal(d["l"]),
            cumulative_filled_qty=Decimal(d["z"]),
            last_executed_price=Decimal(d["L"]),
            commission_amount=Decimal(d["n"]),
            commission_asset=d.get("N"),
            transaction_time=d["T"],
            trade_id=int(d["t"]),
            execution_id=int(d["I"]) if d.get("I") else None,
            is_on_book=bool(d.get("w", False)),
            is_maker=bool(d.get("m", False)),
            order_creation_time=d["O"],
            cumulative_quote_qty=Decimal(d["Z"]),
            last_quote_qty=Decimal(d["Y"]),
        )


@dataclass
class BookTicker:
    symbol: str
    bid_price: Decimal
    ask_price: Decimal

    @property
    def mid_price(self) -> Decimal:
        return (self.bid_price + self.ask_price) / 2
