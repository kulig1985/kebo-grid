"""FastAPI response sémák."""
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    timestamp: str


class BotStatusResponse(BaseModel):
    status: str
    bot_run_id: Optional[int]
    symbol: str
    uptime_sec: Optional[float]
    open_orders: int
    grid_levels: dict


class WsStatusResponse(BaseModel):
    trading_ws_connected: bool
    user_stream_connected: bool
    trading_ws_last_msg_age_sec: float
    user_stream_last_event_age_sec: float
    reconnect_counts: dict


class BotRunResponse(BaseModel):
    id: int
    symbol: str
    status: str
    started_at: Optional[datetime]
    stopped_at: Optional[datetime]
    grid_type: str
    anchor_price: Optional[Decimal]
    total_capital_quote: Decimal
    order_quote_value: Decimal

    class Config:
        from_attributes = True


class OrderResponse(BaseModel):
    client_order_id: str
    exchange_order_id: Optional[int]
    symbol: str
    side: str
    price: Decimal
    original_quantity: Decimal
    executed_quantity: Decimal
    status_local: str
    status_exchange: Optional[str]
    grid_level_index: Optional[int]
    is_working: bool

    class Config:
        from_attributes = True


class FillResponse(BaseModel):
    trade_id: int
    client_order_id: str
    side: str
    price: Decimal
    quantity: Decimal
    quote_quantity: Decimal
    commission_amount: Decimal
    commission_asset: Optional[str]
    is_maker: bool
    transaction_time: int

    class Config:
        from_attributes = True


class BalanceResponse(BaseModel):
    asset: str
    free: Decimal
    locked: Decimal
    source: str

    class Config:
        from_attributes = True


class PnlResponse(BaseModel):
    total_realized_quote: Decimal
    completed_cycles: int
    avg_profit_per_cycle: Decimal


class CommandResponse(BaseModel):
    accepted: bool
    message: str


class StartBotRequest(BaseModel):
    config_override: Optional[dict[str, Any]] = None
