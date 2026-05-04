"""SQLAlchemy ORM modellek – 11 adatbázis tábla."""
from datetime import datetime
from decimal import Decimal
from typing import Optional
from sqlalchemy import (
    BigInteger, Boolean, DateTime, Integer, Numeric, String, Text, JSON,
    UniqueConstraint, Index, func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class BotRun(Base):
    __tablename__ = "bot_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    base_asset: Mapped[str] = mapped_column(String(10), nullable=False)
    quote_asset: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="INITIALIZING")
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    config_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    anchor_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    grid_type: Mapped[str] = mapped_column(String(20), nullable=False)
    grid_step_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 8))
    grid_step_abs: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    total_capital_quote: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    order_quote_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))  # exchangeInfo után kerül beállításra
    target_net_profit_quote: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    grid_low_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    grid_high_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class GridLevel(Base):
    __tablename__ = "grid_levels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bot_run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    level_index: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    side_zone: Mapped[str] = mapped_column(String(20), nullable=False)  # BELOW_ANCHOR | ABOVE_ANCHOR | ANCHOR
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("bot_run_id", "level_index", name="uq_grid_level"),
    )


class OrderIntent(Base):
    __tablename__ = "order_intents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bot_run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    client_order_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    order_type: Mapped[str] = mapped_column(String(20), nullable=False)
    time_in_force: Mapped[str] = mapped_column(String(10), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    quote_value_estimate: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    grid_level_index: Mapped[Optional[int]] = mapped_column(Integer)
    pair_id: Mapped[Optional[str]] = mapped_column(String(50))
    cycle_id: Mapped[Optional[str]] = mapped_column(String(50))
    local_state: Mapped[str] = mapped_column(String(30), nullable=False, default="PLANNED")
    created_monotonic_ns: Mapped[Optional[int]] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    submit_error_json: Mapped[Optional[dict]] = mapped_column(JSON)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bot_run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    client_order_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    exchange_order_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    order_type: Mapped[str] = mapped_column(String(20), nullable=False)
    time_in_force: Mapped[str] = mapped_column(String(10), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    original_quantity: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    executed_quantity: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False, default=0)
    cumulative_quote_quantity: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False, default=0)
    status_exchange: Mapped[Optional[str]] = mapped_column(String(30))
    status_local: Mapped[str] = mapped_column(String(30), nullable=False, default="SUBMIT_QUEUED")
    reject_reason: Mapped[Optional[str]] = mapped_column(String(100))
    is_working: Mapped[bool] = mapped_column(Boolean, default=False)
    maker_only: Mapped[bool] = mapped_column(Boolean, default=False)
    grid_level_index: Mapped[Optional[int]] = mapped_column(Integer)
    pair_id: Mapped[Optional[str]] = mapped_column(String(50))
    cycle_id: Mapped[Optional[str]] = mapped_column(String(50))
    created_exchange_time: Mapped[Optional[int]] = mapped_column(BigInteger)
    working_time: Mapped[Optional[int]] = mapped_column(BigInteger)
    last_event_time: Mapped[Optional[int]] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("ix_orders_exchange_id", "exchange_order_id", unique=True, postgresql_where="exchange_order_id IS NOT NULL"),
    )


class Fill(Base):
    __tablename__ = "fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bot_run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    client_order_id: Mapped[str] = mapped_column(String(36), nullable=False)
    exchange_order_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    trade_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    execution_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    quote_quantity: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    commission_amount: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    commission_asset: Mapped[Optional[str]] = mapped_column(String(20))
    is_maker: Mapped[bool] = mapped_column(Boolean, default=False)
    transaction_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    raw_event_json: Mapped[Optional[dict]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("symbol", "exchange_order_id", "trade_id", name="uq_fill"),
    )


class ExecutionEvent(Base):
    __tablename__ = "execution_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bot_run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    execution_type: Mapped[Optional[str]] = mapped_column(String(30))
    order_status: Mapped[Optional[str]] = mapped_column(String(30))
    client_order_id: Mapped[Optional[str]] = mapped_column(String(36))
    exchange_order_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    execution_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    trade_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    event_time: Mapped[Optional[int]] = mapped_column(BigInteger)
    transaction_time: Mapped[Optional[int]] = mapped_column(BigInteger)
    raw_json: Mapped[Optional[dict]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_exec_event_execution_id", "execution_id", unique=True,
              postgresql_where="execution_id IS NOT NULL"),
    )


class Balance(Base):
    __tablename__ = "balances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bot_run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    asset: Mapped[str] = mapped_column(String(20), nullable=False)
    free: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    locked: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    source: Mapped[str] = mapped_column(String(30), nullable=False)  # user_stream | reconciliation | startup
    event_time: Mapped[Optional[int]] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ExternalEvent(Base):
    __tablename__ = "external_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bot_run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    event_category: Mapped[str] = mapped_column(String(50), nullable=False)
    symbol: Mapped[Optional[str]] = mapped_column(String(20))
    client_order_id: Mapped[Optional[str]] = mapped_column(String(36))
    exchange_order_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    description: Mapped[Optional[str]] = mapped_column(Text)
    raw_json: Mapped[Optional[dict]] = mapped_column(JSON)
    policy_action: Mapped[Optional[str]] = mapped_column(String(30))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class WsConnection(Base):
    __tablename__ = "ws_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_type: Mapped[str] = mapped_column(String(30), nullable=False)  # trading_api | user_stream | market
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    connected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    disconnected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    reconnect_count: Mapped[int] = mapped_column(Integer, default=0)
    last_msg_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_ping_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_pong_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SystemEvent(Base):
    __tablename__ = "system_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    severity: Mapped[str] = mapped_column(String(10), nullable=False)  # INFO | WARNING | ERROR | CRITICAL
    component: Mapped[str] = mapped_column(String(50), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[Optional[dict]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ApiAudit(Base):
    __tablename__ = "api_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(100), nullable=False)
    method: Mapped[str] = mapped_column(String(10), nullable=False)
    request_json: Mapped[Optional[dict]] = mapped_column(JSON)
    response_status: Mapped[Optional[int]] = mapped_column(Integer)
    actor: Mapped[Optional[str]] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
