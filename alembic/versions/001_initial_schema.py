"""Kezdeti adatbázis séma – mind a 11 tábla.

Revision ID: 001
Revises:
Create Date: 2026-05-01
"""
from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bot_runs",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("base_asset", sa.String(10), nullable=False),
        sa.Column("quote_asset", sa.String(10), nullable=False),
        sa.Column("status", sa.String(30), nullable=False, server_default="INITIALIZING"),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("stopped_at", sa.DateTime(timezone=True)),
        sa.Column("config_json", sa.JSON, nullable=False),
        sa.Column("anchor_price", sa.Numeric(24, 8)),
        sa.Column("grid_type", sa.String(20), nullable=False),
        sa.Column("grid_step_pct", sa.Numeric(10, 8)),
        sa.Column("grid_step_abs", sa.Numeric(24, 8)),
        sa.Column("total_capital_quote", sa.Numeric(24, 8), nullable=False),
        sa.Column("order_quote_value", sa.Numeric(24, 8), nullable=False),
        sa.Column("target_net_profit_quote", sa.Numeric(24, 8)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "grid_levels",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("bot_run_id", sa.Integer, sa.ForeignKey("bot_runs.id"), nullable=False),
        sa.Column("level_index", sa.Integer, nullable=False),
        sa.Column("price", sa.Numeric(24, 8), nullable=False),
        sa.Column("side_zone", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("bot_run_id", "level_index", name="uq_grid_level"),
    )
    op.create_index("ix_grid_levels_bot_run_id", "grid_levels", ["bot_run_id"])

    op.create_table(
        "order_intents",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("bot_run_id", sa.Integer, nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False, unique=True),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("order_type", sa.String(20), nullable=False),
        sa.Column("time_in_force", sa.String(10), nullable=False),
        sa.Column("price", sa.Numeric(24, 8), nullable=False),
        sa.Column("quantity", sa.Numeric(24, 8), nullable=False),
        sa.Column("quote_value_estimate", sa.Numeric(24, 8)),
        sa.Column("grid_level_index", sa.Integer),
        sa.Column("pair_id", sa.String(50)),
        sa.Column("cycle_id", sa.String(50)),
        sa.Column("local_state", sa.String(30), nullable=False, server_default="PLANNED"),
        sa.Column("created_monotonic_ns", sa.BigInteger),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.Column("submit_error_json", sa.JSON),
    )
    op.create_index("ix_order_intents_bot_run_id", "order_intents", ["bot_run_id"])

    op.create_table(
        "orders",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("bot_run_id", sa.Integer, nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False, unique=True),
        sa.Column("exchange_order_id", sa.BigInteger),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("order_type", sa.String(20), nullable=False),
        sa.Column("time_in_force", sa.String(10), nullable=False),
        sa.Column("price", sa.Numeric(24, 8), nullable=False),
        sa.Column("original_quantity", sa.Numeric(24, 8), nullable=False),
        sa.Column("executed_quantity", sa.Numeric(24, 8), nullable=False, server_default="0"),
        sa.Column("cumulative_quote_quantity", sa.Numeric(24, 8), nullable=False, server_default="0"),
        sa.Column("status_exchange", sa.String(30)),
        sa.Column("status_local", sa.String(30), nullable=False, server_default="SUBMIT_QUEUED"),
        sa.Column("reject_reason", sa.String(100)),
        sa.Column("is_working", sa.Boolean, server_default="false"),
        sa.Column("maker_only", sa.Boolean, server_default="false"),
        sa.Column("grid_level_index", sa.Integer),
        sa.Column("pair_id", sa.String(50)),
        sa.Column("cycle_id", sa.String(50)),
        sa.Column("created_exchange_time", sa.BigInteger),
        sa.Column("working_time", sa.BigInteger),
        sa.Column("last_event_time", sa.BigInteger),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_orders_bot_run_id", "orders", ["bot_run_id"])
    op.execute(
        "CREATE UNIQUE INDEX ix_orders_exchange_id ON orders (exchange_order_id) "
        "WHERE exchange_order_id IS NOT NULL"
    )

    op.create_table(
        "fills",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("bot_run_id", sa.Integer, nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("exchange_order_id", sa.BigInteger, nullable=False),
        sa.Column("trade_id", sa.BigInteger, nullable=False),
        sa.Column("execution_id", sa.BigInteger),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("price", sa.Numeric(24, 8), nullable=False),
        sa.Column("quantity", sa.Numeric(24, 8), nullable=False),
        sa.Column("quote_quantity", sa.Numeric(24, 8), nullable=False),
        sa.Column("commission_amount", sa.Numeric(24, 8), nullable=False),
        sa.Column("commission_asset", sa.String(20)),
        sa.Column("is_maker", sa.Boolean, server_default="false"),
        sa.Column("transaction_time", sa.BigInteger, nullable=False),
        sa.Column("raw_event_json", sa.JSON),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("symbol", "exchange_order_id", "trade_id", name="uq_fill"),
    )
    op.create_index("ix_fills_bot_run_id", "fills", ["bot_run_id"])

    op.create_table(
        "execution_events",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("bot_run_id", sa.Integer, nullable=False),
        sa.Column("event_type", sa.String(30), nullable=False),
        sa.Column("execution_type", sa.String(30)),
        sa.Column("order_status", sa.String(30)),
        sa.Column("client_order_id", sa.String(36)),
        sa.Column("exchange_order_id", sa.BigInteger),
        sa.Column("execution_id", sa.BigInteger),
        sa.Column("trade_id", sa.BigInteger),
        sa.Column("event_time", sa.BigInteger),
        sa.Column("transaction_time", sa.BigInteger),
        sa.Column("raw_json", sa.JSON),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_exec_events_bot_run_id", "execution_events", ["bot_run_id"])
    op.execute(
        "CREATE UNIQUE INDEX ix_exec_event_execution_id ON execution_events (execution_id) "
        "WHERE execution_id IS NOT NULL"
    )

    op.create_table(
        "balances",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("bot_run_id", sa.Integer, nullable=False),
        sa.Column("asset", sa.String(20), nullable=False),
        sa.Column("free", sa.Numeric(24, 8), nullable=False),
        sa.Column("locked", sa.Numeric(24, 8), nullable=False),
        sa.Column("source", sa.String(30), nullable=False),
        sa.Column("event_time", sa.BigInteger),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_balances_bot_run_id", "balances", ["bot_run_id"])

    op.create_table(
        "external_events",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("bot_run_id", sa.Integer, nullable=False),
        sa.Column("event_category", sa.String(50), nullable=False),
        sa.Column("symbol", sa.String(20)),
        sa.Column("client_order_id", sa.String(36)),
        sa.Column("exchange_order_id", sa.BigInteger),
        sa.Column("description", sa.Text),
        sa.Column("raw_json", sa.JSON),
        sa.Column("policy_action", sa.String(30)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_external_events_bot_run_id", "external_events", ["bot_run_id"])

    op.create_table(
        "ws_connections",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("connection_type", sa.String(30), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("connected_at", sa.DateTime(timezone=True)),
        sa.Column("disconnected_at", sa.DateTime(timezone=True)),
        sa.Column("reconnect_count", sa.Integer, server_default="0"),
        sa.Column("last_msg_at", sa.DateTime(timezone=True)),
        sa.Column("last_ping_at", sa.DateTime(timezone=True)),
        sa.Column("last_pong_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "system_events",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("severity", sa.String(10), nullable=False),
        sa.Column("component", sa.String(50), nullable=False),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("payload_json", sa.JSON),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_system_events_created", "system_events", ["created_at"])

    op.create_table(
        "api_audit",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("endpoint", sa.String(100), nullable=False),
        sa.Column("method", sa.String(10), nullable=False),
        sa.Column("request_json", sa.JSON),
        sa.Column("response_status", sa.Integer),
        sa.Column("actor", sa.String(100)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("api_audit")
    op.drop_table("system_events")
    op.drop_table("ws_connections")
    op.drop_table("external_events")
    op.drop_table("balances")
    op.drop_table("execution_events")
    op.drop_table("fills")
    op.drop_table("orders")
    op.drop_table("order_intents")
    op.drop_table("grid_levels")
    op.drop_table("bot_runs")
