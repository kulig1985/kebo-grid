"""bot_runs.order_quote_value nullable – exchangeInfo után kerül beállításra.

Revision ID: 002
Revises: 001
Create Date: 2026-05-01
"""
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("bot_runs", "order_quote_value", nullable=True)


def downgrade() -> None:
    op.alter_column("bot_runs", "order_quote_value", nullable=False)
