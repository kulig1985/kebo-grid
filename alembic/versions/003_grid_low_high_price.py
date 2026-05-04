"""bot_runs.grid_low_price, grid_high_price – grid sáv alsó/felső határ.

Revision ID: 003
Revises: 002
Create Date: 2026-05-04
"""
import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("bot_runs", sa.Column("grid_low_price", sa.Numeric(24, 8), nullable=True))
    op.add_column("bot_runs", sa.Column("grid_high_price", sa.Numeric(24, 8), nullable=True))


def downgrade() -> None:
    op.drop_column("bot_runs", "grid_high_price")
    op.drop_column("bot_runs", "grid_low_price")
