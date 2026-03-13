"""add reminders table

Revision ID: 20260304_0003
Revises: 20260304_0002
Create Date: 2026-03-04 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260304_0003"
down_revision: Union[str, Sequence[str], None] = "20260304_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "reminders",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("trigger_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"]),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_reminders_event_id"), "reminders", ["event_id"], unique=False)
    op.create_index(op.f("ix_reminders_household_id"), "reminders", ["household_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_reminders_household_id"), table_name="reminders")
    op.drop_index(op.f("ix_reminders_event_id"), table_name="reminders")
    op.drop_table("reminders")
