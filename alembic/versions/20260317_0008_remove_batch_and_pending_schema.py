"""remove batch and pending schema leftovers

Revision ID: 20260317_0008
Revises: 20260312_0007
Create Date: 2026-03-17 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260317_0008"
down_revision: Union[str, Sequence[str], None] = "20260312_0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index(op.f("ix_pending_events_household_id"), table_name="pending_events")
    op.drop_table("pending_events")

    with op.batch_alter_table("households") as batch_op:
        batch_op.drop_column("auto_add_batch_a_enabled")

    with op.batch_alter_table("preference_rules") as batch_op:
        batch_op.drop_column("bucket")


def downgrade() -> None:
    with op.batch_alter_table("preference_rules") as batch_op:
        batch_op.add_column(sa.Column("bucket", sa.String(length=1), nullable=False, server_default="B"))

    with op.batch_alter_table("households") as batch_op:
        batch_op.add_column(
            sa.Column("auto_add_batch_a_enabled", sa.Boolean(), nullable=False, server_default=sa.true())
        )

    op.create_table(
        "pending_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("origin_channel", sa.String(length=32), nullable=False),
        sa.Column("origin_thread_id", sa.String(length=255), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_pending_events_household_id"), "pending_events", ["household_id"], unique=False)
