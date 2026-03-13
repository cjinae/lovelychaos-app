"""add informational items table

Revision ID: 20260305_0006
Revises: 20260304_0005
Create Date: 2026-03-05 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260305_0006"
down_revision: Union[str, Sequence[str], None] = "20260304_0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "informational_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("source_message_id", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("details", sa.Text(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.ForeignKeyConstraint(["source_message_id"], ["source_messages.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_informational_items_household_id"), "informational_items", ["household_id"], unique=False)
    op.create_index(
        op.f("ix_informational_items_source_message_id"),
        "informational_items",
        ["source_message_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_informational_items_source_message_id"), table_name="informational_items")
    op.drop_index(op.f("ix_informational_items_household_id"), table_name="informational_items")
    op.drop_table("informational_items")
