"""add followup contexts and preference behavior

Revision ID: 20260312_0007
Revises: 20260305_0006
Create Date: 2026-03-12 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260312_0007"
down_revision: Union[str, Sequence[str], None] = "20260305_0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "preference_rules",
        sa.Column("behavior", sa.String(length=32), nullable=False, server_default="mention"),
    )
    op.create_table(
        "followup_contexts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("source_message_id", sa.Integer(), nullable=False),
        sa.Column("origin_channel", sa.String(length=32), nullable=False),
        sa.Column("response_channel", sa.String(length=32), nullable=False),
        sa.Column("thread_or_conversation_key", sa.String(length=255), nullable=False),
        sa.Column("summary_title", sa.String(length=255), nullable=False),
        sa.Column("summary_items_shown", sa.JSON(), nullable=False),
        sa.Column("all_extracted_items", sa.JSON(), nullable=False),
        sa.Column("section_snippets", sa.JSON(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.ForeignKeyConstraint(["source_message_id"], ["source_messages.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_followup_contexts_household_id"), "followup_contexts", ["household_id"], unique=False)
    op.create_index(
        op.f("ix_followup_contexts_source_message_id"),
        "followup_contexts",
        ["source_message_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_followup_contexts_source_message_id"), table_name="followup_contexts")
    op.drop_index(op.f("ix_followup_contexts_household_id"), table_name="followup_contexts")
    op.drop_table("followup_contexts")
    op.drop_column("preference_rules", "behavior")
