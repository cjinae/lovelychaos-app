"""add sms conversation state storage

Revision ID: 20260318_0009
Revises: 20260317_0008
Create Date: 2026-03-18 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260318_0009"
down_revision: Union[str, Sequence[str], None] = "20260317_0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sms_conversation_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False, server_default="sms"),
        sa.Column("state_type", sa.String(length=64), nullable=False, server_default="followup_selection"),
        sa.Column("requested_action", sa.String(length=32), nullable=False, server_default="add"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("candidate_items", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("source_followup_context_ids", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("prompt_message", sa.Text(), nullable=False, server_default=""),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_sms_conversation_states_household_id"), "sms_conversation_states", ["household_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_sms_conversation_states_household_id"), table_name="sms_conversation_states")
    op.drop_table("sms_conversation_states")
