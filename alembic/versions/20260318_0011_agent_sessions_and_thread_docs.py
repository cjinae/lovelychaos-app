"""add agent session storage and thread documents

Revision ID: 20260318_0011
Revises: 20260318_0010
Create Date: 2026-03-18 15:30:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260318_0011"
down_revision: Union[str, Sequence[str], None] = "20260318_0010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "source_messages",
        sa.Column("internet_message_id", sa.String(length=512), nullable=True),
    )
    op.add_column(
        "source_messages",
        sa.Column("in_reply_to_message_id", sa.String(length=512), nullable=True),
    )
    op.add_column(
        "source_messages",
        sa.Column("references_header", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column(
        "source_messages",
        sa.Column("thread_key", sa.String(length=512), nullable=False, server_default=""),
    )
    op.create_index(op.f("ix_source_messages_internet_message_id"), "source_messages", ["internet_message_id"], unique=False)
    op.create_index(op.f("ix_source_messages_thread_key"), "source_messages", ["thread_key"], unique=False)

    op.execute("UPDATE source_messages SET thread_key = provider_message_id WHERE thread_key = ''")
    op.execute(
        "UPDATE source_messages SET internet_message_id = provider_message_id "
        "WHERE source_channel = 'email' AND internet_message_id IS NULL"
    )

    op.create_table(
        "thread_documents",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("source_message_id", sa.Integer(), nullable=False),
        sa.Column("thread_key", sa.String(length=512), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("content_type", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("source_url", sa.Text(), nullable=False, server_default=""),
        sa.Column("extracted_text", sa.Text(), nullable=False, server_default=""),
        sa.Column("openai_file_id", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.ForeignKeyConstraint(["source_message_id"], ["source_messages.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_thread_documents_household_id"), "thread_documents", ["household_id"], unique=False)
    op.create_index(op.f("ix_thread_documents_source_message_id"), "thread_documents", ["source_message_id"], unique=False)
    op.create_index(op.f("ix_thread_documents_thread_key"), "thread_documents", ["thread_key"], unique=False)

    op.create_table(
        "agent_session_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_agent_session_items_session_id"), "agent_session_items", ["session_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_agent_session_items_session_id"), table_name="agent_session_items")
    op.drop_table("agent_session_items")

    op.drop_index(op.f("ix_thread_documents_thread_key"), table_name="thread_documents")
    op.drop_index(op.f("ix_thread_documents_source_message_id"), table_name="thread_documents")
    op.drop_index(op.f("ix_thread_documents_household_id"), table_name="thread_documents")
    op.drop_table("thread_documents")

    op.drop_index(op.f("ix_source_messages_thread_key"), table_name="source_messages")
    op.drop_index(op.f("ix_source_messages_internet_message_id"), table_name="source_messages")
    op.drop_column("source_messages", "thread_key")
    op.drop_column("source_messages", "references_header")
    op.drop_column("source_messages", "in_reply_to_message_id")
    op.drop_column("source_messages", "internet_message_id")
