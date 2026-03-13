"""initial schema

Revision ID: 20260303_0001
Revises:
Create Date: 2026-03-03 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260303_0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "households",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("spouse_phone", sa.String(length=32), nullable=True),
        sa.Column("spouse_notifications_enabled", sa.Boolean(), nullable=False),
        sa.Column("auto_add_batch_a_enabled", sa.Boolean(), nullable=False),
        sa.Column("daily_summary_enabled", sa.Boolean(), nullable=False),
        sa.Column("weekly_digest_enabled", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "decision_audits",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("actor", sa.String(length=64), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=True),
        sa.Column("request_id", sa.String(length=64), nullable=False),
        sa.Column("inputs", sa.JSON(), nullable=False),
        sa.Column("model_output", sa.JSON(), nullable=False),
        sa.Column("validator_result", sa.JSON(), nullable=False),
        sa.Column("policy_outcome", sa.JSON(), nullable=False),
        sa.Column("committed_actions", sa.JSON(), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_decision_audits_household_id"), "decision_audits", ["household_id"], unique=False)
    op.create_index(op.f("ix_decision_audits_request_id"), "decision_audits", ["request_id"], unique=False)

    op.create_table(
        "webhook_receipts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("provider_event_id", sa.String(length=255), nullable=False),
        sa.Column("provider_message_id", sa.String(length=255), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "provider_event_id", name="uq_webhook_receipt"),
    )
    op.create_index(op.f("ix_webhook_receipts_provider_event_id"), "webhook_receipts", ["provider_event_id"], unique=False)

    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("phone", sa.String(length=32), nullable=True),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("is_admin", sa.Boolean(), nullable=False),
        sa.Column("verified", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_users_email"), "users", ["email"], unique=False)
    op.create_index(op.f("ix_users_household_id"), "users", ["household_id"], unique=False)

    op.create_table(
        "children",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("school_name", sa.String(length=255), nullable=False),
        sa.Column("grade", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_children_household_id"), "children", ["household_id"], unique=False)

    op.create_table(
        "idempotency_keys",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("scope", sa.String(length=64), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.String(length=64), nullable=False),
        sa.Column("target_ref", sa.String(length=255), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("result_hash", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key", "scope", "household_id", name="uq_idempotency_key"),
    )
    op.create_index(op.f("ix_idempotency_keys_household_id"), "idempotency_keys", ["household_id"], unique=False)
    op.create_index(op.f("ix_idempotency_keys_key"), "idempotency_keys", ["key"], unique=False)

    op.create_table(
        "notification_recipients",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("recipient_type", sa.String(length=32), nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("target", sa.String(length=255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("household_id", "recipient_type", "channel", "target", name="uq_notification_recipient"),
    )
    op.create_index(op.f("ix_notification_recipients_household_id"), "notification_recipients", ["household_id"], unique=False)

    op.create_table(
        "operations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.String(length=64), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("processing_state", sa.String(length=32), nullable=False),
        sa.Column("mutation_executed", sa.Boolean(), nullable=False),
        sa.Column("user_message", sa.String(length=255), nullable=False),
        sa.Column("notify_attempts", sa.Integer(), nullable=False),
        sa.Column("notification_status", sa.String(length=32), nullable=False),
        sa.Column("last_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("operation_id"),
    )
    op.create_index(op.f("ix_operations_household_id"), "operations", ["household_id"], unique=False)
    op.create_index(op.f("ix_operations_operation_id"), "operations", ["operation_id"], unique=True)

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

    op.create_table(
        "preference_profiles",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("structured_json", sa.JSON(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("household_id"),
    )
    op.create_index(op.f("ix_preference_profiles_household_id"), "preference_profiles", ["household_id"], unique=True)

    op.create_table(
        "preference_rules",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=32), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("category", sa.String(length=128), nullable=False),
        sa.Column("bucket", sa.String(length=1), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_preference_rules_household_id"), "preference_rules", ["household_id"], unique=False)

    op.create_table(
        "source_messages",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("provider_message_id", sa.String(length=255), nullable=False),
        sa.Column("source_channel", sa.String(length=32), nullable=False),
        sa.Column("sender", sa.String(length=255), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=False),
        sa.Column("body_text", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "provider_message_id", "household_id", name="uq_source_message"),
    )
    op.create_index(op.f("ix_source_messages_household_id"), "source_messages", ["household_id"], unique=False)
    op.create_index(op.f("ix_source_messages_provider_message_id"), "source_messages", ["provider_message_id"], unique=False)
    op.create_index(op.f("ix_source_messages_sender"), "source_messages", ["sender"], unique=False)

    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("child_id", sa.Integer(), nullable=True),
        sa.Column("source_message_id", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("all_day", sa.Boolean(), nullable=False),
        sa.Column("recurrence_rule", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("calendar_event_id", sa.String(length=255), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["child_id"], ["children.id"]),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.ForeignKeyConstraint(["source_message_id"], ["source_messages.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_events_household_id"), "events", ["household_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_events_household_id"), table_name="events")
    op.drop_table("events")
    op.drop_index(op.f("ix_source_messages_sender"), table_name="source_messages")
    op.drop_index(op.f("ix_source_messages_provider_message_id"), table_name="source_messages")
    op.drop_index(op.f("ix_source_messages_household_id"), table_name="source_messages")
    op.drop_table("source_messages")
    op.drop_index(op.f("ix_preference_rules_household_id"), table_name="preference_rules")
    op.drop_table("preference_rules")
    op.drop_index(op.f("ix_preference_profiles_household_id"), table_name="preference_profiles")
    op.drop_table("preference_profiles")
    op.drop_index(op.f("ix_pending_events_household_id"), table_name="pending_events")
    op.drop_table("pending_events")
    op.drop_index(op.f("ix_operations_operation_id"), table_name="operations")
    op.drop_index(op.f("ix_operations_household_id"), table_name="operations")
    op.drop_table("operations")
    op.drop_index(op.f("ix_notification_recipients_household_id"), table_name="notification_recipients")
    op.drop_table("notification_recipients")
    op.drop_index(op.f("ix_idempotency_keys_key"), table_name="idempotency_keys")
    op.drop_index(op.f("ix_idempotency_keys_household_id"), table_name="idempotency_keys")
    op.drop_table("idempotency_keys")
    op.drop_index(op.f("ix_children_household_id"), table_name="children")
    op.drop_table("children")
    op.drop_index(op.f("ix_users_household_id"), table_name="users")
    op.drop_index(op.f("ix_users_email"), table_name="users")
    op.drop_table("users")
    op.drop_index(op.f("ix_webhook_receipts_provider_event_id"), table_name="webhook_receipts")
    op.drop_table("webhook_receipts")
    op.drop_index(op.f("ix_decision_audits_request_id"), table_name="decision_audits")
    op.drop_index(op.f("ix_decision_audits_household_id"), table_name="decision_audits")
    op.drop_table("decision_audits")
    op.drop_table("households")
