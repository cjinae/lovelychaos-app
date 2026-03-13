from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Household(Base):
    __tablename__ = "households"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    spouse_phone: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    spouse_notifications_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    auto_add_batch_a_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    daily_summary_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    weekly_digest_enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    email: Mapped[str] = mapped_column(String(255), index=True)
    phone: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    is_admin: Mapped[bool] = mapped_column(Boolean, default=True)
    verified: Mapped[bool] = mapped_column(Boolean, default=True)

    household = relationship("Household")


class GoogleCredential(Base):
    __tablename__ = "google_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    provider_user_email: Mapped[str] = mapped_column(String(255))
    token_subject: Mapped[str] = mapped_column(String(255))
    access_token: Mapped[str] = mapped_column(Text, default="")
    refresh_token: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    token_expiry: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="active")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class CalendarBinding(Base):
    __tablename__ = "calendar_bindings"
    __table_args__ = (UniqueConstraint("household_id", name="uq_calendar_binding_household"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    google_credential_id: Mapped[int] = mapped_column(ForeignKey("google_credentials.id"), index=True)
    calendar_id: Mapped[str] = mapped_column(String(255))
    calendar_owner_email: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="active")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class Child(Base):
    __tablename__ = "children"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    name: Mapped[str] = mapped_column(String(100))
    school_name: Mapped[str] = mapped_column(String(255))
    grade: Mapped[str] = mapped_column(String(32), default="")
    status: Mapped[str] = mapped_column(String(32), default="active")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class PreferenceProfile(Base):
    __tablename__ = "preference_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True, unique=True)
    raw_text: Mapped[str] = mapped_column(Text, default="")
    structured_json: Mapped[dict] = mapped_column(JSON, default=dict)
    version: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(32), default="active")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class PreferenceRule(Base):
    __tablename__ = "preference_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    source: Mapped[str] = mapped_column(String(32), default="system_default")
    scope: Mapped[str] = mapped_column(String(32), default="household")
    mode: Mapped[str] = mapped_column(String(32), default="route")
    priority: Mapped[int] = mapped_column(Integer, default=0)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    category: Mapped[str] = mapped_column(String(128), default="general")
    bucket: Mapped[str] = mapped_column(String(1), default="B")
    behavior: Mapped[str] = mapped_column(String(32), default="mention")


class SourceMessage(Base):
    __tablename__ = "source_messages"
    __table_args__ = (UniqueConstraint("provider", "provider_message_id", "household_id", name="uq_source_message"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(64))
    provider_message_id: Mapped[str] = mapped_column(String(255), index=True)
    source_channel: Mapped[str] = mapped_column(String(32), default="email")
    sender: Mapped[str] = mapped_column(String(255), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    subject: Mapped[str] = mapped_column(String(255), default="")
    body_text: Mapped[str] = mapped_column(Text, default="")


class WebhookReceipt(Base):
    __tablename__ = "webhook_receipts"
    __table_args__ = (UniqueConstraint("provider", "provider_event_id", name="uq_webhook_receipt"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(64))
    provider_event_id: Mapped[str] = mapped_column(String(255), index=True)
    provider_message_id: Mapped[str] = mapped_column(String(255), default="")
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="received")
    error_code: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"
    __table_args__ = (UniqueConstraint("key", "scope", "household_id", name="uq_idempotency_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(255), index=True)
    scope: Mapped[str] = mapped_column(String(64), default="webhook")
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    action_type: Mapped[str] = mapped_column(String(64), default="")
    target_ref: Mapped[str] = mapped_column(String(255), default="")
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    result_hash: Mapped[str] = mapped_column(String(255), default="")


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    child_id: Mapped[Optional[int]] = mapped_column(ForeignKey("children.id"), nullable=True)
    source_message_id: Mapped[int] = mapped_column(ForeignKey("source_messages.id"))
    title: Mapped[str] = mapped_column(String(255))
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    all_day: Mapped[bool] = mapped_column(Boolean, default=False)
    recurrence_rule: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="intent_saved")
    calendar_event_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class PendingEvent(Base):
    __tablename__ = "pending_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: utcnow() + timedelta(days=7))
    event_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    origin_channel: Mapped[str] = mapped_column(String(32), default="email")
    origin_thread_id: Mapped[str] = mapped_column(String(255), default="")
    version: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    title: Mapped[str] = mapped_column(String(255), default="")


class Reminder(Base):
    __tablename__ = "reminders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id"), index=True)
    channel: Mapped[str] = mapped_column(String(32), default="sms")
    trigger_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")
    status: Mapped[str] = mapped_column(String(32), default="scheduled")
    version: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class DigestItem(Base):
    __tablename__ = "digest_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    item_type: Mapped[str] = mapped_column(String(32), default="event")
    source_ref: Mapped[str] = mapped_column(String(255), default="")
    priority: Mapped[int] = mapped_column(Integer, default=0)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(32), default="sent")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class InformationalItem(Base):
    __tablename__ = "informational_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    source_message_id: Mapped[int] = mapped_column(ForeignKey("source_messages.id"), index=True)
    title: Mapped[str] = mapped_column(String(255))
    details: Mapped[str] = mapped_column(Text, default="")
    priority: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(32), default="stored")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Operation(Base):
    __tablename__ = "operations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    status: Mapped[str] = mapped_column(String(64), default="command_accepted_for_processing")
    processing_state: Mapped[str] = mapped_column(String(32), default="queued")
    mutation_executed: Mapped[bool] = mapped_column(Boolean, default=False)
    user_message: Mapped[str] = mapped_column(String(255), default="")
    notify_attempts: Mapped[int] = mapped_column(Integer, default=0)
    notification_status: Mapped[str] = mapped_column(String(32), default="pending")
    last_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class DecisionAudit(Base):
    __tablename__ = "decision_audits"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    actor: Mapped[str] = mapped_column(String(64), default="system")
    household_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    request_id: Mapped[str] = mapped_column(String(64), index=True)
    inputs: Mapped[dict] = mapped_column(JSON, default=dict)
    model_output: Mapped[dict] = mapped_column(JSON, default=dict)
    validator_result: Mapped[dict] = mapped_column(JSON, default=dict)
    policy_outcome: Mapped[dict] = mapped_column(JSON, default=dict)
    committed_actions: Mapped[dict] = mapped_column(JSON, default=dict)
    severity: Mapped[str] = mapped_column(String(16), default="info")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class NotificationRecipient(Base):
    __tablename__ = "notification_recipients"
    __table_args__ = (
        UniqueConstraint("household_id", "recipient_type", "channel", "target", name="uq_notification_recipient"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    recipient_type: Mapped[str] = mapped_column(String(32), default="admin")
    channel: Mapped[str] = mapped_column(String(32), default="email")
    target: Mapped[str] = mapped_column(String(255), default="")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class NotificationDelivery(Base):
    __tablename__ = "notification_deliveries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    recipient_type: Mapped[str] = mapped_column(String(32), default="admin")
    channel: Mapped[str] = mapped_column(String(32), default="email")
    target: Mapped[str] = mapped_column(String(255), default="")
    template: Mapped[str] = mapped_column(String(64), default="mutation_notice")
    message: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(32), default="sent")
    provider_ref: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class FollowupContext(Base):
    __tablename__ = "followup_contexts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    household_id: Mapped[int] = mapped_column(ForeignKey("households.id"), index=True)
    source_message_id: Mapped[int] = mapped_column(ForeignKey("source_messages.id"), index=True)
    origin_channel: Mapped[str] = mapped_column(String(32), default="email")
    response_channel: Mapped[str] = mapped_column(String(32), default="sms")
    thread_or_conversation_key: Mapped[str] = mapped_column(String(255), default="")
    summary_title: Mapped[str] = mapped_column(String(255), default="")
    summary_items_shown: Mapped[list] = mapped_column(JSON, default=list)
    all_extracted_items: Mapped[list] = mapped_column(JSON, default=list)
    section_snippets: Mapped[list] = mapped_column(JSON, default=list)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: utcnow() + timedelta(days=7))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
