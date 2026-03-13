from __future__ import annotations

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class InboundEmailRequest(BaseModel):
    provider: str = "mock-email"
    provider_event_id: str
    provider_message_id: str
    sender: str
    recipient_alias: str
    subject: str = ""
    body_text: str


class InboundSMSRequest(BaseModel):
    provider: str = "mock-sms"
    provider_event_id: str
    provider_message_id: str
    sender_phone: str
    recipient_phone: str
    body_text: str


class InboundResponse(BaseModel):
    status: str
    message: str
    request_id: str
    mutation_executed: bool
    operation_id: Optional[str] = None
    processing_state: str


class OperationResponse(BaseModel):
    operation_id: str
    status: str
    processing_state: str
    last_updated_at: datetime
    mutation_executed: bool
    user_message: str


class ChildIn(BaseModel):
    name: str
    school_name: str
    grade: str = ""


class ChildOut(ChildIn):
    id: int


class SettingsIn(BaseModel):
    daily_summary_enabled: bool
    weekly_digest_enabled: bool


class HouseholdProfileIn(BaseModel):
    admin_email: str
    secondary_admin_email: str = ""
    admin_phone: str = ""
    timezone: str = "UTC"
    spouse_phone: str = ""
    spouse_notifications_enabled: bool = False


class HouseholdProfileOut(HouseholdProfileIn):
    household_id: int


class PreferenceIn(BaseModel):
    raw_text: str = Field(default="")
    system_defaults: dict[str, bool] = Field(default_factory=dict)
    user_priority_topics: list[str] = Field(default_factory=list)
    structured_json: dict = Field(default_factory=dict)


class CalendarBindingIn(BaseModel):
    provider_user_email: str
    token_subject: str
    access_token: str
    calendar_id: str
    calendar_owner_email: str


class CalendarBindingOut(BaseModel):
    provider_user_email: str
    token_subject: str
    calendar_id: str
    calendar_owner_email: str
    status: str


class ReminderIn(BaseModel):
    event_id: int
    channel: str = "sms"
    minutes_before: int = 60


class ReminderOut(BaseModel):
    id: int
    event_id: int
    channel: str
    trigger_at: datetime
    status: str
