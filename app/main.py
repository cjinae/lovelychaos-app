from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import base64
from html import unescape
import hashlib
import hmac
import json
import re
from urllib.parse import urlencode
import uuid
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import httpx
from svix.webhooks import Webhook, WebhookVerificationError
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from twilio.request_validator import RequestValidator

from app.config import settings
from app.db import engine, get_db
from app.enums import ProcessingState, WebhookStatus
from app.models import (
    CalendarBinding,
    Child,
    DecisionAudit,
    DigestItem,
    Event,
    FollowupContext,
    GoogleCredential,
    Household,
    IdempotencyKey,
    InformationalItem,
    NotificationDelivery,
    Operation,
    PendingEvent,
    Reminder,
    PreferenceProfile,
    SourceMessage,
    User,
    WebhookReceipt,
)
from app.schemas import (
    ChildIn,
    InboundEmailRequest,
    InboundResponse,
    OperationResponse,
    CalendarBindingIn,
    CalendarBindingOut,
    HouseholdProfileIn,
    HouseholdProfileOut,
    InboundSMSRequest,
    PreferenceIn,
    ReminderIn,
    ReminderOut,
    SettingsIn,
)
from app.services.attribution import resolve_admin_phone, resolve_admin_sender
from app.services.auto_add import evaluate_auto_add_candidate
from app.services.calendar import (
    CalendarMutationError,
    CalendarProvider,
    GoogleCalendarHttpProvider,
    MockCalendarProvider,
)
from app.services.brief_summary import build_brief_summary
from app.services.content_analysis import (
    AnalysisChunk,
    build_prioritized_chunks,
    build_analysis_text,
    dedupe_extracted_events,
    extract_candidate_links,
    resolve_and_download_links,
)
from app.services.expiry import expire_pending_events
from app.services.digests import build_daily_summary, build_weekly_digest
from app.services.followups import (
    build_more_info_message,
    load_active_followup_context,
    persist_followup_context,
    resolve_followup_item,
    resolve_response_channel,
)
from app.services.google_auth import GoogleAuthError, refresh_google_access_token, should_refresh_token
from app.services.llm import ExtractedEvent, MockDecisionEngine, OpenAIDecisionEngine
from app.services.operations import NotificationSender, process_operation
from app.services.notifications import (
    MockNotificationProvider,
    ResendNotificationProvider,
    dispatch_household_notification,
    send_channel_notification,
)
from app.services.priorities import (
    ensure_priority_rules,
    load_priority_preferences,
    save_command_written_preference,
    save_priority_preferences,
)
from app.services.retention import purge_old_records
from app.services.relevancy import compute_relevancy_evidence
from app.services.school_directory import resolve_school_timezone, search_gta_schools
from app.services.validation import validate_candidate


@asynccontextmanager
async def lifespan(_: FastAPI):
    with Session(bind=engine) as db:
        try:
            seed_data(db)
            db.commit()
        except OperationalError as exc:
            raise RuntimeError(
                "Database schema is not initialized. Run `alembic upgrade head` before starting the app."
            ) from exc
    yield


app = FastAPI(title="LovelyChaos", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
engine_llm = (
    OpenAIDecisionEngine(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        timeout_sec=settings.openai_timeout_sec,
        base_url=settings.openai_base_url,
    )
    if settings.llm_mode == "openai"
    else MockDecisionEngine()
)
notifier = NotificationSender()
calendar_provider: CalendarProvider = (
    GoogleCalendarHttpProvider(settings.google_calendar_timeout_sec)
    if settings.google_calendar_mode == "live"
    else MockCalendarProvider()
)
notification_provider = (
    ResendNotificationProvider(
        api_key=settings.resend_api_key,
        from_email=settings.resend_from_email,
        twilio_account_sid=settings.twilio_account_sid,
        twilio_auth_token=settings.twilio_auth_token,
        twilio_messaging_service_sid=settings.twilio_messaging_service_sid,
        twilio_phone_number=settings.twilio_phone_number,
    )
    if settings.notification_mode == "live"
    else MockNotificationProvider()
)


def _display_subject(subject: str) -> str:
    return (subject or "").strip() or "Untitled email"


def _request_external_url(request: Request) -> str:
    forwarded_proto = request.headers.get("x-forwarded-proto")
    forwarded_host = request.headers.get("x-forwarded-host")
    if not forwarded_proto and not forwarded_host:
        return str(request.url)
    return str(
        request.url.replace(
            scheme=forwarded_proto or request.url.scheme,
            netloc=forwarded_host or request.url.netloc,
        )
    )


def _build_inbound_sms_from_twilio(form_data: dict[str, str]) -> InboundSMSRequest:
    message_sid = form_data.get("MessageSid") or form_data.get("SmsSid") or str(uuid.uuid4())
    return InboundSMSRequest(
        provider="twilio-sms",
        provider_event_id=message_sid,
        provider_message_id=message_sid,
        sender_phone=form_data.get("From", ""),
        recipient_phone=form_data.get("To", ""),
        body_text=form_data.get("Body", ""),
    )


def _set_household_timezone(db: Session, household_id: int, timezone_name: str) -> None:
    normalized = (timezone_name or "").strip() or "UTC"
    household = db.scalar(select(Household).where(Household.id == household_id))
    if household is not None:
        household.timezone = normalized
    users = db.scalars(select(User).where(User.household_id == household_id)).all()
    for user in users:
        user.timezone = normalized


def _resolve_and_apply_school_timezone(db: Session, household_id: int, school_name: str) -> Optional[dict]:
    resolution = resolve_school_timezone(school_name)
    if resolution is None:
        return None
    _set_household_timezone(db, household_id, resolution.timezone)
    return resolution.as_dict()


@dataclass
class EmailIntentClassification:
    mode: str
    command_preface_text: str
    forwarded_body_text: str
    reason: str
    forwarded_boundary_found: bool
    explicit_command_pattern: Optional[str] = None
    ambiguous_pattern: Optional[str] = None


_LOW_SIGNAL_PREFACES = {
    "fyi",
    "for your information",
    "for reference",
    "see below",
    "see attached",
    "forwarding",
}
_EXPLICIT_COMMAND_PATTERNS = [
    (
        "add",
        re.compile(r"^(?:please\s+)?add\b.*$", re.IGNORECASE),
    ),
    (
        "more_info",
        re.compile(
            r"^(?:please\s+)?(?:more info|tell me more|more details|summarize|summary of)\b.*$",
            re.IGNORECASE,
        ),
    ),
    (
        "delete",
        re.compile(r"^(?:please\s+)?delete\b.*$", re.IGNORECASE),
    ),
    (
        "remind",
        re.compile(r"^(?:please\s+)?(?:(?:set\s+(?:a\s+)?)?remind|reminder)\b.*$", re.IGNORECASE),
    ),
]
_AMBIGUOUS_PATTERNS = [
    ("can_you", re.compile(r"\bcan you\b", re.IGNORECASE)),
    ("could_you", re.compile(r"\bcould you\b", re.IGNORECASE)),
    ("what_do_you_think", re.compile(r"\bwhat do you think\b", re.IGNORECASE)),
    ("is_this_important", re.compile(r"\bis this important\b", re.IGNORECASE)),
    ("do_i_need", re.compile(r"\bdo i need\b", re.IGNORECASE)),
    ("please_look_at", re.compile(r"\bplease look at\b", re.IGNORECASE)),
    ("thoughts", re.compile(r"\bthoughts\b", re.IGNORECASE)),
]


def _compact_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _normalize_preface_text(value: str) -> str:
    lines = []
    for raw_line in (value or "").replace("\r", "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line == "--":
            break
        if re.match(r"^sent from my\b", line, flags=re.IGNORECASE):
            break
        if re.match(r"^(from|date|subject|to):", line, flags=re.IGNORECASE):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _find_forward_boundary(lines: list[str]) -> Optional[int]:
    for idx, raw_line in enumerate(lines):
        line = raw_line.strip()
        if not line:
            continue
        if re.match(r"^-+\s*forwarded message\s*-+$", line, flags=re.IGNORECASE):
            return idx
        if re.match(r"^begin forwarded message:", line, flags=re.IGNORECASE):
            return idx
        if re.match(r"^from:", line, flags=re.IGNORECASE):
            window = [item.strip() for item in lines[idx + 1 : idx + 5] if item.strip()]
            has_header_context = any(re.match(r"^(date|subject|to):", item, flags=re.IGNORECASE) for item in window)
            if has_header_context:
                return idx
    return None


def _classify_email_intent(body_text: str) -> EmailIntentClassification:
    normalized = (body_text or "").replace("\r", "")
    lines = normalized.splitlines()
    boundary_idx = _find_forward_boundary(lines)
    forwarded_boundary_found = boundary_idx is not None

    if boundary_idx is None:
        preface_raw = normalized
        forwarded_body = ""
    else:
        preface_raw = "\n".join(lines[:boundary_idx])
        forwarded_body = "\n".join(lines[boundary_idx:]).strip()

    command_preface_text = _normalize_preface_text(preface_raw)
    compact_preface = _compact_text(command_preface_text)
    lowered_preface = compact_preface.lower()

    if not compact_preface:
        return EmailIntentClassification(
            mode="ingestion",
            command_preface_text="",
            forwarded_body_text=forwarded_body,
            reason="no_preface",
            forwarded_boundary_found=forwarded_boundary_found,
        )

    if lowered_preface in _LOW_SIGNAL_PREFACES:
        return EmailIntentClassification(
            mode="ingestion",
            command_preface_text=command_preface_text,
            forwarded_body_text=forwarded_body,
            reason="low_signal_preface",
            forwarded_boundary_found=forwarded_boundary_found,
        )

    for name, pattern in _EXPLICIT_COMMAND_PATTERNS:
        if pattern.match(compact_preface):
            return EmailIntentClassification(
                mode="command",
                command_preface_text=command_preface_text,
                forwarded_body_text=forwarded_body,
                reason="explicit_command_preface",
                forwarded_boundary_found=forwarded_boundary_found,
                explicit_command_pattern=name,
            )

    for name, pattern in _AMBIGUOUS_PATTERNS:
        if pattern.search(compact_preface):
            return EmailIntentClassification(
                mode="ambiguous",
                command_preface_text=command_preface_text,
                forwarded_body_text=forwarded_body,
                reason="ambiguous_preface",
                forwarded_boundary_found=forwarded_boundary_found,
                ambiguous_pattern=name,
            )

    return EmailIntentClassification(
        mode="ingestion",
        command_preface_text=command_preface_text if forwarded_boundary_found else "",
        forwarded_body_text=forwarded_body,
        reason="non_actionable_preface" if forwarded_boundary_found else "non_command_body",
        forwarded_boundary_found=forwarded_boundary_found,
    )


def _email_intent_metadata(intent: EmailIntentClassification) -> dict:
    return {
        "mode": intent.mode,
        "reason": intent.reason,
        "forwarded_boundary_found": intent.forwarded_boundary_found,
        "preface_char_count": len(intent.command_preface_text),
        "explicit_command_pattern": intent.explicit_command_pattern,
        "ambiguous_pattern": intent.ambiguous_pattern,
    }


def _collect_extraction_results(
    chunks: list[AnalysisChunk],
    subject: str,
    household_preferences: str,
    timezone_hint: str,
) -> tuple[list[ExtractedEvent], list[dict], list[str], list[dict]]:
    if not chunks:
        return [], [], [], []

    events: list[ExtractedEvent] = []
    notes: list[str] = []
    chunk_failures: list[dict] = []
    chunk_summaries: list[dict] = []
    for chunk in chunks:
        try:
            result = engine_llm.extract_events(
                chunk.text,
                subject,
                household_preferences=household_preferences,
                timezone_hint=timezone_hint,
            )
            chunk_events = list(result.get("events") or [])
            events.extend(chunk_events)
            email_level_notes = result.get("email_level_notes")
            if email_level_notes:
                notes.append(str(email_level_notes))
            chunk_summaries.append(
                {
                    "chunk_index": chunk.index,
                    "label": chunk.label,
                    "char_count": len(chunk.text),
                    "priority_score": chunk.priority_score,
                    "section_labels": chunk.section_labels,
                    "event_count": len(chunk_events),
                }
            )
        except Exception as exc:
            chunk_failures.append(
                {
                    "chunk_index": chunk.index,
                    "label": chunk.label,
                    "char_count": len(chunk.text),
                    "priority_score": chunk.priority_score,
                    "section_labels": chunk.section_labels,
                    "detail": exc.__class__.__name__,
                }
            )
    return dedupe_extracted_events(events), chunk_summaries, notes, chunk_failures


def _to_user_timezone(value: Optional[datetime], timezone_name: str) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    try:
        return value.astimezone(ZoneInfo(timezone_name))
    except Exception:
        return value.astimezone(timezone.utc)


def _coerce_datetime(value: Optional[object]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _format_event_window(start_at: Optional[object], end_at: Optional[object], timezone_name: str) -> str:
    start_local = _to_user_timezone(_coerce_datetime(start_at), timezone_name)
    end_local = _to_user_timezone(_coerce_datetime(end_at), timezone_name)
    if start_local is None:
        return ""
    day = f"{start_local.strftime('%b')} {start_local.day}"
    if end_local and start_local.date() != end_local.date():
        return f"{day} to {end_local.strftime('%b')} {end_local.day}"
    if start_local.hour == 0 and start_local.minute == 0:
        return day
    time_part = start_local.strftime("%I:%M %p").lstrip("0")
    return f"{day} at {time_part}"


def _serialize_dt(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def seed_data(db: Session) -> None:
    existing_household = db.scalar(select(Household).where(Household.id == 1))
    if existing_household:
        admin = db.scalar(select(User).where(User.household_id == 1, User.email == "admin@example.com"))
        if admin and not admin.phone:
            admin.phone = "+15550000001"
        if not existing_household.spouse_phone:
            existing_household.spouse_phone = "+15550000002"
        credential = db.scalar(select(GoogleCredential).where(GoogleCredential.household_id == 1))
        if not credential:
            credential = GoogleCredential(
                household_id=1,
                provider_user_email="admin@example.com",
                token_subject="admin@example.com",
                access_token="mock-access-token",
                status="active",
            )
            db.add(credential)
            db.flush()
        binding = db.scalar(select(CalendarBinding).where(CalendarBinding.household_id == 1))
        if not binding:
            db.add(
                CalendarBinding(
                    household_id=1,
                    google_credential_id=credential.id,
                    calendar_id="primary",
                    calendar_owner_email=credential.provider_user_email,
                    status="active",
                )
            )
        ensure_priority_rules(db, 1)
        return

    household = Household(id=1, timezone="UTC", spouse_phone="+15550000002", spouse_notifications_enabled=True)
    admin = User(household_id=1, email="admin@example.com", phone="+15550000001", is_admin=True, verified=True)
    profile = PreferenceProfile(household_id=1, raw_text="Closures are critical", structured_json={"user_priority_topics": []})
    db.add_all([household, admin, profile])
    db.flush()

    credential = GoogleCredential(
        household_id=1,
        provider_user_email="admin@example.com",
        token_subject="admin@example.com",
        access_token="mock-access-token",
        status="active",
    )
    db.add(credential)
    db.flush()

    binding = CalendarBinding(
        household_id=1,
        google_credential_id=credential.id,
        calendar_id="primary",
        calendar_owner_email="admin@example.com",
        status="active",
    )
    db.add(binding)
    ensure_priority_rules(db, 1)


def _hash_result(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:24]


def _audit(
    db: Session,
    request_id: str,
    household_id: Optional[int],
    severity: str,
    inputs: dict,
    model_output: dict,
    validator_result: dict,
    policy_outcome: dict,
    committed_actions: dict,
) -> None:
    db.add(
        DecisionAudit(
            request_id=request_id,
            household_id=household_id,
            severity=severity,
            inputs=inputs,
            model_output=model_output,
            validator_result=validator_result,
            policy_outcome=policy_outcome,
            committed_actions=committed_actions,
        )
    )


def _safe_error(status: WebhookStatus, request_id: str, message: str) -> InboundResponse:
    return InboundResponse(
        status=status.value,
        message=message,
        request_id=request_id,
        mutation_executed=False,
        processing_state=ProcessingState.COMPLETED.value,
    )


def _mark_receipt_processed(receipt: WebhookReceipt) -> None:
    receipt.status = "processed"
    receipt.processed_at = datetime.now(timezone.utc)


def _require_admin_key(x_admin_key: Optional[str] = Header(default=None)) -> None:
    # Optional hardening: when ADMIN_API_KEY is set, internal endpoints require the header.
    if not settings.admin_api_key:
        return
    if x_admin_key != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid admin key")


def _safe_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _validate_pending_actionable(pending: PendingEvent) -> Optional[str]:
    now = datetime.now(timezone.utc)
    event_start = _safe_utc(pending.event_start)
    expires_at = _safe_utc(pending.expires_at)
    if pending.status != "pending":
        return f"Pending item is already {pending.status}."
    if expires_at <= now or event_start < now:
        return "Pending item is expired."
    return None


def _handle_more_info_command(
    *,
    request_id: str,
    topic: Optional[str],
    context: Optional[FollowupContext] = None,
) -> InboundResponse:
    if not topic:
        return InboundResponse(
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION.value,
            message="Tell me which topic you want more info about.",
            request_id=request_id,
            mutation_executed=False,
            processing_state=ProcessingState.COMPLETED.value,
        )
    if context is not None:
        match = resolve_followup_item(context, query_text=topic, topic=topic)
        if match is None:
            return InboundResponse(
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION.value,
                message="I couldn't match that topic to the latest school update. Please be more specific.",
                request_id=request_id,
                mutation_executed=False,
                processing_state=ProcessingState.COMPLETED.value,
            )
        return InboundResponse(
            status=WebhookStatus.COMMAND_COMPLETED.value,
            message=build_more_info_message(context, match),
            request_id=request_id,
            mutation_executed=False,
            processing_state=ProcessingState.COMPLETED.value,
        )
    return InboundResponse(
        status=WebhookStatus.COMMAND_COMPLETED.value,
        message=f"More info follow-up captured for '{topic}'.",
        request_id=request_id,
        mutation_executed=False,
        processing_state=ProcessingState.COMPLETED.value,
    )


def _response_target_for_channel(user: User, channel: str) -> str:
    return user.phone if channel == "sms" else user.email


def _send_user_response(
    *,
    db: Session,
    user: User,
    channel: str,
    template: str,
    subject: str,
    message: str,
) -> None:
    target = _response_target_for_channel(user, channel)
    if not target:
        return
    send_channel_notification(
        db=db,
        provider=notification_provider,
        household_id=user.household_id,
        recipient_type="admin",
        channel=channel,
        target=target,
        template=template,
        subject=subject,
        message=message,
    )


def _reply_subject(subject: str) -> str:
    base = _display_subject(subject)
    return base if base.lower().startswith("re:") else f"Re: {base}"


def _command_reply(
    *,
    db: Session,
    user: User,
    channel: str,
    template: str,
    subject: str,
    status: WebhookStatus,
    message: str,
    request_id: str,
    mutation_executed: bool,
) -> InboundResponse:
    _send_user_response(
        db=db,
        user=user,
        channel=channel,
        template=template,
        subject=subject,
        message=message,
    )
    db.commit()
    return InboundResponse(
        status=status.value,
        message=message,
        request_id=request_id,
        mutation_executed=mutation_executed,
        processing_state=ProcessingState.COMPLETED.value,
    )


def _resolve_calendar_context(db: Session, household_id: int) -> tuple[GoogleCredential, CalendarBinding]:
    credential = db.scalar(
        select(GoogleCredential).where(
            GoogleCredential.household_id == household_id,
            GoogleCredential.status == "active",
        )
    )
    binding = db.scalar(
        select(CalendarBinding).where(
            CalendarBinding.household_id == household_id,
            CalendarBinding.status == "active",
        )
    )
    if not credential or not binding:
        raise ValueError("Calendar integration is not configured for this household.")
    if binding.google_credential_id != credential.id:
        raise ValueError("Calendar credential mismatch.")
    if credential.provider_user_email != credential.token_subject:
        raise ValueError("Google credential subject mismatch.")
    if binding.calendar_owner_email != credential.provider_user_email:
        raise ValueError("Calendar owner mismatch.")
    return credential, binding


def _resolve_calendar_context_with_refresh(db: Session, household_id: int) -> tuple[GoogleCredential, CalendarBinding]:
    credential, binding = _resolve_calendar_context(db, household_id)
    if should_refresh_token(credential.token_expiry):
        if not credential.refresh_token:
            raise ValueError("Google credential expired and refresh token is missing.")
        try:
            new_access_token, new_expiry = refresh_google_access_token(
                refresh_token=credential.refresh_token,
                client_id=settings.google_client_id,
                client_secret=settings.google_client_secret,
                timeout_sec=settings.google_calendar_timeout_sec,
            )
        except GoogleAuthError as exc:
            raise ValueError(f"Google token refresh failed: {exc}") from exc
        credential.access_token = new_access_token
        credential.token_expiry = new_expiry
        db.flush()
    return credential, binding


def _tenant_gate_for_calendar_mutation(
    db: Session,
    request_id: str,
    resolved_household_id: int,
    referenced_household_id: int,
) -> Optional[InboundResponse]:
    if referenced_household_id != resolved_household_id:
        _audit(
            db,
            request_id,
            resolved_household_id,
            "high",
            {"resolved_household_id": resolved_household_id, "referenced_household_id": referenced_household_id},
            {},
            {},
            {"status": "tenant_mismatch"},
            {},
        )
        db.commit()
        return _safe_error(
            WebhookStatus.REJECTED_TENANT_MISMATCH,
            request_id,
            "Unable to process this request right now.",
        )

    try:
        _resolve_calendar_context(db, resolved_household_id)
    except ValueError as exc:
        _audit(
            db,
            request_id,
            resolved_household_id,
            "high",
            {"resolved_household_id": resolved_household_id},
            {},
            {},
            {"status": "tenant_mismatch", "reason": str(exc)},
            {},
        )
        db.commit()
        return _safe_error(
            WebhookStatus.REJECTED_TENANT_MISMATCH,
            request_id,
            "Unable to process this request right now.",
        )

    return None


def _handle_reminder_command(
    db: Session,
    request_id: str,
    command: dict,
    user: User,
    provider_message_id: str,
    receipt: WebhookReceipt,
    response_channel: str,
    response_subject: str,
) -> InboundResponse:
    event_id = command["pending_id"]
    if event_id is None:
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="reminder_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="Please include event id to set a reminder.",
            request_id=request_id,
            mutation_executed=False,
        )
    event = db.scalar(select(Event).where(Event.id == event_id))
    if not event:
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="reminder_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="Event not found.",
            request_id=request_id,
            mutation_executed=False,
        )

    gate = _tenant_gate_for_calendar_mutation(db, request_id, user.household_id, event.household_id)
    if gate:
        return gate

    minutes_before = int(command.get("minutes_before") or 60)
    reminder_channel = command.get("reminder_channel") or "sms"
    start_at = _safe_utc(event.start_at)
    trigger_at = start_at - timedelta(minutes=minutes_before)
    now = datetime.now(timezone.utc)
    if trigger_at >= start_at:
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="reminder_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="Reminder must be before event start.",
            request_id=request_id,
            mutation_executed=False,
        )
    if trigger_at <= now:
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="reminder_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="Reminder time is in the past. Please choose a different reminder time.",
            request_id=request_id,
            mutation_executed=False,
        )

    idem_key = f"{provider_message_id}:remind:{event.id}:{minutes_before}:{reminder_channel}"
    existing_idem = db.scalar(
        select(IdempotencyKey).where(
            IdempotencyKey.key == idem_key,
            IdempotencyKey.scope == "command",
            IdempotencyKey.household_id == user.household_id,
        )
    )
    if existing_idem:
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="reminder_set",
            subject=response_subject,
            status=WebhookStatus.COMMAND_COMPLETED,
            message="Command already processed",
            request_id=request_id,
            mutation_executed=False,
        )

    if reminder_channel == "calendar":
        if not event.calendar_event_id:
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel=response_channel,
                template="reminder_clarification",
                subject=response_subject,
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="Event is not linked to Google Calendar.",
                request_id=request_id,
                mutation_executed=False,
            )
        credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
        try:
            calendar_provider.set_event_reminder(
                access_token=credential.access_token,
                calendar_id=binding.calendar_id,
                calendar_event_id=event.calendar_event_id,
                minutes_before=minutes_before,
            )
        except CalendarMutationError:
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel=response_channel,
                template="reminder_clarification",
                subject=response_subject,
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="Unable to set calendar reminder right now.",
                request_id=request_id,
                mutation_executed=False,
            )

    db.add(
        Reminder(
            household_id=user.household_id,
            event_id=event.id,
            channel=reminder_channel,
            trigger_at=trigger_at,
            timezone=event.timezone,
            status="scheduled",
        )
    )
    db.add(
        IdempotencyKey(
            key=idem_key,
            scope="command",
            household_id=user.household_id,
            action_type="remind",
            target_ref=str(event.id),
            result_hash=_hash_result(f"{minutes_before}:{reminder_channel}"),
        )
    )
    _mark_receipt_processed(receipt)
    db.commit()
    return _command_reply(
        db=db,
        user=user,
        channel=response_channel,
        template="reminder_set",
        subject=response_subject,
        status=WebhookStatus.COMMAND_COMPLETED,
        message="Reminder set",
        request_id=request_id,
        mutation_executed=True,
    )


def _build_candidate_clarification(candidates: list[ExtractedEvent], timezone_name: str) -> str:
    lines = ["I found multiple possible events in that email. Reply with a more specific instruction for one of these:"]
    for candidate in candidates[:5]:
        window = _format_event_window(candidate.start_at, candidate.end_at, timezone_name)
        title = (candidate.title or "Untitled item").strip()
        lines.append(f"- {window}: {title}" if window else f"- {title}")
    return "\n".join(lines)


MONTH_NAME_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _parse_month_day_date(value: str, timezone_name: str) -> Optional[tuple[datetime, datetime]]:
    match = re.search(
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(\d{4}))?\b",
        value,
        re.I,
    )
    if not match:
        return None
    month = MONTH_NAME_MAP[match.group(1).lower().rstrip(".")]
    day = int(match.group(2))
    year = int(match.group(3)) if match.group(3) else datetime.now(ZoneInfo(timezone_name)).year
    try:
        zone = ZoneInfo(timezone_name)
    except Exception:
        zone = timezone.utc
    start_local = datetime(year, month, day, 0, 0, tzinfo=zone)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _extract_direct_add_candidate(body_text: str, timezone_name: str) -> Optional[ExtractedEvent]:
    raw = (body_text or "").strip()
    if not raw:
        return None

    patterns = [
        r"^\s*add\s+(?P<title>.+?)\s+to\s+(?:the\s+)?cal(?:endar)?\s+(?:for|on)\s+(?P<date>.+?)\s*$",
        r"^\s*add\s+(?P<title>.+?)\s+(?:for|on)\s+(?P<date>.+?)\s*$",
    ]
    for pattern in patterns:
        match = re.match(pattern, raw, re.I)
        if not match:
            continue
        title = match.group("title").strip(" .")
        parsed = _parse_month_day_date(match.group("date"), timezone_name)
        if not title or parsed is None:
            return None
        start_at, end_at = parsed
        return ExtractedEvent(
            title=title,
            start_at=start_at,
            end_at=end_at,
            category="manual",
            confidence=0.96,
            target_scope="unknown",
            mentioned_names=[],
            mentioned_schools=[],
            target_grades=[],
            preference_match=False,
            model_reason="direct_add_command",
        )
    return None


def _candidate_from_followup_item(item: dict, timezone_name: str) -> Optional[ExtractedEvent]:
    title = str(item.get("title") or item.get("text") or "").strip()
    if not title:
        return None
    start_at = _coerce_datetime(item.get("start_at") or item.get("date_sort_key"))
    end_at = _coerce_datetime(item.get("end_at"))
    if start_at and end_at is None:
        local_start = _to_user_timezone(start_at, timezone_name)
        if local_start.hour == 0 and local_start.minute == 0:
            end_at = (local_start + timedelta(days=1)).astimezone(timezone.utc)
        else:
            end_at = start_at + timedelta(hours=1)
    return ExtractedEvent(
        title=title,
        start_at=start_at,
        end_at=end_at,
        category="followup",
        confidence=0.95,
        target_scope="unknown",
        mentioned_names=[],
        mentioned_schools=[],
        target_grades=[],
        preference_match=False,
        model_reason=str(item.get("reason") or "followup_context"),
    )


def _create_event_from_candidate(
    *,
    db: Session,
    request_id: str,
    user: User,
    source: SourceMessage,
    receipt: WebhookReceipt,
    provider_message_id: str,
    response_channel: str,
    response_subject: str,
    candidate: ExtractedEvent,
    inputs: dict,
    model_output: dict,
) -> InboundResponse:
    validation = validate_candidate(candidate)
    if not validation["valid"]:
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            inputs,
            model_output,
            {"valid": False, "issues": validation.get("issues") or ["no_actionable_event"]},
            {"status": "command_needs_clarification", "reason": "no_actionable_event"},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="add_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="I couldn't find a clear future event to add. Please include the event name and date.",
            request_id=request_id,
            mutation_executed=False,
        )

    idem_key = f"{provider_message_id}:add:{candidate.title}:{candidate.start_at}:{candidate.end_at}"
    existing_idem = db.scalar(
        select(IdempotencyKey).where(
            IdempotencyKey.key == idem_key,
            IdempotencyKey.scope == "command",
            IdempotencyKey.household_id == user.household_id,
        )
    )
    if existing_idem:
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="event_created",
            subject=response_subject,
            status=WebhookStatus.COMMAND_COMPLETED,
            message="Command already processed",
            request_id=request_id,
            mutation_executed=False,
        )

    gate_response = _tenant_gate_for_calendar_mutation(db, request_id, user.household_id, user.household_id)
    if gate_response:
        return gate_response

    event_created = False
    try:
        credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
        calendar_result = calendar_provider.create_event(
            access_token=credential.access_token,
            calendar_id=binding.calendar_id,
            title=candidate.title,
            start_at=candidate.start_at,
            end_at=candidate.end_at,
            timezone=user.timezone,
        )
        db.add(
            Event(
                household_id=user.household_id,
                source_message_id=source.id,
                title=candidate.title,
                start_at=candidate.start_at,
                end_at=candidate.end_at,
                timezone=user.timezone,
                status="calendar_synced",
                calendar_event_id=calendar_result.calendar_event_id,
            )
        )
        db.add(
            IdempotencyKey(
                key=idem_key,
                scope="command",
                household_id=user.household_id,
                action_type="add",
                target_ref=candidate.title,
                result_hash=_hash_result("calendar_synced"),
            )
        )
        event_created = True
        policy_outcome = {"status": "command_completed", "reason": "event_added"}
        committed_actions = {"event_created": candidate.title}
        message = "Added to calendar."
    except CalendarMutationError:
        pending_start = candidate.start_at or (datetime.now(timezone.utc) + timedelta(days=1))
        db.add(
            PendingEvent(
                household_id=user.household_id,
                event_start=pending_start,
                expires_at=datetime.now(timezone.utc) + timedelta(days=7),
                title=candidate.title,
            )
        )
        db.add(
            IdempotencyKey(
                key=idem_key,
                scope="command",
                household_id=user.household_id,
                action_type="add",
                target_ref=candidate.title,
                result_hash=_hash_result("calendar_retry_needed"),
            )
        )
        policy_outcome = {"status": "command_completed", "reason": "calendar_sync_failed_saved"}
        committed_actions = {"saved_for_retry": candidate.title}
        message = "I saved the event, but I couldn't sync it to the calendar right now."

    _audit(
        db,
        request_id,
        user.household_id,
        "info",
        inputs,
        model_output,
        {"valid": True},
        policy_outcome,
        committed_actions,
    )
    _mark_receipt_processed(receipt)
    db.commit()
    return _command_reply(
        db=db,
        user=user,
        channel=response_channel,
        template="event_created",
        subject=response_subject,
        status=WebhookStatus.COMMAND_COMPLETED,
        message=message,
        request_id=request_id,
        mutation_executed=event_created or bool(committed_actions),
    )


def _handle_set_preference_command(
    *,
    db: Session,
    request_id: str,
    user: User,
    receipt: WebhookReceipt,
    response_channel: str,
    response_subject: str,
    topic: Optional[str],
    preference_behavior: Optional[str],
) -> InboundResponse:
    if not topic or preference_behavior not in {"auto_add", "mention", "suppress"}:
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="preference_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="Tell me which topic you want to change and whether I should always add it, mention it, or suppress it.",
            request_id=request_id,
            mutation_executed=False,
        )

    save_command_written_preference(
        db,
        household_id=user.household_id,
        topic=topic,
        behavior=preference_behavior,
    )
    _mark_receipt_processed(receipt)
    db.commit()
    behavior_text = {
        "auto_add": "I'll always add that when I can.",
        "mention": "I'll keep mentioning that in future updates.",
        "suppress": "I won't include updates about that unless you change the preference.",
    }[preference_behavior]
    return _command_reply(
        db=db,
        user=user,
        channel=response_channel,
        template="preference_saved",
        subject=response_subject,
        status=WebhookStatus.COMMAND_COMPLETED,
        message=f"Saved preference for {topic.strip()}. {behavior_text}",
        request_id=request_id,
        mutation_executed=True,
    )


def _followup_items_from_extracted_events(extracted_events: list[ExtractedEvent]) -> list[dict]:
    return [
        {
            "title": event.title,
            "text": event.title,
            "start_at": _serialize_dt(event.start_at),
            "end_at": _serialize_dt(event.end_at),
            "reason": event.model_reason,
            "applies_to": [*list(event.mentioned_names or []), *[f"Gr {grade}" for grade in list(event.target_grades or [])]],
            "target_scope": event.target_scope,
        }
        for event in extracted_events
        if (event.title or "").strip()
    ]


def _handle_add_command(
    db: Session,
    request_id: str,
    user: User,
    source: SourceMessage,
    receipt: WebhookReceipt,
    provider_message_id: str,
    subject: str,
    raw_body_text: str,
    analysis_body_text: str,
    response_channel: str,
    response_subject: str,
    followup_context: Optional[FollowupContext] = None,
    command_topic: Optional[str] = None,
) -> InboundResponse:
    inputs = {
        "subject": subject,
        "body_text": raw_body_text,
        "command_topic": command_topic,
        "response_channel": response_channel,
    }

    if followup_context is not None:
        followup_match = resolve_followup_item(
            followup_context,
            query_text=command_topic or raw_body_text,
            topic=command_topic,
        )
        if followup_match is not None:
            followup_candidate = _candidate_from_followup_item(followup_match.item, user.timezone)
            if followup_candidate and followup_candidate.start_at and followup_candidate.end_at:
                return _create_event_from_candidate(
                    db=db,
                    request_id=request_id,
                    user=user,
                    source=source,
                    receipt=receipt,
                    provider_message_id=provider_message_id,
                    response_channel=response_channel,
                    response_subject=response_subject,
                    candidate=followup_candidate,
                    inputs=inputs,
                    model_output={
                        "source": "followup_context",
                        "matched_item": followup_match.item,
                    },
                )

    direct_candidate = _extract_direct_add_candidate(raw_body_text, user.timezone)
    if direct_candidate is not None:
        return _create_event_from_candidate(
            db=db,
            request_id=request_id,
            user=user,
            source=source,
            receipt=receipt,
            provider_message_id=provider_message_id,
            response_channel=response_channel,
            response_subject=response_subject,
            candidate=direct_candidate,
            inputs=inputs,
            model_output={"source": "direct_command", "candidate_title": direct_candidate.title},
        )

    content_body_text = analysis_body_text or raw_body_text
    candidate_links = extract_candidate_links(content_body_text)
    link_report = resolve_and_download_links(candidate_links)
    analysis_text = build_analysis_text(content_body_text, link_report.attachments)
    if not analysis_text:
        analysis_text = content_body_text
    sections, prioritized_chunks = build_prioritized_chunks(content_body_text, link_report.attachments)

    priority_preferences = load_priority_preferences(db, user.household_id)
    preference_text = priority_preferences["raw_text"]
    extracted_events, chunk_summaries, chunk_notes, chunk_failures = _collect_extraction_results(
        prioritized_chunks,
        subject,
        preference_text,
        user.timezone,
    )
    audit_model_output = {
        "llm": engine_llm.metadata(),
        "analysis": {
            "links": candidate_links,
            "link_attempts": [attempt.__dict__ for attempt in link_report.attempts],
            "attachment_count": len(link_report.attachments),
            "analysis_char_count": len(analysis_text),
            "section_summaries": [
                {
                    "section_index": section.index,
                    "label": section.label,
                    "section_kind": section.section_kind,
                    "priority_score": section.priority_score,
                    "source_kind": section.source_kind,
                    "char_count": len(section.text),
                }
                for section in sections
            ],
            "chunk_summaries": chunk_summaries,
            "chunk_failures": chunk_failures,
        },
    }

    if chunk_failures and not extracted_events:
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            inputs,
            audit_model_output,
            {"valid": False, "issues": ["llm_extraction_error"]},
            {"status": "command_needs_clarification", "reason": "add_extraction_error"},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="add_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="I couldn't find a clear event to add from that message. Please forward the event details again or be more specific.",
            request_id=request_id,
            mutation_executed=False,
        )

    actionable_candidates: list[ExtractedEvent] = []
    candidate_outcomes: list[dict] = []
    for candidate in extracted_events:
        validation = validate_candidate(candidate)
        candidate_outcomes.append(
            {
                "title": candidate.title,
                "start_at": _serialize_dt(candidate.start_at),
                "end_at": _serialize_dt(candidate.end_at),
                "validation": validation,
            }
        )
        if validation["valid"]:
            actionable_candidates.append(candidate)

    audit_model_output["events"] = candidate_outcomes
    if chunk_notes:
        audit_model_output["email_level_notes"] = "\n".join(chunk_notes)

    if not actionable_candidates:
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            inputs,
            audit_model_output,
            {"valid": False, "issues": ["no_actionable_event"]},
            {"status": "command_needs_clarification", "reason": "no_actionable_event"},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="add_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="I couldn't find a clear future event to add from that message. Please reply with the exact event details you want added.",
            request_id=request_id,
            mutation_executed=False,
        )

    if len(actionable_candidates) > 1:
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            inputs,
            audit_model_output,
            {"valid": False, "issues": ["multiple_actionable_events"]},
            {"status": "command_needs_clarification", "reason": "multiple_actionable_events"},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="add_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message=_build_candidate_clarification(actionable_candidates, user.timezone),
            request_id=request_id,
            mutation_executed=False,
        )

    candidate = actionable_candidates[0]
    audit_model_output["selected_event"] = {
        "title": candidate.title,
        "start_at": _serialize_dt(candidate.start_at),
        "end_at": _serialize_dt(candidate.end_at),
    }
    return _create_event_from_candidate(
        db=db,
        request_id=request_id,
        user=user,
        source=source,
        receipt=receipt,
        provider_message_id=provider_message_id,
        response_channel=response_channel,
        response_subject=response_subject,
        candidate=candidate,
        inputs=inputs,
        model_output=audit_model_output,
    )


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _build_oauth_state(household_id: int = 1) -> str:
    payload = {"household_id": household_id, "nonce": str(uuid.uuid4())}
    payload_raw = json.dumps(payload, separators=(",", ":")).encode()
    payload_token = _b64url_encode(payload_raw)
    signature = hmac.new(settings.webhook_secret.encode(), payload_token.encode(), hashlib.sha256).digest()
    return f"{payload_token}.{_b64url_encode(signature)}"


def _verify_oauth_state(state: str) -> dict:
    try:
        payload_token, sig_token = state.split(".", 1)
    except ValueError as exc:
        raise ValueError("invalid state format") from exc
    expected_sig = hmac.new(settings.webhook_secret.encode(), payload_token.encode(), hashlib.sha256).digest()
    provided_sig = _b64url_decode(sig_token)
    if not hmac.compare_digest(expected_sig, provided_sig):
        raise ValueError("invalid state signature")
    payload = json.loads(_b64url_decode(payload_token).decode())
    if "household_id" not in payload:
        raise ValueError("invalid state payload")
    return payload


@app.post("/webhooks/email/inbound", response_model=InboundResponse)
def inbound_email(
    payload: InboundEmailRequest,
    x_signature: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
):
    request_id = str(uuid.uuid4())

    if x_signature != settings.webhook_secret:
        _audit(
            db,
            request_id,
            None,
            "high",
            {"sender": payload.sender},
            {},
            {},
            {"status": "invalid_signature"},
            {},
        )
        db.commit()
        raise HTTPException(status_code=401, detail="Invalid signature")

    existing_receipt = db.scalar(
        select(WebhookReceipt).where(
            WebhookReceipt.provider == payload.provider,
            WebhookReceipt.provider_event_id == payload.provider_event_id,
        )
    )
    if existing_receipt:
        return InboundResponse(
            status=WebhookStatus.INGESTION_ACCEPTED.value,
            message="Duplicate webhook already processed",
            request_id=request_id,
            mutation_executed=False,
            processing_state=ProcessingState.COMPLETED.value,
        )

    receipt = WebhookReceipt(
        provider=payload.provider,
        provider_event_id=payload.provider_event_id,
        provider_message_id=payload.provider_message_id,
        status="received",
    )
    db.add(receipt)

    attribution = resolve_admin_sender(db, payload.sender)
    if attribution.kind == "unverified":
        _audit(
            db,
            request_id,
            None,
            "high",
            payload.model_dump(),
            {},
            {},
            {"status": "rejected_unverified_sender", "match_count": 0},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _safe_error(
            WebhookStatus.REJECTED_UNVERIFIED,
            request_id,
            "We couldn't verify which LovelyChaos account this email belongs to. "
            "Please send from your registered email or update your account email in settings.",
        )

    if attribution.kind == "ambiguous":
        _audit(
            db,
            request_id,
            None,
            "high",
            payload.model_dump(),
            {},
            {},
            {"status": "rejected_ambiguous_sender", "match_count": 2},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _safe_error(
            WebhookStatus.REJECTED_AMBIGUOUS,
            request_id,
            "We couldn't verify which LovelyChaos account this email belongs to. "
            "Please send from your registered email or update your account email in settings.",
        )

    user = attribution.user
    assert user is not None

    source = SourceMessage(
        provider=payload.provider,
        provider_message_id=payload.provider_message_id,
        source_channel="email",
        sender=payload.sender.lower(),
        household_id=user.household_id,
        subject=payload.subject,
        body_text=payload.body_text,
    )
    db.add(source)
    db.flush()

    email_intent = _classify_email_intent(payload.body_text)

    if email_intent.mode == "ambiguous":
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            payload.model_dump(),
            {
                "llm": engine_llm.metadata(),
                "email_intent": _email_intent_metadata(email_intent),
            },
            {"valid": False, "issues": ["ambiguous_forwarded_preface"]},
            {"status": "command_needs_clarification", "reason": "ambiguous_forwarded_preface"},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return InboundResponse(
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION.value,
            message="I saw your note above the forwarded email, but I'm not sure what action you want. Reply with: add this to the calendar, tell me more about a topic, delete an event, or set a reminder.",
            request_id=request_id,
            mutation_executed=False,
            processing_state=ProcessingState.COMPLETED.value,
        )

    if email_intent.mode == "command":
        response_channel = resolve_response_channel(
            origin_channel="email",
            email_intent_mode=email_intent.mode,
            admin_phone=user.phone,
        )
        response_subject = _reply_subject(payload.subject)
        try:
            command = engine_llm.parse_command(email_intent.command_preface_text)
        except Exception as exc:
            _audit(
                db,
                request_id,
                user.household_id,
                "high",
                payload.model_dump(),
                {
                    "llm": engine_llm.metadata(),
                    "email_intent": _email_intent_metadata(email_intent),
                },
                {"valid": False, "issues": ["llm_command_parse_error"]},
                {"status": "command_parse_error", "detail": exc.__class__.__name__},
                {},
            )
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel=response_channel,
                template="command_clarification",
                subject=response_subject,
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="Unable to parse command. Please clarify and retry.",
                request_id=request_id,
                mutation_executed=False,
            )
        if command["action"] == "add":
            followup_context = load_active_followup_context(
                db,
                household_id=user.household_id,
                response_channel=response_channel,
            )
            return _handle_add_command(
                db=db,
                request_id=request_id,
                user=user,
                source=source,
                receipt=receipt,
                provider_message_id=payload.provider_message_id,
                subject=payload.subject,
                raw_body_text=email_intent.command_preface_text or payload.body_text,
                analysis_body_text=email_intent.forwarded_body_text or payload.body_text,
                response_channel=response_channel,
                response_subject=response_subject,
                followup_context=followup_context,
                command_topic=command.get("topic"),
            )
        if command["action"] == "more_info":
            context = load_active_followup_context(
                db,
                household_id=user.household_id,
                response_channel=response_channel,
            )
            _mark_receipt_processed(receipt)
            db.commit()
            more_info = _handle_more_info_command(
                request_id=request_id,
                topic=command.get("topic"),
                context=context,
            )
            return _command_reply(
                db=db,
                user=user,
                channel=response_channel,
                template="more_info",
                subject=response_subject,
                status=WebhookStatus(more_info.status),
                message=more_info.message,
                request_id=request_id,
                mutation_executed=False,
            )

        if command["action"] == "set_preference":
            return _handle_set_preference_command(
                db=db,
                request_id=request_id,
                user=user,
                receipt=receipt,
                response_channel=response_channel,
                response_subject=response_subject,
                topic=command.get("topic"),
                preference_behavior=command.get("preference_behavior"),
            )

        if command["action"] == "delete":
            event_id = command["pending_id"]
            if event_id is None:
                _mark_receipt_processed(receipt)
                db.commit()
                return _command_reply(
                    db=db,
                    user=user,
                    channel=response_channel,
                    template="delete_clarification",
                    subject=response_subject,
                    status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                    message="Please include event id to delete.",
                    request_id=request_id,
                    mutation_executed=False,
                )
            event = db.scalar(select(Event).where(Event.id == event_id))
            if not event:
                _mark_receipt_processed(receipt)
                db.commit()
                return _command_reply(
                    db=db,
                    user=user,
                    channel=response_channel,
                    template="delete_clarification",
                    subject=response_subject,
                    status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                    message="Event not found.",
                    request_id=request_id,
                    mutation_executed=False,
                )

            gate_response = _tenant_gate_for_calendar_mutation(
                db,
                request_id,
                user.household_id,
                event.household_id,
            )
            if gate_response:
                return gate_response

            if not event.calendar_event_id:
                _mark_receipt_processed(receipt)
                db.commit()
                return _command_reply(
                    db=db,
                    user=user,
                    channel=response_channel,
                    template="delete_clarification",
                    subject=response_subject,
                    status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                    message="Event is not linked to Google Calendar.",
                    request_id=request_id,
                    mutation_executed=False,
                )

            credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
            idem_key = f"{payload.provider_message_id}:delete:{event.id}"
            existing_idem = db.scalar(
                select(IdempotencyKey).where(
                    IdempotencyKey.key == idem_key,
                    IdempotencyKey.scope == "command",
                    IdempotencyKey.household_id == user.household_id,
                )
            )
            if existing_idem:
                _mark_receipt_processed(receipt)
                db.commit()
                return _command_reply(
                    db=db,
                    user=user,
                    channel=response_channel,
                    template="event_deleted",
                    subject=response_subject,
                    status=WebhookStatus.COMMAND_COMPLETED,
                    message="Command already processed",
                    request_id=request_id,
                    mutation_executed=False,
                )

            try:
                calendar_provider.delete_event(
                    access_token=credential.access_token,
                    calendar_id=binding.calendar_id,
                    calendar_event_id=event.calendar_event_id,
                )
            except CalendarMutationError:
                _mark_receipt_processed(receipt)
                db.commit()
                return _command_reply(
                    db=db,
                    user=user,
                    channel=response_channel,
                    template="delete_clarification",
                    subject=response_subject,
                    status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                    message="Unable to delete event from calendar. Please try again.",
                    request_id=request_id,
                    mutation_executed=False,
                )

            event.status = "deleted"
            event.version += 1
            db.add(
                IdempotencyKey(
                    key=idem_key,
                    scope="command",
                    household_id=user.household_id,
                    action_type="delete",
                    target_ref=str(event.id),
                    result_hash=_hash_result("deleted"),
                )
            )
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel=response_channel,
                template="event_deleted",
                subject=response_subject,
                status=WebhookStatus.COMMAND_COMPLETED,
                message="Event deleted",
                request_id=request_id,
                mutation_executed=True,
            )

        if command["action"] == "remind":
            return _handle_reminder_command(
                db=db,
                request_id=request_id,
                command=command,
                user=user,
                provider_message_id=payload.provider_message_id,
                receipt=receipt,
                response_channel=response_channel,
                response_subject=response_subject,
            )

        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel=response_channel,
            template="command_clarification",
            subject=response_subject,
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="Unsupported command for this flow.",
            request_id=request_id,
            mutation_executed=False,
        )

    content_body_text = email_intent.forwarded_body_text or payload.body_text
    candidate_links = extract_candidate_links(content_body_text)
    link_report = resolve_and_download_links(candidate_links)
    analysis_text = build_analysis_text(content_body_text, link_report.attachments)
    if not analysis_text:
        analysis_text = content_body_text
    sections, prioritized_chunks = build_prioritized_chunks(content_body_text, link_report.attachments)
    section_summaries = [
        {
            "section_index": section.index,
            "label": section.label,
            "section_kind": section.section_kind,
            "priority_score": section.priority_score,
            "source_kind": section.source_kind,
            "char_count": len(section.text),
        }
        for section in sections
    ]

    priority_preferences = load_priority_preferences(db, user.household_id)
    preference_text = priority_preferences["raw_text"]
    children = db.scalars(select(Child).where(Child.household_id == user.household_id, Child.status == "active")).all()
    extracted_events, chunk_summaries, chunk_notes, chunk_failures = _collect_extraction_results(
        prioritized_chunks,
        payload.subject,
        preference_text,
        user.timezone,
    )
    analysis_audit = {
        "links": candidate_links,
        "link_attempts": [attempt.__dict__ for attempt in link_report.attempts],
        "attachment_count": len(link_report.attachments),
        "analysis_char_count": len(analysis_text),
        "section_summaries": section_summaries,
        "chunk_summaries": chunk_summaries,
        "chunk_failures": chunk_failures,
    }
    if chunk_failures and not extracted_events:
        _audit(
            db,
            request_id,
            user.household_id,
            "high",
            payload.model_dump(),
            {
                "llm": engine_llm.metadata(),
                "email_intent": _email_intent_metadata(email_intent),
                "analysis": analysis_audit,
            },
            {"valid": False, "issues": ["llm_extraction_error"]},
            {"status": "rejected_validation", "detail": chunk_failures[0]["detail"]},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _safe_error(WebhookStatus.REJECTED_VALIDATION, request_id, "Could not validate event details.")
    if not extracted_events:
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            payload.model_dump(),
            {
                "llm": engine_llm.metadata(),
                "email_intent": _email_intent_metadata(email_intent),
                "analysis": analysis_audit,
            },
            {"valid": False, "issues": ["empty_extraction"]},
            {"status": "rejected_validation"},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _safe_error(WebhookStatus.REJECTED_VALIDATION, request_id, "Could not validate event details.")

    household = db.scalar(select(Household).where(Household.id == user.household_id))
    assert household is not None
    mutation_executed = False
    has_relevant_event = False
    outcome_counts = {"create_event": 0, "pending_event": 0, "informational_item": 0}
    per_event_outcomes: list[dict] = []
    any_valid = False
    summary_result = None
    summary_audit = None

    for idx, candidate in enumerate(extracted_events, start=1):
        validation = validate_candidate(candidate)
        is_actionable = validation["valid"]
        any_valid = any_valid or is_actionable

        event_text = " ".join(
            [
                candidate.title or "",
                " ".join(candidate.mentioned_names or []),
                " ".join(candidate.mentioned_schools or []),
                " ".join(candidate.target_grades or []),
                candidate.model_reason or "",
            ]
        )
        relevancy = compute_relevancy_evidence(
            event_text=event_text,
            target_grades=list(candidate.target_grades or []),
            model_preference_match=bool(candidate.preference_match),
            children=children,
            preference_text=preference_text,
        )
        is_relevant = relevancy.is_relevant
        has_relevant_event = has_relevant_event or is_relevant
        target_scope = (candidate.target_scope or "unknown").strip()
        is_school_global = target_scope == "school_global"
        auto_add_decision = evaluate_auto_add_candidate(candidate, relevancy, children)

        if is_relevant and is_actionable and auto_add_decision.allow:
            execution_disposition = "create_event"
            final_reason = "relevant_and_actionable_auto_add"
        elif is_relevant:
            execution_disposition = "pending_event"
            final_reason = "relevant_but_not_actionable" if not is_actionable else "relevant_but_needs_confirmation"
        elif is_school_global:
            execution_disposition = "informational_item"
            final_reason = "not_relevant_school_global"
        else:
            execution_disposition = ""
            final_reason = "not_relevant"

        idempotency_key = None
        if execution_disposition:
            idempotency_key = (
                f"{payload.provider_message_id}:route:{idx}:{candidate.title}:{candidate.start_at}:"
                f"{candidate.end_at}:{execution_disposition}"
            )
        existing_idem = None
        if idempotency_key:
            existing_idem = db.scalar(
                select(IdempotencyKey).where(
                    IdempotencyKey.key == idempotency_key,
                    IdempotencyKey.scope == "event_route",
                    IdempotencyKey.household_id == user.household_id,
                )
            )
        if existing_idem:
            per_event_outcomes.append(
                {
                    "index": idx,
                    "title": candidate.title,
                    "start_at": _serialize_dt(candidate.start_at),
                    "end_at": _serialize_dt(candidate.end_at),
                    "validation": validation,
                "relevancy_evidence": relevancy.as_dict(),
                    "auto_add_decision": {"allow": auto_add_decision.allow, "reason": auto_add_decision.reason},
                    "execution_disposition": execution_disposition or None,
                    "final_reason": final_reason,
                    "action": {"idempotent_skip": True},
                }
            )
            if execution_disposition:
                outcome_counts[execution_disposition] += 1
            continue

        action: dict = {}
        if execution_disposition == "create_event":
            if household.auto_add_batch_a_enabled and candidate.start_at and candidate.end_at:
                gate_response = _tenant_gate_for_calendar_mutation(
                    db,
                    request_id,
                    user.household_id,
                    user.household_id,
                )
                if gate_response:
                    return gate_response

                credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
                try:
                    calendar_result = calendar_provider.create_event(
                        access_token=credential.access_token,
                        calendar_id=binding.calendar_id,
                        title=candidate.title,
                        start_at=candidate.start_at,
                        end_at=candidate.end_at,
                        timezone=user.timezone,
                    )
                    db.add(
                        Event(
                            household_id=user.household_id,
                            source_message_id=source.id,
                            title=candidate.title,
                            start_at=candidate.start_at,
                            end_at=candidate.end_at,
                            timezone=user.timezone,
                            status="calendar_synced",
                            calendar_event_id=calendar_result.calendar_event_id,
                        )
                    )
                    mutation_executed = True
                    action = {"event": "created", "calendar_synced": True}
                    dispatch_household_notification(
                        db=db,
                        provider=notification_provider,
                        household_id=user.household_id,
                        template="event_created",
                        subject="LovelyChaos event added",
                        message=f"Event '{candidate.title}' was added to calendar.",
                    )
                except CalendarMutationError:
                    pending_start = candidate.start_at or (datetime.now(timezone.utc) + timedelta(days=1))
                    db.add(
                        PendingEvent(
                            household_id=user.household_id,
                            event_start=pending_start,
                            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
                            title=candidate.title,
                        )
                    )
                    mutation_executed = True
                    action = {"pending": "created", "calendar_sync": "failed"}
            else:
                pending_start = candidate.start_at or (datetime.now(timezone.utc) + timedelta(days=1))
                db.add(
                    PendingEvent(
                        household_id=user.household_id,
                        event_start=pending_start,
                        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
                        title=candidate.title,
                    )
                )
                mutation_executed = True
                action = {"pending": "created", "reason": "auto_add_disabled"}
        elif execution_disposition == "pending_event":
            pending_start = candidate.start_at or (datetime.now(timezone.utc) + timedelta(days=1))
            db.add(
                PendingEvent(
                    household_id=user.household_id,
                    event_start=pending_start,
                    expires_at=datetime.now(timezone.utc) + timedelta(days=7),
                    title=candidate.title,
                )
            )
            mutation_executed = True
            action = {"pending": "created"}
        elif execution_disposition == "informational_item":
            db.add(
                InformationalItem(
                    household_id=user.household_id,
                    source_message_id=source.id,
                    title=candidate.title,
                    details=candidate.model_reason or "",
                    priority=1 if is_school_global else 0,
                    status="stored",
                )
            )
            action = {"informational": "stored"}
        else:
            action = {"ignored": True}

        if idempotency_key:
            db.add(
                IdempotencyKey(
                    key=idempotency_key,
                    scope="event_route",
                    household_id=user.household_id,
                    action_type="route",
                    target_ref=f"{source.id}:{idx}",
                    result_hash=_hash_result(execution_disposition),
                )
            )
        if execution_disposition:
            outcome_counts[execution_disposition] += 1
        per_event_outcomes.append(
            {
                "index": idx,
                "title": candidate.title,
                "start_at": _serialize_dt(candidate.start_at),
                "end_at": _serialize_dt(candidate.end_at),
                "validation": validation,
                "relevancy_evidence": relevancy.as_dict(),
                "auto_add_decision": {"allow": auto_add_decision.allow, "reason": auto_add_decision.reason},
                "execution_disposition": execution_disposition or None,
                "final_reason": final_reason,
                "model_reason": candidate.model_reason,
                "action": action,
            }
        )

    summary_result, summary_audit = build_brief_summary(
        engine=engine_llm,
        subject=payload.subject,
        timezone_name=user.timezone,
        household_preferences=preference_text,
        system_defaults={item["key"]: bool(item["enabled"]) for item in priority_preferences["system_defaults"]},
        user_priority_topics=list(priority_preferences["user_priority_topics"]),
        children=children,
        extracted_events=extracted_events,
        per_event_outcomes=per_event_outcomes,
        sections=sections,
        analysis_text=analysis_text,
        chunk_notes=chunk_notes,
    )
    response_channel = resolve_response_channel(
        origin_channel="email",
        email_intent_mode=email_intent.mode,
        admin_phone=user.phone,
    )
    model_output = {
        "llm": engine_llm.metadata(),
        "email_intent": _email_intent_metadata(email_intent),
        "events": [{"title": e.title, "confidence": e.confidence} for e in extracted_events],
        "email_level_notes": "\n".join(chunk_notes) if chunk_notes else None,
        "analysis": analysis_audit,
        "summary": summary_audit,
    }

    if not any_valid and has_relevant_event:
        _mark_receipt_processed(receipt)
        _send_user_response(
            db=db,
            user=user,
            channel=response_channel,
            template="email_analysis_recap",
            subject=f"LovelyChaos: {_display_subject(payload.subject)}",
            message=summary_result.rendered_message,
        )
        persist_followup_context(
            db,
            household_id=user.household_id,
            source_message_id=source.id,
            origin_channel="email",
            response_channel=response_channel,
            thread_or_conversation_key=payload.provider_message_id,
            summary_title=summary_result.title,
            summary_items_shown=[
                *[item.as_dict() for item in summary_result.important_dates],
                *[item.as_dict() for item in summary_result.important_items],
                *[item.as_dict() for item in summary_result.other_topics],
            ],
            all_extracted_items=_followup_items_from_extracted_events(extracted_events),
            section_snippets=[
                {
                    "label": item.get("label"),
                    "text": item.get("text"),
                }
                for item in list(summary_audit.get("prefilter", {}).get("kept_sections") or [])
                if item.get("text")
            ],
        )
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            payload.model_dump(),
            model_output,
            {"valid": False, "issues": ["no_actionable_relevant_events"]},
            {"status": "processed", "counts": outcome_counts},
            {"event_outcomes": per_event_outcomes},
        )
        db.commit()
        return InboundResponse(
            status=WebhookStatus.INGESTION_ACCEPTED.value,
            message="Relevant school updates were summarized for follow-up.",
            request_id=request_id,
            mutation_executed=mutation_executed,
            processing_state=ProcessingState.COMPLETED.value,
        )

    _audit(
        db,
        request_id,
        user.household_id,
        "info",
        payload.model_dump(),
        model_output,
        {"valid": any_valid, "event_outcomes": per_event_outcomes},
        {"status": "processed", "counts": outcome_counts},
        {"event_outcomes": per_event_outcomes},
    )

    _mark_receipt_processed(receipt)
    _send_user_response(
        db=db,
        user=user,
        channel=response_channel,
        template="email_analysis_recap",
        subject=f"LovelyChaos: {_display_subject(payload.subject)}",
        message=summary_result.rendered_message,
    )
    persist_followup_context(
        db,
        household_id=user.household_id,
        source_message_id=source.id,
        origin_channel="email",
        response_channel=response_channel,
        thread_or_conversation_key=payload.provider_message_id,
        summary_title=summary_result.title,
        summary_items_shown=[
            *[item.as_dict() for item in summary_result.important_dates],
            *[item.as_dict() for item in summary_result.important_items],
            *[item.as_dict() for item in summary_result.other_topics],
        ],
        all_extracted_items=_followup_items_from_extracted_events(extracted_events),
        section_snippets=[
            {
                "label": item.get("label"),
                "text": item.get("text"),
            }
            for item in list(summary_audit.get("prefilter", {}).get("kept_sections") or [])
            if item.get("text")
        ],
    )
    db.commit()
    return InboundResponse(
        status=WebhookStatus.INGESTION_ACCEPTED.value,
        message=(
            f"Processed {len(extracted_events)} item(s): "
            f"{outcome_counts['create_event']} calendar updates, "
            f"{outcome_counts['informational_item']} summarized topics."
        ),
        request_id=request_id,
        mutation_executed=mutation_executed,
        processing_state=ProcessingState.COMPLETED.value,
    )


def _extract_email_address(value: Optional[str]) -> str:
    if not value:
        return ""
    raw = value.strip()
    if "<" in raw and ">" in raw:
        return raw.split("<", 1)[1].split(">", 1)[0].strip().lower()
    return raw.lower()


def _first_recipient(value: object) -> str:
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, str):
            return first
        if isinstance(first, dict):
            return str(first.get("email") or first.get("address") or "")
        return ""
    if isinstance(value, str):
        return value
    return ""


def _html_to_text(value: str) -> str:
    text = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def _extract_best_body_text(data: dict, payload: dict) -> str:
    direct_candidates = [
        data.get("text"),
        data.get("plain_text"),
        data.get("body_text"),
        data.get("stripped-text"),
        data.get("text_content"),
        data.get("content"),
        payload.get("text"),
        payload.get("plain_text"),
        payload.get("body_text"),
    ]
    text_candidates = [str(v).strip() for v in direct_candidates if isinstance(v, str) and v.strip()]
    if text_candidates:
        return max(text_candidates, key=len)

    html_candidates_raw = [
        data.get("html"),
        data.get("body_html"),
        data.get("html_content"),
        payload.get("html"),
    ]
    html_candidates = [str(v) for v in html_candidates_raw if isinstance(v, str) and v.strip()]
    if html_candidates:
        html_best = max(html_candidates, key=len)
        extracted = _html_to_text(html_best)
        if extracted:
            return extracted

    # Some inbound providers place MIME parts in nested arrays.
    part_text_candidates: list[str] = []
    for container in [data.get("parts"), data.get("content_parts"), data.get("attachments"), payload.get("parts")]:
        if not isinstance(container, list):
            continue
        for part in container:
            if not isinstance(part, dict):
                continue
            content_type = str(part.get("content_type") or part.get("type") or "").lower()
            content = part.get("content") or part.get("text") or part.get("body")
            if not isinstance(content, str) or not content.strip():
                continue
            if "html" in content_type:
                txt = _html_to_text(content)
                if txt:
                    part_text_candidates.append(txt)
            else:
                part_text_candidates.append(content.strip())
    if part_text_candidates:
        return max(part_text_candidates, key=len)

    # Last-resort: sometimes the raw MIME or raw content is included.
    raw_candidates = [data.get("raw"), data.get("raw_email"), payload.get("raw")]
    for raw in raw_candidates:
        if not isinstance(raw, str) or not raw.strip():
            continue
        lowered = raw.lower()
        marker = "content-type: text/plain"
        idx = lowered.find(marker)
        if idx >= 0:
            body = raw[idx:]
            split = re.split(r"\r?\n\r?\n", body, maxsplit=1)
            if len(split) == 2 and split[1].strip():
                return split[1].strip()
        if raw.strip():
            return raw.strip()
    return ""


def _retrieve_resend_received_email(email_id: str) -> dict:
    if not email_id or not settings.resend_api_key:
        return {}
    try:
        with httpx.Client(timeout=10) as client:
            response = client.get(
                f"https://api.resend.com/emails/receiving/{email_id}",
                headers={
                    "Authorization": f"Bearer {settings.resend_api_key}",
                    "Content-Type": "application/json",
                },
            )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return {}
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}


def _build_inbound_email_from_resend(payload: dict, svix_id: Optional[str] = None) -> InboundEmailRequest:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        raise ValueError("invalid payload")

    event_type = payload.get("type") or payload.get("event")
    if event_type and event_type not in {"email.received", "inbound.email.received", "email_received"}:
        raise ValueError("unsupported event type")

    provider_event_id = str(payload.get("id") or payload.get("event_id") or data.get("id") or svix_id or "")
    provider_message_id = str(
        data.get("id")
        or data.get("message_id")
        or data.get("messageId")
        or data.get("email_id")
        or payload.get("id")
        or svix_id
        or ""
    )
    sender = _extract_email_address(str(data.get("from") or data.get("from_email") or ""))
    recipient_alias = _extract_email_address(
        _first_recipient(
            data.get("to")
            or data.get("to_email")
            or data.get("recipients")
            or data.get("delivered_to")
            or payload.get("to")
            or payload.get("recipient")
        )
    )
    subject = str(data.get("subject") or "")
    body_text = _extract_best_body_text(data, payload)
    if not body_text:
        email_id = str(data.get("email_id") or "")
        fetched = _retrieve_resend_received_email(email_id)
        if fetched:
            body_text = _extract_best_body_text(fetched, {"data": fetched})

    # Some Resend inbound events omit explicit recipient in payload; this alias is deterministic for this deployment.
    if not recipient_alias:
        recipient_alias = "schedule@lovelychaos.ca"

    if not provider_event_id or not provider_message_id or not sender:
        raise ValueError("missing required fields")

    return InboundEmailRequest(
        provider="resend",
        provider_event_id=provider_event_id,
        provider_message_id=provider_message_id,
        sender=sender,
        recipient_alias=recipient_alias,
        subject=subject,
        body_text=body_text,
    )


@app.post("/webhooks/resend/inbound", response_model=InboundResponse)
async def inbound_resend_webhook(request: Request, db: Session = Depends(get_db)):
    raw_body = await request.body()
    svix_id = request.headers.get("svix-id")
    if settings.resend_webhook_secret:
        svix_headers = {
            "svix-id": request.headers.get("svix-id"),
            "svix-timestamp": request.headers.get("svix-timestamp"),
            "svix-signature": request.headers.get("svix-signature"),
        }
        has_svix = all(svix_headers.values())
        if has_svix:
            try:
                Webhook(settings.resend_webhook_secret).verify(raw_body, svix_headers)
            except WebhookVerificationError as exc:
                raise HTTPException(status_code=401, detail="Invalid Resend signature") from exc
        else:
            # Local/dev fallback where caller may send a shared secret header.
            x_resend_signature = request.headers.get("x-resend-signature")
            if x_resend_signature != settings.resend_webhook_secret:
                raise HTTPException(status_code=401, detail="Invalid Resend signature")
    try:
        payload = json.loads(raw_body.decode() or "{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid Resend payload") from exc
    try:
        inbound = _build_inbound_email_from_resend(payload, svix_id=svix_id)
    except ValueError as exc:
        if str(exc) == "unsupported event type":
            return InboundResponse(
                status=WebhookStatus.INGESTION_ACCEPTED.value,
                message="Ignored unsupported Resend event type",
                request_id=str(uuid.uuid4()),
                mutation_executed=False,
                processing_state=ProcessingState.COMPLETED.value,
            )
        raise HTTPException(status_code=400, detail="Invalid Resend payload") from exc
    return inbound_email(payload=inbound, x_signature=settings.webhook_secret, db=db)


@app.post("/webhooks/sms/inbound", response_model=InboundResponse)
def inbound_sms(
    payload: InboundSMSRequest,
    x_signature: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
):
    request_id = str(uuid.uuid4())
    if x_signature != settings.webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid signature")

    existing_receipt = db.scalar(
        select(WebhookReceipt).where(
            WebhookReceipt.provider == payload.provider,
            WebhookReceipt.provider_event_id == payload.provider_event_id,
        )
    )
    if existing_receipt:
        return InboundResponse(
            status=WebhookStatus.COMMAND_COMPLETED.value,
            message="Duplicate webhook already processed",
            request_id=request_id,
            mutation_executed=False,
            processing_state=ProcessingState.COMPLETED.value,
        )

    receipt = WebhookReceipt(
        provider=payload.provider,
        provider_event_id=payload.provider_event_id,
        provider_message_id=payload.provider_message_id,
        status="received",
    )
    db.add(receipt)

    # Spouse replies are receive-only and may not mutate state.
    spouse_household = db.scalar(select(Household).where(Household.spouse_phone == payload.sender_phone))
    if spouse_household:
        db.commit()
        return _safe_error(WebhookStatus.REJECTED_UNAUTHORIZED, request_id, "Only the admin can run SMS commands.")

    attribution = resolve_admin_phone(db, payload.sender_phone)
    if attribution.kind == "unverified":
        db.commit()
        return _safe_error(
            WebhookStatus.REJECTED_UNVERIFIED,
            request_id,
            "We couldn't verify which LovelyChaos account this phone belongs to.",
        )
    if attribution.kind == "ambiguous":
        db.commit()
        return _safe_error(
            WebhookStatus.REJECTED_AMBIGUOUS,
            request_id,
            "We couldn't verify which LovelyChaos account this phone belongs to.",
        )

    user = attribution.user
    assert user is not None
    source = SourceMessage(
        provider=payload.provider,
        provider_message_id=payload.provider_message_id,
        source_channel="sms",
        sender=payload.sender_phone,
        household_id=user.household_id,
        subject="",
        body_text=payload.body_text,
    )
    db.add(source)
    db.flush()

    try:
        command = engine_llm.parse_command(payload.body_text)
    except Exception:
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel="sms",
            template="command_clarification",
            subject="LovelyChaos SMS",
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="Unable to parse SMS command.",
            request_id=request_id,
            mutation_executed=False,
        )
    if command["action"] not in {"delete", "add", "more_info", "remind", "set_preference"}:
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel="sms",
            template="command_clarification",
            subject="LovelyChaos SMS",
            status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
            message="Unsupported SMS command.",
            request_id=request_id,
            mutation_executed=False,
        )

    if command["action"] == "more_info":
        context = load_active_followup_context(db, household_id=user.household_id, response_channel="sms")
        _mark_receipt_processed(receipt)
        db.commit()
        more_info = _handle_more_info_command(
            request_id=request_id,
            topic=command.get("topic"),
            context=context,
        )
        return _command_reply(
            db=db,
            user=user,
            channel="sms",
            template="more_info",
            subject="LovelyChaos SMS",
            status=WebhookStatus(more_info.status),
            message=more_info.message,
            request_id=request_id,
            mutation_executed=False,
        )

    if command["action"] == "set_preference":
        return _handle_set_preference_command(
            db=db,
            request_id=request_id,
            user=user,
            receipt=receipt,
            response_channel="sms",
            response_subject="LovelyChaos SMS",
            topic=command.get("topic"),
            preference_behavior=command.get("preference_behavior"),
        )

    if command["action"] == "delete":
        event_id = command["pending_id"]
        if event_id is None:
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel="sms",
                template="delete_clarification",
                subject="LovelyChaos SMS",
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="Please include event id to delete.",
                request_id=request_id,
                mutation_executed=False,
            )
        event = db.scalar(select(Event).where(Event.id == event_id))
        if not event:
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel="sms",
                template="delete_clarification",
                subject="LovelyChaos SMS",
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="Event not found.",
                request_id=request_id,
                mutation_executed=False,
            )
        gate = _tenant_gate_for_calendar_mutation(db, request_id, user.household_id, event.household_id)
        if gate:
            return gate
        if not event.calendar_event_id:
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel="sms",
                template="delete_clarification",
                subject="LovelyChaos SMS",
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="Event is not linked to Google Calendar.",
                request_id=request_id,
                mutation_executed=False,
            )
        credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
        try:
            calendar_provider.delete_event(
                access_token=credential.access_token,
                calendar_id=binding.calendar_id,
                calendar_event_id=event.calendar_event_id,
            )
        except CalendarMutationError:
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel="sms",
                template="delete_clarification",
                subject="LovelyChaos SMS",
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="Unable to delete event from calendar. Please try again.",
                request_id=request_id,
                mutation_executed=False,
            )
        event.status = "deleted"
        event.version += 1
        _mark_receipt_processed(receipt)
        db.commit()
        return _command_reply(
            db=db,
            user=user,
            channel="sms",
            template="event_deleted",
            subject="LovelyChaos SMS",
            status=WebhookStatus.COMMAND_COMPLETED,
            message="Event deleted",
            request_id=request_id,
            mutation_executed=True,
        )

    if command["action"] == "remind":
        return _handle_reminder_command(
            db=db,
            request_id=request_id,
            command=command,
            user=user,
            provider_message_id=payload.provider_message_id,
            receipt=receipt,
            response_channel="sms",
            response_subject="LovelyChaos SMS",
        )

    followup_context = load_active_followup_context(db, household_id=user.household_id, response_channel="sms")
    return _handle_add_command(
        db=db,
        request_id=request_id,
        user=user,
        source=source,
        receipt=receipt,
        provider_message_id=payload.provider_message_id,
        subject="",
        raw_body_text=payload.body_text,
        analysis_body_text=payload.body_text,
        response_channel="sms",
        response_subject="LovelyChaos SMS",
        followup_context=followup_context,
        command_topic=command.get("topic"),
    )


@app.post("/webhooks/twilio/sms", response_model=InboundResponse)
async def inbound_twilio_sms(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    form_data = {key: str(form.get(key) or "") for key in form.keys()}

    if settings.twilio_auth_token:
        signature = request.headers.get("x-twilio-signature")
        if not signature:
            raise HTTPException(status_code=401, detail="Missing Twilio signature")
        validator = RequestValidator(settings.twilio_auth_token)
        if not validator.validate(_request_external_url(request), form_data, signature):
            raise HTTPException(status_code=401, detail="Invalid Twilio signature")

    try:
        inbound = _build_inbound_sms_from_twilio(form_data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid Twilio payload") from exc
    return inbound_sms(payload=inbound, x_signature=settings.webhook_secret, db=db)


@app.get("/operations/{operation_id}", response_model=OperationResponse)
def get_operation(operation_id: str, db: Session = Depends(get_db)):
    op = db.scalar(select(Operation).where(Operation.operation_id == operation_id))
    if not op:
        raise HTTPException(status_code=404, detail="Operation not found")
    return OperationResponse(
        operation_id=op.operation_id,
        status=op.status,
        processing_state=op.processing_state,
        last_updated_at=op.last_updated_at,
        mutation_executed=op.mutation_executed,
        user_message=op.user_message,
    )


@app.post("/internal/operations/{operation_id}/run", response_model=OperationResponse)
def run_operation(
    operation_id: str,
    _: None = Depends(_require_admin_key),
    db: Session = Depends(get_db),
):
    op = process_operation(db, operation_id, notifier)
    if not op:
        raise HTTPException(status_code=404, detail="Operation not found")
    db.commit()
    return OperationResponse(
        operation_id=op.operation_id,
        status=op.status,
        processing_state=op.processing_state,
        last_updated_at=op.last_updated_at,
        mutation_executed=op.mutation_executed,
        user_message=op.user_message,
    )


@app.post("/internal/jobs/expire")
def run_expiry(_: None = Depends(_require_admin_key), db: Session = Depends(get_db)):
    count = expire_pending_events(db)
    db.commit()
    return {"expired": count}


@app.post("/internal/jobs/retention")
def run_retention(
    retention_days: int = 30,
    _: None = Depends(_require_admin_key),
    db: Session = Depends(get_db),
):
    result = purge_old_records(db, retention_days)
    db.commit()
    return result


def _record_digest_items(db: Session, household_id: int, items: list[dict], status: str = "sent") -> int:
    scheduled_for = datetime.now(timezone.utc)
    if not items:
        db.add(
            DigestItem(
                household_id=household_id,
                item_type="meta",
                source_ref="none",
                priority=0,
                scheduled_for=scheduled_for,
                status=status,
            )
        )
        return 1
    for item in items:
        db.add(
            DigestItem(
                household_id=household_id,
                item_type=item["item_type"],
                source_ref=item["source_ref"],
                priority=item.get("priority", 0),
                scheduled_for=scheduled_for,
                status=status,
            )
        )
    return len(items)


@app.post("/internal/jobs/daily-summary")
def run_daily_summary(
    household_id: Optional[int] = None,
    _: None = Depends(_require_admin_key),
    db: Session = Depends(get_db),
):
    households = (
        [db.scalar(select(Household).where(Household.id == household_id))]
        if household_id is not None
        else db.scalars(select(Household)).all()
    )
    sent = 0
    skipped = 0
    digest_items_created = 0
    for household in households:
        if not household:
            continue
        if not household.daily_summary_enabled:
            skipped += 1
            continue
        summary = build_daily_summary(db, household.id)
        dispatch_household_notification(
            db=db,
            provider=notification_provider,
            household_id=household.id,
            template="daily_summary",
            subject=summary["subject"],
            message=summary["message"],
        )
        digest_items_created += _record_digest_items(db, household.id, summary["items"], status="sent")
        sent += 1
    db.commit()
    return {"sent_households": sent, "skipped_households": skipped, "digest_items_created": digest_items_created}


@app.post("/internal/jobs/weekly-digest")
def run_weekly_digest(
    household_id: Optional[int] = None,
    _: None = Depends(_require_admin_key),
    db: Session = Depends(get_db),
):
    households = (
        [db.scalar(select(Household).where(Household.id == household_id))]
        if household_id is not None
        else db.scalars(select(Household)).all()
    )
    sent = 0
    skipped = 0
    digest_items_created = 0
    for household in households:
        if not household:
            continue
        if not household.weekly_digest_enabled:
            skipped += 1
            continue
        digest = build_weekly_digest(db, household.id)
        dispatch_household_notification(
            db=db,
            provider=notification_provider,
            household_id=household.id,
            template="weekly_digest",
            subject=digest["subject"],
            message=digest["message"],
        )
        digest_items_created += _record_digest_items(db, household.id, digest["items"], status="sent")
        sent += 1
    db.commit()
    return {"sent_households": sent, "skipped_households": skipped, "digest_items_created": digest_items_created}


@app.post("/internal/google/refresh")
def run_google_token_refresh(
    household_id: int = 1,
    _: None = Depends(_require_admin_key),
    db: Session = Depends(get_db),
):
    credential = db.scalar(
        select(GoogleCredential).where(
            GoogleCredential.household_id == household_id,
            GoogleCredential.status == "active",
        )
    )
    if not credential:
        raise HTTPException(status_code=404, detail="Google credential not found")
    if not credential.refresh_token:
        raise HTTPException(status_code=409, detail="Refresh token missing")
    try:
        access_token, expiry = refresh_google_access_token(
            refresh_token=credential.refresh_token,
            client_id=settings.google_client_id,
            client_secret=settings.google_client_secret,
            timeout_sec=settings.google_calendar_timeout_sec,
        )
    except GoogleAuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    credential.access_token = access_token
    credential.token_expiry = expiry
    db.commit()
    return {"status": "refreshed", "household_id": household_id, "token_expiry": expiry}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    return {
        "service": "LovelyChaos",
        "status": "ok",
        "links": {
            "health": "/health",
            "ready": "/ready",
            "admin": "/admin",
            "onboarding": "/onboarding",
            "oauth_start": "/auth/google/start?household_id=1",
        },
    }


@app.get("/ready")
def readiness(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db.execute(text("SELECT version_num FROM alembic_version LIMIT 1"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"not ready: {exc.__class__.__name__}") from exc
    return {"status": "ready"}


@app.get("/auth/google/start")
def auth_google_start(household_id: int = 1):
    if not settings.google_client_id:
        raise HTTPException(status_code=500, detail="GOOGLE_CLIENT_ID is not configured")
    state = _build_oauth_state(household_id=household_id)
    query = urlencode(
        {
            "client_id": settings.google_client_id,
            "redirect_uri": settings.google_oauth_redirect_uri,
            "response_type": "code",
            "scope": "openid email https://www.googleapis.com/auth/calendar",
            "access_type": "offline",
            "include_granted_scopes": "true",
            "prompt": "consent",
            "state": state,
        }
    )
    return RedirectResponse(f"https://accounts.google.com/o/oauth2/v2/auth?{query}")


@app.get("/oauth/google/callback")
def auth_google_callback(code: str, state: str, db: Session = Depends(get_db)):
    if not settings.google_client_id or not settings.google_client_secret:
        raise HTTPException(status_code=500, detail="Google OAuth credentials are not configured")
    try:
        payload = _verify_oauth_state(state)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid OAuth state")

    household_id = int(payload["household_id"])

    with httpx.Client(timeout=settings.google_calendar_timeout_sec) as client:
        token_response = client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "redirect_uri": settings.google_oauth_redirect_uri,
                "grant_type": "authorization_code",
            },
        )
        if token_response.status_code >= 400:
            raise HTTPException(status_code=400, detail="Google token exchange failed")
        token_payload = token_response.json()
        access_token = token_payload["access_token"]
        refresh_token = token_payload.get("refresh_token")
        expires_in = token_payload.get("expires_in")
        token_expiry = None
        if isinstance(expires_in, int):
            token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        userinfo_response = client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if userinfo_response.status_code >= 400:
            raise HTTPException(status_code=400, detail="Google userinfo fetch failed")
        userinfo = userinfo_response.json()
        email = userinfo.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="Google userinfo missing email")

    credential = db.scalar(select(GoogleCredential).where(GoogleCredential.household_id == household_id))
    if not credential:
        credential = GoogleCredential(
            household_id=household_id,
            provider_user_email=email,
            token_subject=email,
            access_token=access_token,
            refresh_token=refresh_token,
            token_expiry=token_expiry,
            status="active",
        )
        db.add(credential)
        db.flush()
    else:
        credential.provider_user_email = email
        credential.token_subject = email
        credential.access_token = access_token
        if refresh_token:
            credential.refresh_token = refresh_token
        credential.token_expiry = token_expiry
        credential.status = "active"

    binding = db.scalar(select(CalendarBinding).where(CalendarBinding.household_id == household_id))
    if not binding:
        binding = CalendarBinding(
            household_id=household_id,
            google_credential_id=credential.id,
            calendar_id="primary",
            calendar_owner_email=email,
            status="active",
        )
        db.add(binding)
    binding.google_credential_id = credential.id
    binding.calendar_id = binding.calendar_id or "primary"
    binding.calendar_owner_email = email
    binding.status = "active"

    db.commit()
    return {
        "status": "connected",
        "household_id": household_id,
        "provider_user_email": email,
        "calendar_id": binding.calendar_id,
    }


@app.get("/admin/profile", response_model=HouseholdProfileOut)
def admin_get_profile(db: Session = Depends(get_db)):
    household = db.scalar(select(Household).where(Household.id == 1))
    admins = db.scalars(
        select(User).where(User.household_id == 1, User.is_admin.is_(True)).order_by(User.id.asc())
    ).all()
    assert household is not None
    assert admins
    admin = admins[0]
    secondary_admin = admins[1] if len(admins) > 1 else None
    return HouseholdProfileOut(
        household_id=household.id,
        admin_email=admin.email,
        secondary_admin_email=secondary_admin.email if secondary_admin else "",
        admin_phone=admin.phone or "",
        timezone=household.timezone,
        spouse_phone=household.spouse_phone or "",
        spouse_notifications_enabled=household.spouse_notifications_enabled,
    )


@app.put("/admin/profile")
def admin_put_profile(payload: HouseholdProfileIn, db: Session = Depends(get_db)):
    household = db.scalar(select(Household).where(Household.id == 1))
    admins = db.scalars(
        select(User).where(User.household_id == 1, User.is_admin.is_(True)).order_by(User.id.asc())
    ).all()
    assert household is not None
    assert admins
    admin = admins[0]
    secondary_admin = admins[1] if len(admins) > 1 else None

    primary_email = payload.admin_email.strip().lower()
    secondary_email = payload.secondary_admin_email.strip().lower()
    if secondary_email and secondary_email == primary_email:
        raise HTTPException(status_code=409, detail="Secondary admin email must be different from primary admin email")

    admin.email = payload.admin_email.strip().lower()
    admin.phone = payload.admin_phone.strip() or None

    if secondary_email:
        if secondary_admin is None:
            secondary_admin = User(household_id=1, is_admin=True, verified=True)
            db.add(secondary_admin)
        secondary_admin.email = secondary_email
        secondary_admin.phone = secondary_admin.phone or None
        secondary_admin.verified = True
    elif secondary_admin is not None:
        db.delete(secondary_admin)

    _set_household_timezone(db, household.id, payload.timezone.strip() or household.timezone or "UTC")
    household.spouse_phone = payload.spouse_phone.strip() or None
    household.spouse_notifications_enabled = payload.spouse_notifications_enabled
    db.commit()
    return {"ok": True}


@app.get("/admin/settings")
def admin_get_settings(db: Session = Depends(get_db)):
    household = db.scalar(select(Household).where(Household.id == 1))
    assert household is not None
    return {
        "daily_summary_enabled": household.daily_summary_enabled,
        "weekly_digest_enabled": household.weekly_digest_enabled,
    }


@app.put("/admin/settings")
def admin_put_settings(payload: SettingsIn, db: Session = Depends(get_db)):
    household = db.scalar(select(Household).where(Household.id == 1))
    assert household is not None
    household.daily_summary_enabled = payload.daily_summary_enabled
    household.weekly_digest_enabled = payload.weekly_digest_enabled
    db.commit()
    return {"ok": True}


@app.get("/admin/children")
def admin_get_children(db: Session = Depends(get_db)):
    children = db.scalars(select(Child).where(Child.household_id == 1)).all()
    return [{"id": c.id, "name": c.name, "school_name": c.school_name, "grade": c.grade} for c in children]


@app.get("/admin/schools/search")
def admin_search_schools(q: str = "", limit: int = 8):
    safe_limit = max(1, min(limit, 20))
    return {"results": search_gta_schools(q, limit=safe_limit)}


@app.get("/admin/schools/resolve")
def admin_resolve_school(name: str):
    resolution = resolve_school_timezone(name)
    if resolution is None:
        raise HTTPException(status_code=404, detail="School not found")
    return resolution.as_dict()


@app.post("/admin/children")
def admin_create_child(payload: ChildIn, db: Session = Depends(get_db)):
    resolution = _resolve_and_apply_school_timezone(db, 1, payload.school_name.strip())
    stored_school_name = (
        resolution["school_name"]
        if resolution and resolution.get("matched_from_directory")
        else payload.school_name.strip()
    )
    child = Child(household_id=1, name=payload.name, school_name=stored_school_name, grade=payload.grade)
    db.add(child)
    db.commit()
    db.refresh(child)
    response = {"id": child.id, "name": child.name, "school_name": child.school_name, "grade": child.grade}
    if resolution:
        response.update(
            {
                "resolved_timezone": resolution["timezone"],
                "school_city": resolution.get("city", ""),
                "school_source": resolution.get("source", ""),
            }
        )
    return response


@app.get("/admin/preferences")
def admin_get_preferences(db: Session = Depends(get_db)):
    return load_priority_preferences(db, 1)


@app.put("/admin/preferences")
def admin_put_preferences(payload: PreferenceIn, db: Session = Depends(get_db)):
    profile = save_priority_preferences(
        db,
        1,
        raw_text=payload.raw_text,
        system_defaults=payload.system_defaults,
        user_priority_topics=payload.user_priority_topics,
    )
    db.commit()
    return {"ok": True, "version": profile.version}


@app.get("/admin/calendar-binding", response_model=CalendarBindingOut)
def admin_get_calendar_binding(db: Session = Depends(get_db)):
    credential, binding = _resolve_calendar_context(db, 1)
    return CalendarBindingOut(
        provider_user_email=credential.provider_user_email,
        token_subject=credential.token_subject,
        calendar_id=binding.calendar_id,
        calendar_owner_email=binding.calendar_owner_email,
        status=binding.status,
    )


@app.put("/admin/calendar-binding")
def admin_put_calendar_binding(payload: CalendarBindingIn, db: Session = Depends(get_db)):
    credential = db.scalar(select(GoogleCredential).where(GoogleCredential.household_id == 1))
    if not credential:
        credential = GoogleCredential(household_id=1, provider_user_email=payload.provider_user_email, token_subject=payload.token_subject)
        db.add(credential)
        db.flush()
    credential.provider_user_email = payload.provider_user_email
    credential.token_subject = payload.token_subject
    credential.access_token = payload.access_token
    credential.status = "active"

    binding = db.scalar(select(CalendarBinding).where(CalendarBinding.household_id == 1))
    if not binding:
        binding = CalendarBinding(
            household_id=1,
            google_credential_id=credential.id,
            calendar_id=payload.calendar_id,
            calendar_owner_email=payload.calendar_owner_email,
            status="active",
        )
        db.add(binding)
    binding.google_credential_id = credential.id
    binding.calendar_id = payload.calendar_id
    binding.calendar_owner_email = payload.calendar_owner_email
    binding.status = "active"
    db.commit()
    return {"ok": True}


@app.get("/admin/pending-events")
def admin_get_pending(db: Session = Depends(get_db)):
    raise HTTPException(status_code=410, detail="Pending review is no longer part of the admin experience")


@app.get("/admin/reminders", response_model=list[ReminderOut])
def admin_get_reminders(db: Session = Depends(get_db)):
    reminders = db.scalars(select(Reminder).where(Reminder.household_id == 1)).all()
    return [
        ReminderOut(
            id=r.id,
            event_id=r.event_id,
            channel=r.channel,
            trigger_at=r.trigger_at,
            status=r.status,
        )
        for r in reminders
    ]


@app.post("/admin/reminders")
def admin_create_reminder(payload: ReminderIn, db: Session = Depends(get_db)):
    event = db.scalar(select(Event).where(Event.id == payload.event_id, Event.household_id == 1))
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    trigger_at = _safe_utc(event.start_at) - timedelta(minutes=int(payload.minutes_before))
    if trigger_at <= datetime.now(timezone.utc):
        raise HTTPException(status_code=409, detail="Reminder time is in the past")
    if payload.channel == "calendar":
        if not event.calendar_event_id:
            raise HTTPException(status_code=409, detail="Event is not linked to Google Calendar")
        credential, binding = _resolve_calendar_context_with_refresh(db, 1)
        calendar_provider.set_event_reminder(
            access_token=credential.access_token,
            calendar_id=binding.calendar_id,
            calendar_event_id=event.calendar_event_id,
            minutes_before=int(payload.minutes_before),
        )
    reminder = Reminder(
        household_id=1,
        event_id=payload.event_id,
        channel=payload.channel,
        trigger_at=trigger_at,
        timezone=event.timezone,
        status="scheduled",
    )
    db.add(reminder)
    db.commit()
    return {"ok": True}


@app.get("/admin/notifications")
def admin_get_notifications(db: Session = Depends(get_db)):
    deliveries = db.scalars(
        select(NotificationDelivery)
        .where(NotificationDelivery.household_id == 1)
        .order_by(NotificationDelivery.id.desc())
    ).all()
    return [
        {
            "id": d.id,
            "recipient_type": d.recipient_type,
            "channel": d.channel,
            "target": d.target,
            "template": d.template,
            "status": d.status,
            "created_at": d.created_at,
        }
        for d in deliveries
    ]


@app.get("/admin/digests")
def admin_get_digests(db: Session = Depends(get_db)):
    items = db.scalars(
        select(DigestItem).where(DigestItem.household_id == 1).order_by(DigestItem.id.desc())
    ).all()
    return [
        {
            "id": i.id,
            "item_type": i.item_type,
            "source_ref": i.source_ref,
            "priority": i.priority,
            "scheduled_for": i.scheduled_for,
            "status": i.status,
            "created_at": i.created_at,
        }
        for i in items
    ]


@app.get("/admin/inbound-activity")
def admin_get_inbound_activity(limit: int = 25, db: Session = Depends(get_db)):
    safe_limit = max(1, min(limit, 200))

    receipts = db.scalars(
        select(WebhookReceipt).order_by(WebhookReceipt.id.desc()).limit(safe_limit)
    ).all()
    messages = db.scalars(
        select(SourceMessage).order_by(SourceMessage.id.desc()).limit(safe_limit)
    ).all()
    events = db.scalars(
        select(Event).where(Event.household_id == 1).order_by(Event.id.desc()).limit(safe_limit)
    ).all()
    informational_items = db.scalars(
        select(InformationalItem).where(InformationalItem.household_id == 1).order_by(InformationalItem.id.desc()).limit(safe_limit)
    ).all()
    decision_audits = db.scalars(
        select(DecisionAudit).where(DecisionAudit.household_id == 1).order_by(DecisionAudit.id.desc()).limit(safe_limit)
    ).all()

    return {
        "receipts": [
            {
                "id": r.id,
                "provider": r.provider,
                "provider_event_id": r.provider_event_id,
                "provider_message_id": r.provider_message_id,
                "status": r.status,
                "error_code": r.error_code,
                "received_at": r.received_at,
                "processed_at": r.processed_at,
            }
            for r in receipts
        ],
        "messages": [
            {
                "id": m.id,
                "provider": m.provider,
                "provider_message_id": m.provider_message_id,
                "source_channel": m.source_channel,
                "sender": m.sender,
                "subject": m.subject,
                "household_id": m.household_id,
                "received_at": m.received_at,
            }
            for m in messages
        ],
        "events": [
            {
                "id": e.id,
                "source_message_id": e.source_message_id,
                "title": e.title,
                "status": e.status,
                "calendar_event_id": e.calendar_event_id,
                "start_at": e.start_at,
                "updated_at": e.updated_at,
            }
            for e in events
        ],
        "informational_items": [
            {
                "id": item.id,
                "source_message_id": item.source_message_id,
                "title": item.title,
                "details": item.details,
                "priority": item.priority,
                "status": item.status,
                "created_at": item.created_at,
            }
            for item in informational_items
        ],
        "decision_audits": [
            {
                "id": audit.id,
                "request_id": audit.request_id,
                "created_at": audit.created_at,
                "policy_outcome": audit.policy_outcome,
                "committed_actions": audit.committed_actions,
            }
            for audit in decision_audits
        ],
    }


@app.post("/admin/pending-events/{pending_id}/confirm")
def admin_confirm_pending(
    pending_id: int,
    db: Session = Depends(get_db),
):
    raise HTTPException(status_code=410, detail="Pending review is no longer part of the admin experience")


@app.post("/admin/pending-events/{pending_id}/reject")
def admin_reject_pending(
    pending_id: int,
    db: Session = Depends(get_db),
):
    raise HTTPException(status_code=410, detail="Pending review is no longer part of the admin experience")


@app.get("/admin", response_class=HTMLResponse)
def admin_home(request: Request, db: Session = Depends(get_db)):
    settings_data = admin_get_settings(db)
    children = admin_get_children(db)
    prefs = admin_get_preferences(db)
    calendar = admin_get_calendar_binding(db)
    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "settings": settings_data,
            "children": children,
            "prefs": prefs,
            "calendar": calendar,
        },
    )


@app.get("/onboarding", response_class=HTMLResponse)
def onboarding_home(request: Request, db: Session = Depends(get_db)):
    profile = admin_get_profile(db)
    settings_data = admin_get_settings(db)
    children = admin_get_children(db)
    prefs = admin_get_preferences(db)
    return templates.TemplateResponse(
        request,
        "onboarding.html",
        {
            "profile": profile,
            "settings": settings_data,
            "children": children,
            "prefs": prefs,
        },
    )


@app.get("/admin/activity", response_class=HTMLResponse)
def admin_activity_page(request: Request, limit: int = 25, db: Session = Depends(get_db)):
    data = admin_get_inbound_activity(limit=limit, db=db)
    return templates.TemplateResponse(
        request,
        "inbound_activity.html",
        {
            "limit": limit,
            "receipts": data["receipts"],
            "messages": data["messages"],
            "events": data["events"],
            "informational_items": data["informational_items"],
            "decision_audits": data["decision_audits"],
        },
    )


@app.get("/design-language", response_class=HTMLResponse)
def design_language_page(request: Request):
    return templates.TemplateResponse(request, "design_language.html", {})


@app.post("/admin/settings/form")
def admin_settings_form(
    daily_summary_enabled: bool = Form(default=False),
    weekly_digest_enabled: bool = Form(default=False),
    db: Session = Depends(get_db),
):
    admin_put_settings(
        SettingsIn(
            daily_summary_enabled=daily_summary_enabled,
            weekly_digest_enabled=weekly_digest_enabled,
        ),
        db,
    )
    return {"ok": True}


@app.post("/admin/children/form")
def admin_child_form(name: str = Form(), school_name: str = Form(), grade: str = Form(default=""), db: Session = Depends(get_db)):
    return admin_create_child(ChildIn(name=name, school_name=school_name, grade=grade), db)


@app.post("/admin/preferences/form")
def admin_pref_form(
    raw_text: str = Form(default=""),
    user_priority_topics: str = Form(default=""),
    system_default_school_closures: bool = Form(default=False),
    system_default_grade_relevant: bool = Form(default=False),
    db: Session = Depends(get_db),
):
    topics = [item.strip() for item in user_priority_topics.split("\n") if item.strip()]
    return admin_put_preferences(
        PreferenceIn(
            raw_text=raw_text,
            system_defaults={
                "school_closures": system_default_school_closures,
                "grade_relevant": system_default_grade_relevant,
            },
            user_priority_topics=topics,
            structured_json={},
        ),
        db,
    )


@app.post("/admin/calendar-binding/form")
def admin_calendar_binding_form(
    provider_user_email: str = Form(),
    token_subject: str = Form(),
    access_token: str = Form(),
    calendar_id: str = Form(),
    calendar_owner_email: str = Form(),
    db: Session = Depends(get_db),
):
    return admin_put_calendar_binding(
        CalendarBindingIn(
            provider_user_email=provider_user_email,
            token_subject=token_subject,
            access_token=access_token,
            calendar_id=calendar_id,
            calendar_owner_email=calendar_owner_email,
        ),
        db,
    )


@app.post("/admin/pending-events/{pending_id}/confirm/form")
def admin_confirm_form(pending_id: int, db: Session = Depends(get_db)):
    raise HTTPException(status_code=410, detail="Pending review is no longer part of the admin experience")


@app.post("/admin/pending-events/{pending_id}/reject/form")
def admin_reject_form(pending_id: int, db: Session = Depends(get_db)):
    raise HTTPException(status_code=410, detail="Pending review is no longer part of the admin experience")


@app.post("/onboarding/profile/form")
def onboarding_profile_form(
    admin_email: str = Form(),
    secondary_admin_email: str = Form(default=""),
    admin_phone: str = Form(default=""),
    timezone_value: str = Form(default="UTC"),
    spouse_phone: str = Form(default=""),
    spouse_notifications_enabled: bool = Form(default=False),
    db: Session = Depends(get_db),
):
    return admin_put_profile(
        HouseholdProfileIn(
            admin_email=admin_email,
            secondary_admin_email=secondary_admin_email,
            admin_phone=admin_phone,
            timezone=timezone_value,
            spouse_phone=spouse_phone,
            spouse_notifications_enabled=spouse_notifications_enabled,
        ),
        db,
    )


@app.post("/onboarding/settings/form")
def onboarding_settings_form(
    daily_summary_enabled: bool = Form(default=False),
    weekly_digest_enabled: bool = Form(default=False),
    db: Session = Depends(get_db),
):
    return admin_put_settings(
        SettingsIn(
            daily_summary_enabled=daily_summary_enabled,
            weekly_digest_enabled=weekly_digest_enabled,
        ),
        db,
    )


@app.post("/onboarding/preferences/form")
def onboarding_preferences_form(
    raw_text: str = Form(default=""),
    user_priority_topics: str = Form(default=""),
    system_default_school_closures: bool = Form(default=False),
    system_default_grade_relevant: bool = Form(default=False),
    db: Session = Depends(get_db),
):
    topics = [item.strip() for item in user_priority_topics.split("\n") if item.strip()]
    return admin_put_preferences(
        PreferenceIn(
            raw_text=raw_text,
            system_defaults={
                "school_closures": system_default_school_closures,
                "grade_relevant": system_default_grade_relevant,
            },
            user_priority_topics=topics,
            structured_json={},
        ),
        db,
    )


@app.post("/onboarding/children/form")
def onboarding_child_form(name: str = Form(), school_name: str = Form(), grade: str = Form(default=""), db: Session = Depends(get_db)):
    return admin_create_child(ChildIn(name=name, school_name=school_name, grade=grade), db)
