from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager, contextmanager, nullcontext
from contextvars import ContextVar, copy_context
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import base64
from email.utils import parseaddr, parsedate_to_datetime
from html import unescape
import hashlib
import hmac
import json
import os
import re
from urllib.parse import urlencode
import uuid
from typing import Any, Iterator, Optional
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import httpx
from starlette.concurrency import run_in_threadpool
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
    TeacherContact,
    Reminder,
    SmsConversationState,
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
    CalendarNewIn,
    CalendarSelectIn,
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
from app.services.agent_threads import (
    build_email_reply_headers,
    email_session_id,
    extract_header_value,
    load_thread_documents,
    persist_thread_documents,
    queue_session_message,
    resolve_email_thread_key,
    sms_session_id,
)
from app.services.add_requests import extract_context_documents, resolve_add_request_from_context
from app.services.calendar import (
    CalendarMutationError,
    CalendarProvider,
    GoogleCalendarHttpProvider,
    MockCalendarProvider,
)
from app.services.brief_summary import build_brief_summary
from app.services.content_analysis import (
    AnalysisChunk,
    DownloadedAttachment,
    build_prioritized_chunks,
    build_analysis_text,
    dedupe_extracted_events,
    extract_candidate_links,
    maybe_extract_pdf_text,
    resolve_and_download_links,
)
from app.services.digests import build_daily_summary, build_weekly_digest
from app.services.followups import (
    FollowupMatch,
    assess_more_info_context,
    build_more_info_message,
    load_active_sms_conversation_state,
    load_active_followup_context,
    load_recent_followup_contexts,
    persist_followup_context,
    persist_sms_conversation_state,
    resolve_candidate_items,
    resolve_followup_candidates,
    resolve_followup_item,
    resolve_response_channel,
    resolve_sms_conversation_state,
    retrieve_more_info_source_snippets,
)
from app.services.google_auth import GoogleAuthError, refresh_google_access_token, should_refresh_token
from app.services.llm import CommandToolRuntime, ExtractedEvent, MockDecisionEngine, OpenAIDecisionEngine
from app.services.openai_tracing import configure_openai_tracing, current_trace_context, request_trace_context
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
    priority_topic_catalog,
    save_command_written_preference,
    save_priority_preferences,
    topic_matches_text,
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
configure_openai_tracing(api_key=settings.openai_api_key, enabled=settings.openai_tracing_enabled)
_CURRENT_CONVERSATION_SESSION_ID: ContextVar[str | None] = ContextVar(
    "lovelychaos_current_conversation_session_id",
    default=None,
)
_CURRENT_EMAIL_REPLY_HEADERS: ContextVar[dict[str, str] | None] = ContextVar(
    "lovelychaos_current_email_reply_headers",
    default=None,
)


def _agent_db_session_factory() -> Session:
    return Session(bind=engine)


engine_llm = (
    OpenAIDecisionEngine(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        reasoning_effort=settings.openai_reasoning_effort,
        timeout_sec=settings.openai_timeout_sec,
        base_url=settings.openai_base_url,
        store_responses=settings.openai_store_responses,
        db_session_factory=_agent_db_session_factory,
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
APP_BOOTED_AT = datetime.now(timezone.utc)

_TRACED_HTTP_WORKFLOWS = {
    "/webhooks/email/inbound": "lovelychaos.email_inbound",
    "/webhooks/resend/inbound": "lovelychaos.resend_inbound",
    "/webhooks/sms/inbound": "lovelychaos.sms_inbound",
    "/webhooks/twilio/sms": "lovelychaos.twilio_sms_inbound",
    "/internal/operations/{operation_id}/run": "lovelychaos.run_operation",
}


@app.middleware("http")
async def _openai_trace_requests(request: Request, call_next):
    if not getattr(settings, "openai_tracing_enabled", True):
        return await call_next(request)
    route = request.scope.get("route")
    route_path = route.path if route is not None else None
    workflow_name = _TRACED_HTTP_WORKFLOWS.get(route_path or request.url.path)
    if not workflow_name:
        return await call_next(request)
    metadata = {
        "method": request.method,
        "path": request.url.path,
        "query": request.url.query or None,
    }
    group_id = request.headers.get("x-svix-id") or request.headers.get("x-request-id") or None
    with request_trace_context(workflow_name=workflow_name, group_id=group_id, metadata=metadata):
        return await call_next(request)


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
    user_preface_text: str
    forwarded_body_text: str
    forwarded_subject: str
    forwarded_sender: str
    forwarded_date: str
    reason: str
    forwarded_boundary_found: bool

_PLAIN_EMAIL_COMMAND_MIN_CONFIDENCE = 0.7
_FORWARDED_COMMAND_MIN_CONFIDENCE = 0.75
_FORWARDED_CLARIFICATION_MIN_CONFIDENCE = 0.65


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
        if re.match(r"^on .+ wrote:\s*$", line, flags=re.IGNORECASE):
            break
        if line.startswith(">"):
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
        if re.match(r"^on .+ wrote:\s*$", line, flags=re.IGNORECASE):
            return idx
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


def _extract_forwarded_metadata(forwarded_body: str) -> tuple[str, str, str]:
    forwarded_subject = ""
    forwarded_sender = ""
    forwarded_date = ""
    for raw_line in (forwarded_body or "").replace("\r", "").splitlines():
        line = raw_line.strip()
        if not line:
            if forwarded_subject or forwarded_sender or forwarded_date:
                break
            continue
        if re.match(r"^-+\s*forwarded message\s*-+$", line, flags=re.IGNORECASE):
            continue
        if re.match(r"^begin forwarded message:", line, flags=re.IGNORECASE):
            continue
        subject_match = re.match(r"^subject:\s*(.+)$", line, flags=re.IGNORECASE)
        sender_match = re.match(r"^from:\s*(.+)$", line, flags=re.IGNORECASE)
        date_match = re.match(r"^date:\s*(.+)$", line, flags=re.IGNORECASE)
        if subject_match:
            forwarded_subject = subject_match.group(1).strip()
        elif sender_match:
            forwarded_sender = sender_match.group(1).strip()
        elif date_match:
            forwarded_date = date_match.group(1).strip()
    return forwarded_subject, forwarded_sender, forwarded_date


def _classify_email_intent(body_text: str) -> EmailIntentClassification:
    normalized = (body_text or "").replace("\r", "")
    lines = normalized.splitlines()
    boundary_idx = _find_forward_boundary(lines)
    forwarded_boundary_found = boundary_idx is not None

    if boundary_idx is None:
        preface_raw = normalized
        forwarded_body = ""
        forwarded_subject = ""
        forwarded_sender = ""
        forwarded_date = ""
    else:
        preface_raw = "\n".join(lines[:boundary_idx])
        forwarded_body = "\n".join(lines[boundary_idx:]).strip()
        forwarded_subject, forwarded_sender, forwarded_date = _extract_forwarded_metadata(forwarded_body)

    user_preface_text = _normalize_preface_text(preface_raw)
    compact_preface = _compact_text(user_preface_text)

    if not compact_preface:
        return EmailIntentClassification(
            mode="ingestion",
            user_preface_text="",
            forwarded_body_text=forwarded_body,
            forwarded_subject=forwarded_subject,
            forwarded_sender=forwarded_sender,
            forwarded_date=forwarded_date,
            reason="no_preface",
            forwarded_boundary_found=forwarded_boundary_found,
        )

    if not forwarded_boundary_found:
        return EmailIntentClassification(
            mode="command_candidate",
            user_preface_text=user_preface_text,
            forwarded_body_text="",
            forwarded_subject="",
            forwarded_sender="",
            forwarded_date="",
            reason="plain_body_command_candidate",
            forwarded_boundary_found=False,
        )

    return EmailIntentClassification(
        mode="forwarded_preface_candidate",
        user_preface_text=user_preface_text,
        forwarded_body_text=forwarded_body,
        forwarded_subject=forwarded_subject,
        forwarded_sender=forwarded_sender,
        forwarded_date=forwarded_date,
        reason="forwarded_preface_candidate",
        forwarded_boundary_found=True,
    )


def _email_intent_metadata(intent: EmailIntentClassification) -> dict:
    return {
        "mode": intent.mode,
        "reason": intent.reason,
        "forwarded_boundary_found": intent.forwarded_boundary_found,
        "preface_char_count": len(intent.user_preface_text),
        "forwarded_subject": intent.forwarded_subject,
        "forwarded_sender": intent.forwarded_sender,
        "forwarded_date": intent.forwarded_date,
    }


def _should_accept_plain_email_command(command: dict) -> bool:
    action = str(command.get("action") or "none").strip().lower()
    if action not in {"add", "more_info", "update", "delete", "remind", "set_preference"}:
        return False
    strategy = _command_execution_strategy(command)
    if not _command_strategy_matches_action(action, strategy):
        return False

    confidence = float(command.get("confidence") or 0.0)
    if confidence < _PLAIN_EMAIL_COMMAND_MIN_CONFIDENCE:
        return False

    topic = str(command.get("topic") or "").strip()
    preference_behavior = str(command.get("preference_behavior") or "").strip().lower()
    if action == "set_preference":
        return bool(topic) and preference_behavior in {"auto_add", "mention", "suppress"}
    if action == "more_info":
        return bool(topic)
    return True


def _should_accept_forwarded_command(intent: dict) -> bool:
    action = str(intent.get("action") or "none").strip().lower()
    if action not in {"add", "more_info", "update", "delete", "remind", "set_preference"}:
        return False
    strategy = _command_execution_strategy(intent)
    if not _command_strategy_matches_action(action, strategy):
        return False
    confidence = float(intent.get("confidence") or 0.0)
    if confidence < _FORWARDED_COMMAND_MIN_CONFIDENCE:
        return False
    topic = str(intent.get("topic") or "").strip()
    preference_behavior = str(intent.get("preference_behavior") or "").strip().lower()
    if action == "set_preference":
        return bool(topic) and preference_behavior in {"auto_add", "mention", "suppress"}
    return True


def _should_prefer_direct_preface_command(command: Optional[dict]) -> bool:
    if not command or not _should_accept_plain_email_command(command):
        return False
    action = str(command.get("action") or "none").strip().lower()
    if action in {"more_info", "set_preference"}:
        return True
    return bool(str(command.get("topic") or "").strip())


def _command_execution_strategy(command: dict) -> str:
    strategy = str(command.get("execution_strategy") or "").strip().lower()
    if strategy in {"deterministic", "semantic", "none"}:
        return strategy
    action = str(command.get("action") or "none").strip().lower()
    if action in {"add", "update", "delete", "remind", "set_preference"}:
        return "deterministic"
    if action == "more_info":
        return "semantic"
    return "none"


def _command_strategy_matches_action(action: str, strategy: str) -> bool:
    if action in {"add", "update", "delete", "remind", "set_preference"}:
        return strategy == "deterministic"
    if action == "more_info":
        return strategy == "semantic"
    return strategy == "none"


def _tool_status_to_webhook(status: str) -> WebhookStatus:
    mapping = {
        "command_completed": WebhookStatus.COMMAND_COMPLETED,
        "command_needs_clarification": WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
        "command_noop_past_event": WebhookStatus.COMMAND_NOOP_PAST_EVENT,
    }
    return mapping.get(status, WebhookStatus.COMMAND_NEEDS_CLARIFICATION)


def _parse_iso_or_none(value: str | None) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _matches_query_text(title: str, query: str) -> bool:
    normalized_title = _compact_text(title)
    normalized_query = _compact_text(query)
    if not normalized_query:
        return False
    if normalized_query in normalized_title:
        return True
    tokens = _command_topic_tokens(query)
    return bool(tokens) and all(token in normalized_title for token in tokens)


def _resolve_household_event_match(
    db: Session,
    *,
    household_id: int,
    event_id: int | None = None,
    query: str | None = None,
) -> tuple[Optional[Event], bool]:
    if event_id is not None:
        event = db.scalar(select(Event).where(Event.id == event_id, Event.household_id == household_id))
        return event, False

    normalized_query = str(query or "").strip()
    if not normalized_query:
        return None, False

    candidates = [
        event
        for event in list(
            db.scalars(
                select(Event)
                .where(Event.household_id == household_id)
                .order_by(Event.start_at.asc(), Event.id.asc())
            )
        )
        if event.status != "deleted" and _matches_query_text(event.title or "", normalized_query)
    ]
    if not candidates:
        return None, False
    if len(candidates) > 1:
        return None, True
    return candidates[0], False


def _serialize_tool_event(event: Event) -> dict[str, Any]:
    return {
        "event_id": event.id,
        "title": event.title,
        "start_at": _serialize_dt(event.start_at),
        "end_at": _serialize_dt(event.end_at),
        "all_day": bool(event.all_day),
        "status": event.status,
        "calendar_event_id": event.calendar_event_id,
        "source": "stored_event",
    }


def _serialize_followup_tool_item(item: dict, timezone_name: str) -> Optional[dict[str, Any]]:
    candidate = _candidate_from_followup_item(item, timezone_name)
    if candidate is None:
        return None
    return {
        "event_id": None,
        "title": candidate.title,
        "start_at": _serialize_dt(candidate.start_at),
        "end_at": _serialize_dt(candidate.end_at),
        "all_day": False,
        "status": "followup_context",
        "calendar_event_id": None,
        "source": "followup_context",
        "item_id": item.get("item_id"),
    }


def _build_command_tool_runtime(
    *,
    db: Session,
    request_id: str,
    user: User,
    source: SourceMessage,
    provider_message_id: str,
    subject: str,
    raw_message_text: str,
    analysis_message_text: str,
    forwarded_subject: str,
    forwarded_date: str,
    response_channel: str,
) -> CommandToolRuntime:
    def read_preferences() -> dict[str, Any]:
        prefs = load_priority_preferences(db, user.household_id)
        return {
            "ok": True,
            "status": "command_completed",
            "message": "Loaded preferences.",
            "raw_text": prefs.get("raw_text") or "",
            "user_priority_topics": list(prefs.get("user_priority_topics") or []),
            "suppressed_priority_topics": list(prefs.get("effective_suppressed_priority_topics") or []),
            "command_written_preferences": list(prefs.get("command_written_preferences") or []),
        }

    def update_preference(topic: str, behavior: str, reason: str) -> dict[str, Any]:
        if not topic or behavior not in {"auto_add", "mention", "suppress"}:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "Tell me which topic to change and whether I should auto-add it, mention it, or suppress it.",
                "mutation_executed": False,
            }
        save_command_written_preference(
            db,
            household_id=user.household_id,
            topic=topic,
            behavior=behavior,
        )
        db.commit()
        behavior_text = {
            "auto_add": "I'll always add that when I can.",
            "mention": "I'll keep mentioning that in future updates.",
            "suppress": "I won't include updates about that unless you change the preference.",
        }[behavior]
        return {
            "ok": True,
            "status": "command_completed",
            "message": f"Saved preference for {topic.strip()}. {behavior_text}",
            "mutation_executed": True,
            "reason": reason,
        }

    def search_calendar(query: str, from_iso: str | None, to_iso: str | None, limit: int) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        if query:
            active_context = load_active_followup_context(
                db,
                household_id=user.household_id,
                response_channel=response_channel,
                thread_or_conversation_key=_followup_context_key(source),
            )
            contexts = [active_context] if active_context is not None else []
            contexts.extend(load_recent_followup_contexts(db, household_id=user.household_id, limit=3))
            for context in contexts:
                if context is None:
                    continue
                for match in resolve_followup_candidates(context, query_text=query):
                    payload = _serialize_followup_tool_item(match.item, user.timezone)
                    if payload is None:
                        continue
                    dedupe_key = json.dumps([payload.get("title"), payload.get("start_at"), payload.get("source")])
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    items.append(payload)
        from_dt = _parse_iso_or_none(from_iso)
        to_dt = _parse_iso_or_none(to_iso)
        for event in list(
            db.scalars(
                select(Event)
                .where(Event.household_id == user.household_id)
                .order_by(Event.start_at.asc(), Event.id.asc())
            )
        ):
            if event.status == "deleted":
                continue
            if query and not _matches_query_text(event.title or "", query):
                continue
            if from_dt and event.end_at < from_dt:
                continue
            if to_dt and event.start_at > to_dt:
                continue
            payload = _serialize_tool_event(event)
            dedupe_key = json.dumps([payload.get("title"), payload.get("start_at"), payload.get("source")])
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            items.append(payload)
        try:
            credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
            remote_items = calendar_provider.find_events(
                access_token=credential.access_token,
                calendar_id=binding.calendar_id,
                query=query,
                time_min=from_dt,
                time_max=to_dt,
                max_results=limit,
            )
            for event in remote_items:
                payload = {
                    "event_id": None,
                    "title": event.title,
                    "start_at": _serialize_dt(event.start_at),
                    "end_at": _serialize_dt(event.end_at),
                    "all_day": bool(event.all_day),
                    "status": "google_calendar",
                    "calendar_event_id": event.calendar_event_id,
                    "source": "google_calendar",
                }
                dedupe_key = json.dumps([payload.get("title"), payload.get("start_at"), payload.get("source")])
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                items.append(payload)
        except Exception:
            pass
        return {
            "ok": bool(items),
            "status": "command_completed" if items else "command_needs_clarification",
            "message": "Matches found." if items else "I couldn't find a matching event.",
            "items": items[: max(1, min(limit, 10))],
        }

    def add_calendar_event_from_context(query: str | None, title: str | None, start_at_iso: str | None, end_at_iso: str | None, all_day: bool) -> dict[str, Any]:
        active_context = load_active_followup_context(
            db,
            household_id=user.household_id,
            response_channel=response_channel,
            thread_or_conversation_key=_followup_context_key(source),
        )
        explicit_candidate: Optional[ExtractedEvent] = None
        if title and start_at_iso and end_at_iso:
            explicit_candidate = ExtractedEvent(
                title=title.strip(),
                start_at=_parse_iso_or_none(start_at_iso),
                end_at=_parse_iso_or_none(end_at_iso),
                category="command",
                confidence=0.95,
                model_reason="tool_explicit_event",
            )

        priority_preferences = load_priority_preferences(db, user.household_id)
        reference_datetime_hint = _serialize_dt(_parse_forwarded_reference_datetime(forwarded_date, user.timezone)) or ""
        extraction_result = extract_context_documents(
            content_body_text=analysis_message_text or raw_message_text,
            reference_datetime_hint=reference_datetime_hint,
        )
        result = resolve_add_request_from_context(
            raw_body_text=query or raw_message_text,
            subject=subject,
            timezone_name=user.timezone,
            response_channel=response_channel,
            command_topic=query,
            followup_context=active_context,
            forwarded_subject=forwarded_subject,
            forwarded_date=forwarded_date,
            preference_text=priority_preferences["raw_text"],
            explicit_candidate=explicit_candidate,
            extraction_result=extraction_result,
            fallback_command_topic_fn=_fallback_command_topic,
            extract_direct_add_candidate_fn=_extract_direct_add_candidate,
            candidate_from_followup_item_fn=_candidate_from_followup_item,
            resolve_forwarded_add_candidates_fn=_resolve_forwarded_add_candidates,
            collect_extraction_results_fn=_collect_extraction_results,
            validate_candidate_fn=validate_candidate,
            serialize_dt_fn=_serialize_dt,
            build_candidate_clarification_fn=_build_candidate_clarification,
            build_past_event_message_fn=_build_past_event_message,
            past_only_candidates_fn=_past_only_candidates,
            allows_multiple_add_fn=_allows_multiple_add,
            create_candidate_event_fn=lambda candidate: _create_event_from_candidate_result(
                db=db,
                request_id=request_id,
                user=user,
                source=source,
                provider_message_id=provider_message_id,
                candidate=candidate,
                all_day_override=True if all_day else None,
            ),
        )
        return {
            "ok": result.status == "command_completed",
            "status": result.status,
            "message": result.message,
            "mutation_executed": result.mutation_executed,
            "candidate_choices": result.candidate_choices,
            "created_event_ids": result.created_event_ids,
            "created_titles": result.created_titles,
        }

    def update_calendar_event(
        event_id: int | None,
        query: str | None,
        title: str | None,
        location: str | None,
        start_at_iso: str | None,
        end_at_iso: str | None,
        all_day: bool | None,
    ) -> dict[str, Any]:
        event, ambiguous = _resolve_household_event_match(db, household_id=user.household_id, event_id=event_id, query=query)
        if ambiguous:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "I found more than one matching calendar event. Please be more specific.",
                "mutation_executed": False,
            }
        if event is None:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "I couldn't find that event to update.",
                "mutation_executed": False,
            }
        if not event.calendar_event_id:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "That event is not linked to Google Calendar.",
                "mutation_executed": False,
            }
        if not any([title, location, start_at_iso, end_at_iso, all_day is not None]):
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "Tell me what you want to change about that event.",
                "mutation_executed": False,
            }
        gate_response = _tenant_gate_for_calendar_mutation(db, request_id, user.household_id, event.household_id)
        if gate_response is not None:
            return {
                "ok": False,
                "status": gate_response.status,
                "message": gate_response.message,
                "mutation_executed": False,
            }
        start_at = _parse_iso_or_none(start_at_iso) if start_at_iso else None
        end_at = _parse_iso_or_none(end_at_iso) if end_at_iso else None
        try:
            credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
            calendar_provider.update_event(
                access_token=credential.access_token,
                calendar_id=binding.calendar_id,
                calendar_event_id=event.calendar_event_id,
                title=title,
                start_at=start_at or event.start_at,
                end_at=end_at or event.end_at,
                timezone=user.timezone,
                all_day=event.all_day if all_day is None else all_day,
                location=location,
            )
        except CalendarMutationError:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "I couldn't update that event in Google Calendar right now. Please try again.",
                "mutation_executed": False,
            }
        if title:
            event.title = title
        if start_at:
            event.start_at = start_at
        if end_at:
            event.end_at = end_at
        if all_day is not None:
            event.all_day = all_day
        event.version += 1
        db.commit()
        return {
            "ok": True,
            "status": "command_completed",
            "message": "Updated calendar event.",
            "mutation_executed": True,
            "event_id": event.id,
        }

    def delete_calendar_event(event_id: int | None, query: str | None) -> dict[str, Any]:
        event, ambiguous = _resolve_household_event_match(db, household_id=user.household_id, event_id=event_id, query=query)
        if ambiguous:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "I found more than one matching event. Please be more specific.",
                "mutation_executed": False,
            }
        if event is None:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "Event not found.",
                "mutation_executed": False,
            }
        if not event.calendar_event_id:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "Event is not linked to Google Calendar.",
                "mutation_executed": False,
            }
        gate_response = _tenant_gate_for_calendar_mutation(db, request_id, user.household_id, event.household_id)
        if gate_response is not None:
            return {
                "ok": False,
                "status": gate_response.status,
                "message": gate_response.message,
                "mutation_executed": False,
            }
        try:
            credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
            calendar_provider.delete_event(
                access_token=credential.access_token,
                calendar_id=binding.calendar_id,
                calendar_event_id=event.calendar_event_id,
            )
        except CalendarMutationError:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "Unable to delete event from calendar. Please try again.",
                "mutation_executed": False,
            }
        event.status = "deleted"
        event.version += 1
        db.commit()
        return {
            "ok": True,
            "status": "command_completed",
            "message": "Event deleted",
            "mutation_executed": True,
            "event_id": event.id,
        }

    def set_calendar_reminder(event_id: int | None, query: str | None, minutes_before: int, reminder_channel: str) -> dict[str, Any]:
        event, ambiguous = _resolve_household_event_match(db, household_id=user.household_id, event_id=event_id, query=query)
        if ambiguous:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "I found more than one matching event. Please be more specific.",
                "mutation_executed": False,
            }
        if event is None:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "Event not found.",
                "mutation_executed": False,
            }
        gate = _tenant_gate_for_calendar_mutation(db, request_id, user.household_id, event.household_id)
        if gate is not None:
            return {
                "ok": False,
                "status": gate.status,
                "message": gate.message,
                "mutation_executed": False,
            }
        trigger_at = _safe_utc(event.start_at) - timedelta(minutes=int(minutes_before or 60))
        now = datetime.now(timezone.utc)
        if trigger_at <= now:
            return {
                "ok": False,
                "status": "command_needs_clarification",
                "message": "Reminder time is in the past. Please choose a different reminder time.",
                "mutation_executed": False,
            }
        if reminder_channel == "calendar":
            if not event.calendar_event_id:
                return {
                    "ok": False,
                    "status": "command_needs_clarification",
                    "message": "Event is not linked to Google Calendar.",
                    "mutation_executed": False,
                }
            try:
                credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
                calendar_provider.set_event_reminder(
                    access_token=credential.access_token,
                    calendar_id=binding.calendar_id,
                    calendar_event_id=event.calendar_event_id,
                    minutes_before=int(minutes_before or 60),
                )
            except CalendarMutationError:
                return {
                    "ok": False,
                    "status": "command_needs_clarification",
                    "message": "Unable to set calendar reminder right now.",
                    "mutation_executed": False,
                }
        db.add(
            Reminder(
                household_id=user.household_id,
                event_id=event.id,
                channel=reminder_channel,
                trigger_at=trigger_at,
                timezone=user.timezone,
                status="scheduled",
            )
        )
        db.commit()
        return {
            "ok": True,
            "status": "command_completed",
            "message": "Reminder set.",
            "mutation_executed": True,
            "event_id": event.id,
        }

    return CommandToolRuntime(
        household_id=user.household_id,
        response_channel=response_channel,
        timezone_name=user.timezone,
        current_message=raw_message_text,
        read_preferences=read_preferences,
        update_preference=update_preference,
        search_calendar=search_calendar,
        add_calendar_event_from_context=add_calendar_event_from_context,
        update_calendar_event=update_calendar_event,
        delete_calendar_event=delete_calendar_event,
        set_calendar_reminder=set_calendar_reminder,
        notes={"request_id": request_id, "provider_message_id": provider_message_id},
    )


def _run_command_tools(
    *,
    db: Session,
    request_id: str,
    user: User,
    source: SourceMessage,
    receipt: WebhookReceipt,
    response_channel: str,
    response_subject: str,
    provider_message_id: str,
    subject: str,
    raw_message_text: str,
    analysis_message_text: str,
    forwarded_subject: str = "",
    forwarded_date: str = "",
    command: dict,
    allow_add_fallback: bool = False,
) -> Optional[InboundResponse]:
    execute = getattr(engine_llm, "execute_command_with_tools", None)
    if not callable(execute):
        return None
    runtime = _build_command_tool_runtime(
        db=db,
        request_id=request_id,
        user=user,
        source=source,
        provider_message_id=provider_message_id,
        subject=subject,
        raw_message_text=raw_message_text,
        analysis_message_text=analysis_message_text,
        forwarded_subject=forwarded_subject,
        forwarded_date=forwarded_date,
        response_channel=response_channel,
    )
    try:
        result = execute(
            {
                "message_text": raw_message_text,
                "parsed_command": command,
                "response_channel": response_channel,
                "session_key": _followup_context_key(source),
            },
            runtime,
        )
    except Exception:
        return None
    status = str(result.get("status") or "command_needs_clarification")
    action = str(command.get("action") or "")
    if status != "command_completed" and action == "more_info":
        return None
    _mark_receipt_processed(receipt)
    db.commit()
    return _command_reply(
        db=db,
        user=user,
        channel=response_channel,
        template="command_reply",
        subject=response_subject,
        status=_tool_status_to_webhook(status),
        message=str(result.get("message") or "I need a more specific command."),
        request_id=request_id,
        mutation_executed=bool(result.get("mutation_executed")),
    )


def _collect_extraction_results(
    chunks: list[AnalysisChunk],
    subject: str,
    household_preferences: str,
    timezone_hint: str,
    reference_datetime_hint: str = "",
    document_understanding: dict | None = None,
) -> tuple[list[ExtractedEvent], list[dict], list[str], list[dict]]:
    if not chunks:
        return [], [], [], []

    events: list[ExtractedEvent] = []
    notes: list[str] = []
    chunk_failures: list[dict] = []
    chunk_summaries: list[dict] = []
    ordered_results: list[dict] = []

    def _extract_chunk(chunk: AnalysisChunk) -> dict:
        try:
            try:
                result = engine_llm.extract_events(
                    chunk.text,
                    subject,
                    household_preferences=household_preferences,
                    timezone_hint=timezone_hint,
                    reference_datetime_hint=reference_datetime_hint,
                    document_understanding=document_understanding,
                )
            except TypeError as exc:
                if "document_understanding" not in str(exc):
                    raise
                result = engine_llm.extract_events(
                    chunk.text,
                    subject,
                    household_preferences=household_preferences,
                    timezone_hint=timezone_hint,
                    reference_datetime_hint=reference_datetime_hint,
                )
            chunk_events = list(result.get("events") or [])
            email_level_notes = result.get("email_level_notes")
            return {
                "chunk_index": chunk.index,
                "events": chunk_events,
                "note": str(email_level_notes) if email_level_notes else None,
                "summary": {
                    "chunk_index": chunk.index,
                    "label": chunk.label,
                    "char_count": len(chunk.text),
                    "priority_score": chunk.priority_score,
                    "section_labels": chunk.section_labels,
                    "event_count": len(chunk_events),
                },
                "failure": None,
            }
        except Exception as exc:
            return {
                "chunk_index": chunk.index,
                "events": [],
                "note": None,
                "summary": None,
                "failure": {
                    "chunk_index": chunk.index,
                    "label": chunk.label,
                    "char_count": len(chunk.text),
                    "priority_score": chunk.priority_score,
                    "section_labels": chunk.section_labels,
                    "detail": _llm_failure_detail(exc),
                },
            }

    max_workers = min(4, len(chunks))
    if max_workers <= 1:
        ordered_results = [_extract_chunk(chunk) for chunk in chunks]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(copy_context().run, _extract_chunk, chunk)
                for chunk in chunks
            ]
            for future in as_completed(futures):
                ordered_results.append(future.result())

    for item in sorted(ordered_results, key=lambda entry: entry["chunk_index"]):
        events.extend(item["events"])
        if item["note"]:
            notes.append(item["note"])
        if item["summary"]:
            chunk_summaries.append(item["summary"])
        if item["failure"]:
            chunk_failures.append(item["failure"])
    return dedupe_extracted_events(events), chunk_summaries, notes, chunk_failures


def _relevancy_payload_is_relevant(payload: dict) -> bool:
    return any(
        bool(payload.get(key))
        for key in ["name_match", "teacher_match", "school_match", "grade_match", "preference_match"]
    )


def _legacy_route_extracted_events(
    *,
    extracted_events: list[ExtractedEvent],
    children: list[Child],
    priority_preferences: dict,
    sender_email_hint: str,
    sender_name_hint: str,
) -> list[dict]:
    decisions: list[dict] = []
    for idx, candidate in enumerate(extracted_events, start=1):
        validation = validate_candidate(candidate)
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
            positive_preference_topics=list(priority_preferences["user_priority_topics"]),
            sender_email=sender_email_hint,
            sender_display_name=sender_name_hint,
            target_scope=(candidate.target_scope or "unknown").strip(),
        )
        matched_positive_topics = [
            topic
            for topic in list(priority_preferences["user_priority_topics"])
            if topic_matches_text(
                topic,
                candidate.title,
                candidate.category,
                candidate.model_reason,
                " ".join(candidate.target_grades or []),
                " ".join(candidate.mentioned_names or []),
                " ".join(candidate.mentioned_schools or []),
            )
        ]
        matched_suppressed_topics = [
            topic
            for topic in list(priority_preferences["effective_suppressed_priority_topics"])
            if topic_matches_text(
                topic,
                candidate.title,
                candidate.category,
                candidate.model_reason,
                " ".join(candidate.target_grades or []),
                " ".join(candidate.mentioned_names or []),
                " ".join(candidate.mentioned_schools or []),
            )
        ]
        suppressed_match = any(
            matched_suppressed_topics
        )
        is_relevant = relevancy.is_relevant
        target_scope = (candidate.target_scope or "unknown").strip()
        is_school_global = target_scope == "school_global"
        auto_add_decision = evaluate_auto_add_candidate(candidate, relevancy, children, suppressed_match=suppressed_match)

        if is_relevant and validation["valid"] and auto_add_decision.allow:
            execution_disposition = "create_event"
            final_reason = "relevant_and_actionable_auto_add"
        elif is_relevant:
            execution_disposition = "followup_available"
            final_reason = "relevant_for_followup"
        elif is_school_global:
            execution_disposition = "informational_item"
            final_reason = "not_relevant_school_global"
        else:
            execution_disposition = "ignore"
            final_reason = "not_relevant"

        decisions.append(
            {
                "index": idx,
                "validation": validation,
                "relevancy_evidence": relevancy.as_dict(),
                "suppressed_match": suppressed_match,
                "matched_positive_topics": matched_positive_topics,
                "matched_suppressed_topics": matched_suppressed_topics,
                "auto_add_decision": {"allow": auto_add_decision.allow, "reason": auto_add_decision.reason},
                "execution_disposition": execution_disposition,
                "final_reason": final_reason,
            }
        )
    return decisions


def _match_extracted_event_preferences(
    *,
    extracted_events: list[ExtractedEvent],
    priority_preferences: dict,
    document_understanding: dict | None = None,
) -> list[dict]:
    if not extracted_events:
        return []
    if not list(priority_preferences["user_priority_topics"]) and not list(priority_preferences["effective_suppressed_priority_topics"]):
        return []
    matcher = getattr(engine_llm, "match_event_preferences", None)
    if not callable(matcher):
        return []
    try:
        decisions = matcher(
            extracted_events=extracted_events,
            positive_preference_topics=list(priority_preferences["user_priority_topics"]),
            suppressed_priority_topics=list(priority_preferences["effective_suppressed_priority_topics"]),
            document_understanding=document_understanding,
        )
    except NotImplementedError:
        return []
    except Exception:
        return []
    if len(decisions) != len(extracted_events):
        return []
    return [dict(item or {}) for item in decisions]


def _overlay_preference_match_decision(base: dict, match: dict) -> dict:
    if not match:
        return dict(base or {})
    merged = dict(base or {})
    relevancy = dict(merged.get("relevancy_evidence") or {})
    if "preference_match" in match:
        relevancy["preference_match"] = bool(match.get("preference_match"))
    if "matched_positive_topics" in match:
        relevancy["matched_positive_topics"] = [
            str(item).strip() for item in list(match.get("matched_positive_topics") or []) if str(item).strip()
        ]
        merged["matched_positive_topics"] = list(relevancy["matched_positive_topics"])
    if "suppressed_match" in match:
        merged["suppressed_match"] = bool(match.get("suppressed_match"))
    if "matched_suppressed_topics" in match:
        merged["matched_suppressed_topics"] = [
            str(item).strip() for item in list(match.get("matched_suppressed_topics") or []) if str(item).strip()
        ]
    if relevancy:
        merged["relevancy_evidence"] = relevancy
    return merged


def _route_extracted_events(
    *,
    extracted_events: list[ExtractedEvent],
    children: list[Child],
    priority_preferences: dict,
    sender_email_hint: str,
    sender_name_hint: str,
    timezone_hint: str,
    evaluation_datetime_utc: str,
    document_understanding: dict | None = None,
) -> list[dict]:
    if not extracted_events:
        return []
    fallback_decisions = _legacy_route_extracted_events(
        extracted_events=extracted_events,
        children=children,
        priority_preferences=priority_preferences,
        sender_email_hint=sender_email_hint,
        sender_name_hint=sender_name_hint,
    )
    preference_match_decisions = _match_extracted_event_preferences(
        extracted_events=extracted_events,
        priority_preferences=priority_preferences,
        document_understanding=document_understanding,
    )
    route_events = getattr(engine_llm, "route_events", None)
    if not callable(route_events):
        if not preference_match_decisions:
            return fallback_decisions
        return [
            _apply_routing_guardrails(
                proposed=_overlay_preference_match_decision({}, match),
                fallback=dict(fallback or {}),
                candidate=candidate,
            )
            for candidate, fallback, match in zip(extracted_events, fallback_decisions, preference_match_decisions)
        ]
    try:
        decisions = route_events(
            extracted_events=extracted_events,
            children=children,
            positive_preference_topics=list(priority_preferences["user_priority_topics"]),
            suppressed_priority_topics=list(priority_preferences["effective_suppressed_priority_topics"]),
            sender_email=sender_email_hint,
            sender_display_name=sender_name_hint,
            timezone_hint=timezone_hint,
            evaluation_datetime_utc=evaluation_datetime_utc,
            document_understanding=document_understanding,
        )
    except NotImplementedError:
        decisions = []
    except Exception:
        decisions = []
    if decisions and len(decisions) != len(extracted_events):
        raise ValueError("Event routing decision count mismatch")
    if not decisions:
        if not preference_match_decisions:
            return fallback_decisions
        return [
            _apply_routing_guardrails(
                proposed=_overlay_preference_match_decision({}, match),
                fallback=dict(fallback or {}),
                candidate=candidate,
            )
            for candidate, fallback, match in zip(extracted_events, fallback_decisions, preference_match_decisions)
        ]
    return [
        _apply_routing_guardrails(
            proposed=_overlay_preference_match_decision(dict(proposed or {}), preference_match),
            fallback=dict(fallback or {}),
            candidate=candidate,
        )
        for candidate, proposed, fallback, preference_match in zip(
            extracted_events,
            decisions,
            fallback_decisions,
            preference_match_decisions or [{} for _ in extracted_events],
        )
    ]


def _apply_routing_guardrails(*, proposed: dict, fallback: dict, candidate: ExtractedEvent) -> dict:
    validation = dict(fallback.get("validation") or {})
    relevancy_fallback = dict(fallback.get("relevancy_evidence") or {})
    relevancy_proposed = dict(proposed.get("relevancy_evidence") or {})
    merged_relevancy = {}
    non_preference_bool_keys = {"name_match", "teacher_match", "school_match", "grade_match"}
    child_id_keys = {"name_child_ids", "teacher_child_ids", "school_child_ids", "grade_child_ids"}
    for key, value in relevancy_fallback.items():
        if isinstance(value, list):
            merged_relevancy[key] = list(value)
        else:
            merged_relevancy[key] = bool(value)
    for key, value in relevancy_proposed.items():
        if key in child_id_keys and isinstance(value, list):
            existing = [int(item) for item in list(merged_relevancy.get(key) or []) if str(item).isdigit()]
            merged_relevancy[key] = sorted({*existing, *[int(item) for item in value if str(item).isdigit()]})
        elif key in non_preference_bool_keys and isinstance(value, bool):
            merged_relevancy[key] = bool(merged_relevancy.get(key)) or value
        elif key == "matched_positive_topics" and isinstance(value, list):
            merged_relevancy[key] = [str(item).strip() for item in value if str(item).strip()]
        elif key == "preference_match" and isinstance(value, bool):
            merged_relevancy[key] = bool(value)

    auto_add_decision = dict(fallback.get("auto_add_decision") or {})
    suppressed_match = (
        bool(proposed.get("suppressed_match"))
        if "suppressed_match" in proposed
        else bool(fallback.get("suppressed_match"))
    )
    matched_positive_topics = list(merged_relevancy.get("matched_positive_topics") or [])
    if not matched_positive_topics:
        matched_positive_topics = [str(item).strip() for item in list(fallback.get("matched_positive_topics") or []) if str(item).strip()]
        if "matched_positive_topics" in proposed:
            matched_positive_topics = [str(item).strip() for item in list(proposed.get("matched_positive_topics") or []) if str(item).strip()]
    matched_suppressed_topics = [str(item).strip() for item in list(fallback.get("matched_suppressed_topics") or []) if str(item).strip()]
    if "matched_suppressed_topics" in proposed:
        matched_suppressed_topics = [str(item).strip() for item in list(proposed.get("matched_suppressed_topics") or []) if str(item).strip()]
    is_relevant = _relevancy_payload_is_relevant(merged_relevancy)
    is_school_global = (candidate.target_scope or "unknown").strip() == "school_global"

    allowed_dispositions: set[str]
    if is_relevant and bool(validation.get("valid")) and bool(auto_add_decision.get("allow")):
        allowed_dispositions = {"create_event", "followup_available"}
    elif is_relevant:
        allowed_dispositions = {"followup_available"}
    elif is_school_global:
        allowed_dispositions = {"informational_item", "ignore"}
    else:
        allowed_dispositions = {"ignore"}

    proposed_disposition = str(proposed.get("execution_disposition") or "").strip()
    if proposed_disposition not in allowed_dispositions:
        proposed_disposition = str(fallback.get("execution_disposition") or "").strip()
    if proposed_disposition not in allowed_dispositions:
        proposed_disposition = next(iter(sorted(allowed_dispositions)))

    final_reason = {
        "create_event": "relevant_and_actionable_auto_add",
        "followup_available": "relevant_for_followup",
        "informational_item": "not_relevant_school_global",
        "ignore": "not_relevant",
    }[proposed_disposition]
    return {
        "index": int(fallback.get("index") or proposed.get("index") or 0),
        "validation": validation,
        "relevancy_evidence": merged_relevancy,
        "suppressed_match": suppressed_match,
        "matched_positive_topics": list(dict.fromkeys(matched_positive_topics)),
        "matched_suppressed_topics": list(dict.fromkeys(matched_suppressed_topics)),
        "auto_add_decision": auto_add_decision,
        "execution_disposition": proposed_disposition,
        "final_reason": final_reason,
    }


def _llm_failure_detail(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code if exc.response is not None else None
        detail = exc.__class__.__name__
        if status_code is not None:
            detail = f"{detail}:{status_code}"
        if exc.response is not None:
            try:
                payload = exc.response.json()
            except ValueError:
                payload = {}
            error = payload.get("error") if isinstance(payload, dict) else {}
            if isinstance(error, dict):
                code = str(error.get("code") or error.get("type") or "").strip()
                if code:
                    detail = f"{detail}:{code}"
        return detail
    return exc.__class__.__name__


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
    if (
        end_local is not None
        and start_local.hour == 0
        and start_local.minute == 0
        and start_local.second == 0
        and (
            (
                end_local.date() == (start_local + timedelta(days=1)).date()
                and end_local.hour == 0
                and end_local.minute == 0
                and end_local.second == 0
            )
            or (
                end_local.date() == start_local.date()
                and end_local.hour == 23
                and end_local.minute == 59
                and end_local.second == 59
            )
        )
    ):
        return day
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
    default_admin_email = settings.local_test_admin_email or "admin@example.com"
    existing_household = db.scalar(select(Household).where(Household.id == 1))
    if existing_household:
        admin = db.scalar(select(User).where(User.household_id == 1, User.email == default_admin_email))
        if admin is None and settings.local_test_admin_email:
            admin = db.scalar(
                select(User).where(
                    User.household_id == 1,
                    User.is_admin.is_(True),
                    User.verified.is_(True),
                )
            )
            if admin is not None:
                admin.email = default_admin_email
        if admin and not admin.phone:
            admin.phone = "+15550000001"
        credential = db.scalar(select(GoogleCredential).where(GoogleCredential.household_id == 1))
        if not credential:
            credential = GoogleCredential(
                household_id=1,
                provider_user_email=default_admin_email,
                token_subject=default_admin_email,
                access_token="mock-access-token",
                status="active",
            )
            db.add(credential)
            db.flush()
        elif settings.local_test_admin_email:
            credential.provider_user_email = default_admin_email
            credential.token_subject = default_admin_email
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
        elif settings.local_test_admin_email:
            binding.calendar_owner_email = default_admin_email
        ensure_priority_rules(db, 1)
        return

    household = Household(id=1, timezone="UTC")
    admin = User(household_id=1, email=default_admin_email, phone="+15550000001", is_admin=True, verified=True)
    profile = PreferenceProfile(household_id=1, raw_text="Closures are critical", structured_json={"user_priority_topics": []})
    db.add_all([household, admin, profile])
    db.flush()

    credential = GoogleCredential(
        household_id=1,
        provider_user_email=default_admin_email,
        token_subject=default_admin_email,
        access_token="mock-access-token",
        status="active",
    )
    db.add(credential)
    db.flush()

    binding = CalendarBinding(
        household_id=1,
        google_credential_id=credential.id,
        calendar_id="primary",
        calendar_owner_email=default_admin_email,
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


def _normalized_sender_identity(value: str) -> tuple[str, str]:
    name, email = parseaddr(value or "")
    normalized_email = email.strip().lower()
    normalized_name = re.sub(r"\s+", " ", name.strip())
    if normalized_email:
        return normalized_email, normalized_name
    fallback = re.sub(r"\s+", " ", (value or "").strip())
    return "", fallback


_GRADE_ANNOTATIONS = {"JK": "JK (Junior Kindergarten)", "SK": "SK (Senior Kindergarten)"}


def _annotate_grade(grade: str) -> str:
    return _GRADE_ANNOTATIONS.get(grade.upper(), grade)


def _serialize_teacher_contacts(child: Child) -> list[dict]:
    contacts = []
    for contact in list(child.teacher_contacts or []):
        contacts.append(
            {
                "id": contact.id,
                "teacher_name": contact.teacher_name,
                "teacher_email": contact.teacher_email,
                "status": contact.status,
            }
        )
    return contacts


def _clean_teacher_contacts(raw_contacts: list[dict]) -> list[dict]:
    cleaned: list[dict] = []
    seen_emails: set[str] = set()
    for raw_contact in raw_contacts:
        teacher_name = re.sub(r"\s+", " ", str(raw_contact.get("teacher_name") or "").strip())
        teacher_email, parsed_name = _normalized_sender_identity(str(raw_contact.get("teacher_email") or ""))
        if not teacher_email:
            continue
        if not teacher_name:
            teacher_name = parsed_name
        status = str(raw_contact.get("status") or "active").strip().lower() or "active"
        if teacher_email in seen_emails:
            continue
        seen_emails.add(teacher_email)
        cleaned.append(
            {
                "teacher_name": teacher_name,
                "teacher_email": teacher_email,
                "status": status,
            }
        )
    return cleaned


def _replace_teacher_contacts(child: Child, raw_contacts: list[dict]) -> None:
    child.teacher_contacts[:] = [
        TeacherContact(
            teacher_name=contact["teacher_name"],
            teacher_email=contact["teacher_email"],
            status=contact["status"],
        )
        for contact in _clean_teacher_contacts(raw_contacts)
    ]


def _parse_teacher_contacts_text(raw_text: str) -> list[dict]:
    contacts: list[dict] = []
    for raw_line in (raw_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        name, email = parseaddr(line)
        if email:
            contacts.append({"teacher_name": name.strip(), "teacher_email": email.strip(), "status": "active"})
            continue
        if "," in line:
            left, right = [part.strip() for part in line.split(",", 1)]
            contacts.append({"teacher_name": left, "teacher_email": right, "status": "active"})
            continue
        contacts.append({"teacher_name": "", "teacher_email": line, "status": "active"})
    return contacts


def _build_agent_household_context(
    db: Session,
    *,
    user: User,
    household: Household | None = None,
    children: list[Child] | None = None,
    priority_preferences: dict | None = None,
) -> dict:
    household_row = household or db.scalar(select(Household).where(Household.id == user.household_id))
    child_rows = (
        children
        if children is not None
        else db.scalars(select(Child).where(Child.household_id == user.household_id, Child.status == "active")).all()
    )
    preference_payload = priority_preferences or load_priority_preferences(db, user.household_id)
    timezone_name = (
        str(user.timezone or "").strip()
        or str(getattr(household_row, "timezone", "") or "").strip()
        or "UTC"
    )
    return {
        "timezone": timezone_name,
        "household": {
            "household_id": user.household_id,
            "timezone": str(getattr(household_row, "timezone", "") or timezone_name),
            "spouse_phone": str(getattr(household_row, "spouse_phone", "") or ""),
            "spouse_notifications_enabled": bool(getattr(household_row, "spouse_notifications_enabled", False)),
            "daily_summary_enabled": bool(getattr(household_row, "daily_summary_enabled", False)),
            "weekly_digest_enabled": bool(getattr(household_row, "weekly_digest_enabled", False)),
        },
        "admin_user": {
            "user_id": user.id,
            "email": str(user.email or ""),
            "phone": str(user.phone or ""),
            "timezone": timezone_name,
            "is_admin": bool(user.is_admin),
            "verified": bool(user.verified),
        },
        "children": [
            {
                "id": child.id,
                "name": str(child.name or ""),
                "school_name": str(child.school_name or ""),
                "grade": _annotate_grade(str(child.grade or "")),
                "status": str(child.status or ""),
                "teacher_contacts": _serialize_teacher_contacts(child),
            }
            for child in child_rows
        ],
        "preferences": {
            "raw_text": str(preference_payload.get("raw_text") or ""),
            "system_defaults": list(preference_payload.get("system_defaults") or []),
            "user_priority_topics": list(preference_payload.get("user_priority_topics") or []),
            "effective_suppressed_priority_topics": list(
                preference_payload.get("effective_suppressed_priority_topics") or []
            ),
            "command_written_preferences": list(preference_payload.get("command_written_preferences") or []),
        },
    }


def _handle_more_info_command(
    *,
    db: Session | None = None,
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
        message = _compose_more_info_message(db=db, topic=topic, context=context, match=match)
        return InboundResponse(
            status=WebhookStatus.COMMAND_COMPLETED.value,
            message=message,
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


def _compose_more_info_message(
    *,
    db: Session | None = None,
    topic: str,
    context: FollowupContext,
    match: FollowupMatch,
) -> str:
    matched_item = dict(match.item or {})
    assistant_summary = ""
    for snippet in list(context.section_snippets or []):
        if str(snippet.get("meta") or "") == "document_understanding":
            assistant_summary = str(snippet.get("text") or "").strip()
            if assistant_summary:
                break
    context_assessment = assess_more_info_context(context, match, limit=3)
    source_snippets = list(context_assessment.stored_snippets)
    if context_assessment.weak and db is not None:
        source_snippets.extend(
            retrieve_more_info_source_snippets(
                db,
                context=context,
                match=match,
                query_text=topic,
                limit=3,
            )
        )
    deduped_source_snippets: list[str] = []
    seen_source_snippets: set[str] = set()
    for snippet in source_snippets:
        normalized = _normalize_followup_text(snippet)
        if not normalized or normalized in seen_source_snippets:
            continue
        seen_source_snippets.add(normalized)
        deduped_source_snippets.append(str(snippet).strip())
    payload = {
        "user_query": str(topic or "").strip(),
        "summary_title": str(context.summary_title or "").strip(),
        "assistant_summary": assistant_summary,
        "summary_line": str(
            matched_item.get("display_text")
            or matched_item.get("text")
            or matched_item.get("title")
            or ""
        ).strip(),
        "matched_item": {
            "item_id": matched_item.get("item_id"),
            "title": matched_item.get("title"),
            "display_text": matched_item.get("display_text"),
            "text": matched_item.get("text"),
            "aliases": list(matched_item.get("aliases") or []),
            "kind": matched_item.get("kind"),
            "start_at": matched_item.get("start_at"),
            "end_at": matched_item.get("end_at"),
            "date_sort_key": matched_item.get("date_sort_key"),
            "reason": matched_item.get("reason"),
            "assistant_detail": matched_item.get("assistant_detail"),
            "timing_hint": matched_item.get("timing_hint"),
            "action_hint": matched_item.get("action_hint"),
            "scope_hint": matched_item.get("scope_hint"),
            "applies_to": list(matched_item.get("applies_to") or []),
            "action_capabilities": dict(matched_item.get("action_capabilities") or {}),
            "source_refs": list(matched_item.get("source_refs") or []),
        },
        "source_snippets": deduped_source_snippets[:4],
        "source_retrieval_used": bool(context_assessment.weak and len(deduped_source_snippets) > len(context_assessment.stored_snippets)),
        "source_retrieval_reason": context_assessment.reason,
    }
    composer = getattr(engine_llm, "compose_more_info_reply", None)
    if callable(composer):
        try:
            result = composer(payload)
        except Exception:
            result = None
        if isinstance(result, dict):
            message = str(result.get("message") or "").strip()
            if message:
                return message
    return build_more_info_message(context, match, snippets=deduped_source_snippets[:4])


def _response_target_for_channel(user: User, channel: str) -> str:
    return user.phone if channel == "sms" else user.email


def _source_session_id(source: SourceMessage) -> str:
    if source.source_channel == "sms":
        return sms_session_id(household_id=source.household_id)
    thread_key = str(source.thread_key or source.provider_message_id or "").strip()
    return email_session_id(household_id=source.household_id, thread_key=thread_key)


def _followup_context_key(source: SourceMessage) -> str:
    if source.source_channel == "sms":
        return sms_session_id(household_id=source.household_id)
    return str(source.thread_key or source.provider_message_id or "").strip()


def _append_user_turn_to_session(db: Session, source: SourceMessage) -> None:
    if source.source_channel == "sms":
        text = str(source.body_text or "").strip()
    else:
        text = (
            "Email subject:\n"
            f"{source.subject or ''}\n\n"
            "Email body:\n"
            f"{source.body_text or ''}"
        ).strip()
    if not text:
        return
    queue_session_message(
        db,
        _source_session_id(source),
        role="user",
        text=text,
    )


@contextmanager
def _conversation_runtime_scope(
    *,
    source: SourceMessage,
    thread_documents: list,
    household_context: dict | None = None,
) -> Iterator[None]:
    session_id = _source_session_id(source)
    email_headers = build_email_reply_headers(source) if source.source_channel == "email" else None
    trace_context = current_trace_context()
    trace_metadata = dict(trace_context.metadata or {}) if trace_context and trace_context.metadata else {}
    trace_metadata.update(
        {
            "household_id": source.household_id,
            "source_channel": source.source_channel,
            "thread_key": str(source.thread_key or ""),
            "provider_message_id": source.provider_message_id,
        }
    )
    workflow_name = (
        trace_context.workflow_name
        if trace_context and trace_context.workflow_name
        else "LovelyChaos inbound conversation"
    )
    group_id = str(source.thread_key or source.provider_message_id or session_id)
    llm_scope = (
        engine_llm.conversation_scope(
            session_id=session_id,
            workflow_name=workflow_name,
            group_id=group_id,
            thread_documents=thread_documents,
            household_context=household_context or {},
            trace_metadata=trace_metadata,
        )
        if hasattr(engine_llm, "conversation_scope")
        else nullcontext()
    )
    session_token = _CURRENT_CONVERSATION_SESSION_ID.set(session_id)
    header_token = _CURRENT_EMAIL_REPLY_HEADERS.set(email_headers)
    try:
        with llm_scope:
            yield
    finally:
        _CURRENT_EMAIL_REPLY_HEADERS.reset(header_token)
        _CURRENT_CONVERSATION_SESSION_ID.reset(session_token)


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
    result = send_channel_notification(
        db=db,
        provider=notification_provider,
        household_id=user.household_id,
        recipient_type="admin",
        channel=channel,
        target=target,
        template=template,
        subject=subject,
        message=message,
        email_headers=_CURRENT_EMAIL_REPLY_HEADERS.get() if channel == "email" else None,
    )
    session_id = _CURRENT_CONVERSATION_SESSION_ID.get()
    if session_id and result.get("sent"):
        queue_session_message(
            db,
            session_id,
            role="assistant",
            text=message,
        )


def _commit_then_send_user_response(
    *,
    db: Session,
    user: User,
    channel: str,
    template: str,
    subject: str,
    message: str,
) -> None:
    # End the ingestion transaction before outbound network calls so concurrent
    # webhooks do not sit behind a long-lived SQLite writer.
    db.commit()
    _send_user_response(
        db=db,
        user=user,
        channel=channel,
        template=template,
        subject=subject,
        message=message,
    )
    db.commit()


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
    _commit_then_send_user_response(
        db=db,
        user=user,
        channel=channel,
        template=template,
        subject=subject,
        message=message,
    )
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
    if getattr(settings, "google_calendar_mode", "live") != "live":
        return credential, binding
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
    event_id = command["event_id"]
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


def _followup_item_display(item: dict, timezone_name: str) -> str:
    label = str(item.get("display_text") or item.get("text") or item.get("title") or "Untitled item").strip()
    start_at = _coerce_datetime(item.get("start_at") or item.get("date_sort_key"))
    end_at = _coerce_datetime(item.get("end_at"))
    window = _format_event_window(start_at, end_at, timezone_name)
    return f"{window}: {label}" if window else label


def _build_numbered_followup_clarification(
    candidate_items: list[dict],
    *,
    timezone_name: str,
    requested_action: str,
) -> str:
    if requested_action == "more_info":
        lines = ["I found multiple matching topics in the latest school updates. Reply with the number or exact topic:"]
    else:
        lines = ["I found multiple possible events in that email. Reply with the number or exact event you want:"]
    for idx, item in enumerate(candidate_items[:5], start=1):
        lines.append(f"{idx}. {_followup_item_display(item, timezone_name)}")
    return "\n".join(lines)


def _looks_like_calendar_add_request(text: str) -> bool:
    normalized = (text or "").strip().lower()
    if not normalized:
        return False
    if re.search(r"\badd\b", normalized):
        return True
    return bool(re.search(r"\b(?:calendar|cal)\b", normalized) and _parse_month_day_date(text, "UTC"))


def _select_candidate_by_index(reply_text: str, candidate_items: list[dict]) -> Optional[dict]:
    match = re.search(r"\b(?:option\s*)?(\d{1,2})\b", reply_text or "", re.I)
    if not match:
        return None
    index = int(match.group(1))
    if 1 <= index <= len(candidate_items):
        return candidate_items[index - 1]
    return None


def _resolve_recent_followup_matches(
    contexts: list[FollowupContext],
    *,
    query_text: str,
    topic: Optional[str] = None,
) -> tuple[Optional[FollowupContext], list]:
    for context in contexts:
        matches = resolve_followup_candidates(context, query_text=query_text, topic=topic)
        if matches:
            return context, matches
    return None, []


def _conversation_state_context(
    state: SmsConversationState,
    contexts: list[FollowupContext],
) -> Optional[FollowupContext]:
    allowed_ids = {int(value) for value in list(state.source_followup_context_ids or []) if str(value).isdigit()}
    if not allowed_ids:
        return contexts[0] if contexts else None
    for context in contexts:
        if context.id in allowed_ids:
            return context
    return contexts[0] if contexts else None


def _clarification_reply(
    *,
    db: Session,
    user: User,
    request_id: str,
    message: str,
) -> InboundResponse:
    return _command_reply(
        db=db,
        user=user,
        channel="sms",
        template="add_clarification",
        subject="LovelyChaos SMS",
        status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
        message=message,
        request_id=request_id,
        mutation_executed=False,
    )


def _persist_sms_selection_state(
    db: Session,
    *,
    user: User,
    requested_action: str,
    candidate_items: list[dict],
    source_followup_context_ids: list[int],
) -> SmsConversationState:
    prompt_message = _build_numbered_followup_clarification(
        candidate_items,
        timezone_name=user.timezone,
        requested_action=requested_action,
    )
    return persist_sms_conversation_state(
        db,
        household_id=user.household_id,
        requested_action=requested_action,
        candidate_items=candidate_items,
        source_followup_context_ids=source_followup_context_ids,
        prompt_message=prompt_message,
    )


def _resolve_sms_selection_state(
    *,
    db: Session,
    request_id: str,
    user: User,
    state: SmsConversationState,
    source: SourceMessage,
    receipt: WebhookReceipt,
    provider_message_id: str,
    raw_body_text: str,
    recent_contexts: list[FollowupContext],
) -> InboundResponse:
    selected_item = _select_candidate_by_index(raw_body_text, list(state.candidate_items or []))
    if selected_item is None:
        matches = resolve_candidate_items(list(state.candidate_items or []), query_text=raw_body_text)
        if not matches:
            return _clarification_reply(
                db=db,
                user=user,
                request_id=request_id,
                message=state.prompt_message or "Tell me which item you mean.",
            )
        if len(matches) > 1:
            narrowed_items = [dict(match.item) for match in matches]
            _persist_sms_selection_state(
                db,
                user=user,
                requested_action=state.requested_action,
                candidate_items=narrowed_items,
                source_followup_context_ids=list(state.source_followup_context_ids or []),
            )
            return _clarification_reply(
                db=db,
                user=user,
                request_id=request_id,
                message=_build_numbered_followup_clarification(
                    narrowed_items,
                    timezone_name=user.timezone,
                    requested_action=state.requested_action,
                ),
            )
        selected_item = dict(matches[0].item)

    resolve_sms_conversation_state(state)
    if state.requested_action == "more_info":
        context = _conversation_state_context(state, recent_contexts)
        if context is not None:
            message = _compose_more_info_message(
                db=db,
                topic=raw_body_text,
                context=context,
                match=FollowupMatch(item=selected_item, from_summary=False, score=10),
            )
        else:
            title = str(selected_item.get("display_text") or selected_item.get("text") or selected_item.get("title") or "Topic").strip()
            reason = str(selected_item.get("reason") or "").strip()
            message = title if not reason else f"{title}\n- {reason}"
        return _command_reply(
            db=db,
            user=user,
            channel="sms",
            template="more_info",
            subject="LovelyChaos SMS",
            status=WebhookStatus.COMMAND_COMPLETED,
            message=message,
            request_id=request_id,
            mutation_executed=False,
        )

    candidate = _candidate_from_followup_item(selected_item, user.timezone)
    if candidate is None:
        return _clarification_reply(
            db=db,
            user=user,
            request_id=request_id,
            message="I matched that topic from the last update, but it doesn't have enough scheduling detail to add to the calendar.",
        )
    return _create_event_from_candidate(
        db=db,
        request_id=request_id,
        user=user,
        source=source,
        receipt=receipt,
        provider_message_id=provider_message_id,
        response_channel="sms",
        response_subject="LovelyChaos SMS",
        candidate=candidate,
        inputs={
            "subject": "",
            "body_text": raw_body_text,
            "command_topic": None,
            "response_channel": "sms",
        },
        model_output={"source": "sms_conversation_state", "matched_item": selected_item},
    )


def _try_contextual_sms_assistant(
    *,
    db: Session,
    request_id: str,
    user: User,
    source: SourceMessage,
    receipt: WebhookReceipt,
    provider_message_id: str,
    raw_body_text: str,
    command: dict,
    active_state: Optional[SmsConversationState],
) -> Optional[InboundResponse]:
    recent_contexts = load_recent_followup_contexts(db, household_id=user.household_id)

    if active_state and command.get("action") in {"none", "add", "more_info"}:
        return _resolve_sms_selection_state(
            db=db,
            request_id=request_id,
            user=user,
            state=active_state,
            source=source,
            receipt=receipt,
            provider_message_id=provider_message_id,
            raw_body_text=raw_body_text,
            recent_contexts=recent_contexts,
        )

    requested_action = str(command.get("action") or "none")
    if requested_action == "none" and _looks_like_calendar_add_request(raw_body_text):
        requested_action = "add"
    if requested_action not in {"add", "more_info"}:
        return None

    context, matches = _resolve_recent_followup_matches(
        recent_contexts,
        query_text=raw_body_text,
        topic=command.get("topic"),
    )
    if not matches:
        return None
    if len(matches) > 1:
        candidate_items = [dict(match.item) for match in matches]
        source_ids = [context.id] if context is not None else []
        _persist_sms_selection_state(
            db,
            user=user,
            requested_action=requested_action,
            candidate_items=candidate_items,
            source_followup_context_ids=source_ids,
        )
        return _clarification_reply(
            db=db,
            user=user,
            request_id=request_id,
            message=_build_numbered_followup_clarification(
                candidate_items,
                timezone_name=user.timezone,
                requested_action=requested_action,
            ),
        )

    match = matches[0]
    if requested_action == "more_info":
        if context is None:
            return None
        return _command_reply(
            db=db,
            user=user,
            channel="sms",
            template="more_info",
            subject="LovelyChaos SMS",
            status=WebhookStatus.COMMAND_COMPLETED,
            message=_compose_more_info_message(
                db=db,
                topic=command.get("topic") or raw_body_text,
                context=context,
                match=match,
            ),
            request_id=request_id,
            mutation_executed=False,
        )

    candidate = _candidate_from_followup_item(match.item, user.timezone)
    if candidate is None:
        return _clarification_reply(
            db=db,
            user=user,
            request_id=request_id,
            message="I matched that topic from the last update, but it doesn't have enough scheduling detail to add to the calendar.",
        )
    return _create_event_from_candidate(
        db=db,
        request_id=request_id,
        user=user,
        source=source,
        receipt=receipt,
        provider_message_id=provider_message_id,
        response_channel="sms",
        response_subject="LovelyChaos SMS",
        candidate=candidate,
        inputs={
            "subject": "",
            "body_text": raw_body_text,
            "command_topic": command.get("topic"),
            "response_channel": "sms",
        },
        model_output={"source": "recent_followup_context", "matched_item": match.item},
    )


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
        r"^\s*(?:please\s+)?add\s+(?P<title>.+?)\s+to\s+(?:the\s+)?cal(?:endar)?\s+(?:for|on)\s+(?P<date>.+?)\s*$",
        r"^\s*(?:please\s+)?add\s+(?P<title>.+?)\s+(?:for|on)\s+(?P<date>.+?)\s*$",
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


def _parse_forwarded_reference_datetime(value: str, timezone_name: str) -> Optional[datetime]:
    raw = (value or "").replace("\u202f", " ").replace("\xa0", " ").strip()
    raw = re.sub(r"\s+at\s+(?=\d{1,2}:\d{2}\s*[ap]m\b)", " ", raw, flags=re.I)
    if not raw:
        return None
    parsed = None
    for fmt in (
        "%a, %b %d, %Y %I:%M %p",
        "%a, %B %d, %Y %I:%M %p",
        "%b %d, %Y %I:%M %p",
        "%B %d, %Y %I:%M %p",
        "%a, %b %d, %Y %I:%M %p %z",
        "%a, %B %d, %Y %I:%M %p %z",
        "%b %d, %Y %I:%M %p %z",
        "%B %d, %Y %I:%M %p %z",
    ):
        try:
            parsed = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError, IndexError):
            return None
    if parsed.tzinfo is None:
        try:
            parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
        except Exception:
            parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_time_mentions(text: str) -> list[tuple[int, int]]:
    seen: set[tuple[int, int]] = set()
    times: list[tuple[int, int]] = []
    for match in re.finditer(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", text or "", re.I):
        hour = int(match.group(1))
        minute = int(match.group(2) or 0)
        meridiem = match.group(3).lower()
        if meridiem == "pm" and hour != 12:
            hour += 12
        if meridiem == "am" and hour == 12:
            hour = 0
        key = (hour, minute)
        if key in seen:
            continue
        seen.add(key)
        times.append(key)
    times.sort()
    return times


def _extract_date_windows(
    text: str,
    *,
    timezone_name: str,
    reference_dt: Optional[datetime],
) -> list[tuple[datetime, datetime]]:
    zone = ZoneInfo(timezone_name)
    year_hint = _to_user_timezone(reference_dt, timezone_name).year if reference_dt else datetime.now(zone).year
    windows: list[tuple[datetime, datetime]] = []
    seen: set[str] = set()
    pattern = re.compile(
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(\d{4}))?\b",
        re.I,
    )
    for match in pattern.finditer(text or ""):
        month = MONTH_NAME_MAP[match.group(1).lower().rstrip(".")]
        day = int(match.group(2))
        year = int(match.group(3)) if match.group(3) else year_hint
        try:
            start_local = datetime(year, month, day, 0, 0, tzinfo=zone)
        except ValueError:
            continue
        key = start_local.date().isoformat()
        if key in seen:
            continue
        seen.add(key)
        windows.append((start_local.astimezone(timezone.utc), (start_local + timedelta(days=1)).astimezone(timezone.utc)))

    normalized = _compact_text(text)
    if not windows and reference_dt is not None:
        ref_local = _to_user_timezone(reference_dt, timezone_name)
        relative_day: Optional[datetime] = None
        if re.search(r"\btonight\b", normalized, re.I):
            relative_day = ref_local
        elif re.search(r"\btomorrow\b", normalized, re.I):
            relative_day = ref_local + timedelta(days=1)
        if relative_day is not None:
            start_local = datetime(relative_day.year, relative_day.month, relative_day.day, 0, 0, tzinfo=zone)
            windows.append((start_local.astimezone(timezone.utc), (start_local + timedelta(days=1)).astimezone(timezone.utc)))
    return windows


def _with_time_window(
    start_at: datetime,
    end_at: datetime,
    *,
    timezone_name: str,
    times: list[tuple[int, int]],
) -> tuple[datetime, datetime]:
    if not times:
        return start_at, end_at
    zone = ZoneInfo(timezone_name)
    start_local = _to_user_timezone(start_at, timezone_name)
    start_hour, start_minute = times[0]
    end_hour, end_minute = times[-1]
    start_local = datetime(
        start_local.year,
        start_local.month,
        start_local.day,
        start_hour,
        start_minute,
        tzinfo=zone,
    )
    end_local = datetime(
        start_local.year,
        start_local.month,
        start_local.day,
        end_hour,
        end_minute,
        tzinfo=zone,
    )
    if end_local <= start_local:
        end_local = start_local + timedelta(hours=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _allows_multiple_add(text: str) -> bool:
    normalized = _compact_text(text)
    return bool(re.search(r"\b(these|all|dates|days|both|every)\b", normalized, re.I))


def _strip_forwarded_headers(text: str) -> str:
    lines = (text or "").replace("\r", "").splitlines()
    if not lines:
        return ""
    idx = 0
    saw_headers = False
    while idx < len(lines):
        line = lines[idx].strip()
        if not line:
            if saw_headers:
                idx += 1
                break
            idx += 1
            continue
        if re.match(r"^-+\s*forwarded message\s*-+$", line, flags=re.IGNORECASE):
            saw_headers = True
            idx += 1
            continue
        if re.match(r"^begin forwarded message:", line, flags=re.IGNORECASE):
            saw_headers = True
            idx += 1
            continue
        if re.match(r"^(from|date|subject|to):", line, flags=re.IGNORECASE):
            saw_headers = True
            idx += 1
            continue
        if saw_headers:
            break
        return "\n".join(lines).strip()
    return "\n".join(lines[idx:]).strip()


def _command_topic_tokens(text: Optional[str]) -> list[str]:
    stopwords = {
        "add",
        "calendar",
        "date",
        "dates",
        "day",
        "days",
        "event",
        "events",
        "please",
        "this",
        "these",
        "that",
        "those",
        "the",
        "to",
        "all",
        "both",
        "every",
    }
    tokens: list[str] = []
    seen: set[str] = set()
    for token in re.findall(r"[a-z0-9]+", _compact_text(text), flags=re.I):
        if ((len(token) < 3 and not token.isdigit()) or token in stopwords or token in seen):
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _fallback_command_topic(raw_body_text: str) -> Optional[str]:
    focused_text = _normalize_preface_text(raw_body_text)
    for pattern in [
        r"^\s*(?:please\s+)?add\s+(.+?)\s+to\s+(?:the\s+)?cal(?:endar)?\s*$",
        r"^\s*(?:please\s+)?add\s+(.+?)\s*$",
    ]:
        match = re.match(pattern, focused_text or "", re.I)
        if not match:
            continue
        candidate = match.group(1).strip(" .?!")
        tokens = _command_topic_tokens(candidate)
        if tokens:
            return " ".join(tokens[:6])
        if candidate:
            return candidate
    return None


def _relevant_forwarded_text(content_body_text: str, command_topic: Optional[str]) -> str:
    body_text = _strip_forwarded_headers(content_body_text)
    if not body_text:
        return ""
    topic_tokens = _command_topic_tokens(command_topic)
    if not topic_tokens:
        return body_text

    paragraphs = [chunk.strip() for chunk in re.split(r"\n\s*\n", body_text) if chunk.strip()]
    matches = [
        paragraph
        for paragraph in paragraphs
        if any(token in _compact_text(paragraph) for token in topic_tokens)
    ]
    if matches:
        return "\n\n".join(matches)

    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    line_matches = [line for line in lines if any(token in _compact_text(line) for token in topic_tokens)]
    if line_matches:
        return "\n".join(line_matches)
    return body_text


def _normalize_resolved_title(title: str, *, singular: bool) -> str:
    resolved = (title or "").strip() or "School event"
    if singular:
        resolved = re.sub(r"\b(?:dates?|days?)\b\s*$", "", resolved, flags=re.I).strip(" -:")
        if not resolved:
            resolved = title.strip() or "School event"
    if resolved.islower():
        resolved = resolved.title()
    return resolved


def _resolve_forwarded_add_candidates(
    *,
    extracted_events: list[ExtractedEvent],
    command_topic: Optional[str],
    content_body_text: str,
    forwarded_subject: str,
    forwarded_date: str,
    timezone_name: str,
) -> tuple[list[ExtractedEvent], dict]:
    scoped_body_text = _relevant_forwarded_text(content_body_text, command_topic)
    combined_text = "\n".join(part for part in [forwarded_subject, scoped_body_text] if part).strip()
    reference_dt = _parse_forwarded_reference_datetime(forwarded_date, timezone_name)
    date_windows = _extract_date_windows(combined_text, timezone_name=timezone_name, reference_dt=reference_dt)
    times = _extract_time_mentions(combined_text)
    base_title = (command_topic or "").strip() or ((extracted_events[0].title if extracted_events else "") or "").strip()
    if not base_title:
        base_title = _display_subject(forwarded_subject or "").strip() or "School event"

    resolved: list[ExtractedEvent] = []
    extracted_with_dates = [candidate for candidate in extracted_events if candidate.start_at]
    for candidate in extracted_with_dates:
        title = (candidate.title or "").strip() or base_title
        start_at = candidate.start_at
        end_at = candidate.end_at
        if start_at and end_at is None:
            local_start = _to_user_timezone(start_at, timezone_name)
            if local_start.hour == 0 and local_start.minute == 0:
                end_at = start_at + timedelta(hours=1)
            else:
                end_at = start_at + timedelta(hours=1)
        elif start_at is not None and start_at.hour == 0 and start_at.minute == 0 and times:
            start_at, end_at = _with_time_window(start_at, end_at or (start_at + timedelta(hours=1)), timezone_name=timezone_name, times=times)
        if title and start_at and end_at:
            resolved.append(
                ExtractedEvent(
                    title=title,
                    start_at=start_at,
                    end_at=end_at,
                    category=candidate.category,
                    confidence=max(candidate.confidence, 0.9),
                    target_scope=candidate.target_scope,
                    mentioned_names=list(candidate.mentioned_names or []),
                    mentioned_schools=list(candidate.mentioned_schools or []),
                    target_grades=list(candidate.target_grades or []),
                    preference_match=bool(candidate.preference_match),
                    model_reason=(candidate.model_reason or "resolved_from_forwarded_context"),
                )
            )

    if not resolved and date_windows:
        singular_title = _normalize_resolved_title(base_title, singular=len(date_windows) > 1)
        for start_at, end_at in date_windows:
            if times:
                actual_start, actual_end = _with_time_window(start_at, end_at, timezone_name=timezone_name, times=times)
            else:
                actual_start = start_at
                actual_end = start_at + timedelta(hours=1)
            resolved.append(
                ExtractedEvent(
                    title=singular_title,
                    start_at=actual_start,
                    end_at=actual_end,
                    category=(extracted_events[0].category if extracted_events else "forwarded"),
                    confidence=0.9,
                    target_scope=(extracted_events[0].target_scope if extracted_events else "school_specific"),
                    mentioned_names=list(extracted_events[0].mentioned_names or []) if extracted_events else [],
                    mentioned_schools=list(extracted_events[0].mentioned_schools or []) if extracted_events else [],
                    target_grades=list(extracted_events[0].target_grades or []) if extracted_events else [],
                    preference_match=bool(extracted_events[0].preference_match) if extracted_events else False,
                    model_reason="resolved_from_forwarded_dates",
                )
            )

    return dedupe_extracted_events(resolved), {
        "source": "forwarded_date_resolver",
        "reference_datetime": _serialize_dt(reference_dt),
        "date_windows": [_serialize_dt(start) for start, _ in date_windows],
        "times": [f"{hour:02d}:{minute:02d}" for hour, minute in times],
        "scoped_text": scoped_body_text[:500],
    }


def _past_only_candidates(candidates: list[ExtractedEvent]) -> list[ExtractedEvent]:
    past: list[ExtractedEvent] = []
    for candidate in candidates:
        validation = validate_candidate(candidate)
        issues = set(validation.get("issues") or [])
        if "event_in_past" in issues and not (issues - {"event_in_past", "low_confidence"}):
            past.append(candidate)
    return past


def _build_past_event_message(candidates: list[ExtractedEvent], timezone_name: str) -> str:
    if len(candidates) == 1:
        candidate = candidates[0]
        window = _format_event_window(candidate.start_at, candidate.end_at, timezone_name)
        title = (candidate.title or "That event").strip()
        if window:
            return f"I found {title} on {window}, but it has already passed, so I didn't add it to your calendar."
        return f"I found {title}, but it has already passed, so I didn't add it to your calendar."
    items = []
    for candidate in candidates[:5]:
        title = (candidate.title or "Untitled event").strip()
        window = _format_event_window(candidate.start_at, candidate.end_at, timezone_name)
        items.append(f"{window}: {title}" if window else title)
    return (
        "I found these events, but they have already passed, so I didn't add them to your calendar:\n- "
        + "\n- ".join(items)
    )


def _summary_has_content(summary_result) -> bool:
    return bool(
        summary_result.important_info
        or summary_result.other_dates
        or summary_result.other_topics
        or "couldn't extract a clean summary" in (summary_result.rendered_message or "").lower()
    )


def _document_understanding_priority_terms(document_understanding: dict | None) -> list[str]:
    if not isinstance(document_understanding, dict):
        return []
    terms: list[str] = []
    for key in ("assistant_summary", "assistant_intro"):
        value = str(document_understanding.get(key) or "").strip()
        if value:
            terms.append(value)
    for bucket in ("actionable_topics", "informational_topics"):
        for topic in list(document_understanding.get(bucket) or []):
            if not isinstance(topic, dict):
                continue
            for key in ("title", "why_it_matters", "timing_hint", "action_hint"):
                value = str(topic.get(key) or "").strip()
                if value:
                    terms.append(value)
    return terms


def _topic_label_matches(topic_label: str, *text_values: str) -> bool:
    return topic_matches_text(topic_label, *text_values)


def _document_topics_to_followup_items(
    document_understanding: dict | None,
    *,
    source_message_id: int,
    user_priority_topics: list[str] | None = None,
    suppressed_priority_topics: list[str] | None = None,
) -> list[dict]:
    if not isinstance(document_understanding, dict):
        return []
    positive_topics = list(user_priority_topics or [])
    suppressed_topics = list(suppressed_priority_topics or [])
    items: list[dict] = []
    for bucket_name, default_can_add in (("actionable_topics", False), ("informational_topics", False)):
        for idx, topic in enumerate(list(document_understanding.get(bucket_name) or []), start=1):
            if not isinstance(topic, dict):
                continue
            title = str(topic.get("title") or "").strip()
            detail = str(topic.get("why_it_matters") or "").strip()
            action_hint = str(topic.get("action_hint") or "").strip()
            timing_hint = str(topic.get("timing_hint") or "").strip()
            if not title:
                continue
            aliases = [title]
            if timing_hint:
                aliases.append(timing_hint)
            text = title
            if detail:
                text = f"{title}: {detail}"
            matched_user = [t for t in positive_topics if _topic_label_matches(t, title, detail)]
            matched_suppressed = [t for t in suppressed_topics if _topic_label_matches(t, title, detail)]
            items.append(
                {
                    "item_id": f"msg-{source_message_id}-{bucket_name}-{idx}",
                    "title": title,
                    "text": text,
                    "display_text": title,
                    "aliases": aliases,
                    "kind": "document_topic",
                    "start_at": None,
                    "end_at": None,
                    "date_sort_key": None,
                    "all_day": False,
                    "source_message_id": source_message_id,
                    "reason": detail or action_hint or "document_understanding_topic",
                    "assistant_detail": detail,
                    "action_hint": action_hint or None,
                    "timing_hint": timing_hint or None,
                    "scope_hint": str(topic.get("scope_hint") or "unknown"),
                    "applies_to": [],
                    "matched_user_priorities": matched_user,
                    "suppressed_match": bool(matched_suppressed),
                    "matched_suppressed_topics": matched_suppressed,
                    "action_capabilities": {
                        "can_add": default_can_add,
                        "can_explain": True,
                    },
                }
            )
    return items


def _document_understanding_section_snippets(document_understanding: dict | None) -> list[dict]:
    if not isinstance(document_understanding, dict):
        return []
    snippets: list[dict] = []
    assistant_summary = str(document_understanding.get("assistant_summary") or "").strip()
    if assistant_summary:
        snippets.append(
            {
                "label": "assistant_summary",
                "text": assistant_summary,
                "meta": "document_understanding",
            }
        )
    for bucket_name in ("actionable_topics", "informational_topics"):
        for idx, topic in enumerate(list(document_understanding.get(bucket_name) or []), start=1):
            if not isinstance(topic, dict):
                continue
            title = str(topic.get("title") or "").strip()
            detail = str(topic.get("why_it_matters") or "").strip()
            action_hint = str(topic.get("action_hint") or "").strip()
            timing_hint = str(topic.get("timing_hint") or "").strip()
            text_parts = [detail]
            if action_hint:
                text_parts.append(action_hint)
            if timing_hint:
                text_parts.append(f"Timing: {timing_hint}")
            if not title or not any(text_parts):
                continue
            snippets.append(
                {
                    "label": title,
                    "text": " ".join(part for part in text_parts if part).strip(),
                    "meta": "document_topic",
                    "bucket": bucket_name,
                    "topic_index": idx,
                }
            )
    return snippets


def _is_all_day_window(start_at: Optional[datetime], end_at: Optional[datetime], timezone_name: str) -> bool:
    if start_at is None or end_at is None:
        return False
    start_local = _to_user_timezone(start_at, timezone_name)
    end_local = _to_user_timezone(end_at, timezone_name)
    if start_local is None or end_local is None:
        return False
    end_is_day_boundary = (
        end_local.hour == 0
        and end_local.minute == 0
        and end_local.second == 0
        and end_local.date() > start_local.date()
    )
    end_is_end_of_day = (
        end_local.date() == start_local.date()
        and end_local.hour == 23
        and end_local.minute == 59
        and end_local.second == 59
    )
    return (
        start_local.hour == 0
        and start_local.minute == 0
        and start_local.second == 0
        and (end_is_day_boundary or end_is_end_of_day)
    )


def _normalize_followup_text(value: str) -> str:
    cleaned = re.sub(r"^[A-Z][a-z]{2}\s+\d{1,2}(?:-[A-Z][a-z]{2}\s+\d{1,2})?:\s*", "", (value or "").strip())
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _looks_like_deadline_text(*values: str) -> bool:
    haystack = " ".join(str(value or "") for value in values).lower()
    return any(token in haystack for token in ["deadline", "register", "registration", "ordering", "order by", "due"])


def _normalize_deadline_window(
    start_at: Optional[datetime],
    end_at: Optional[datetime],
    timezone_name: str,
    *,
    title: str = "",
    reason: str = "",
    force_all_day: bool = False,
) -> tuple[Optional[datetime], Optional[datetime], bool]:
    if start_at is None:
        return start_at, end_at, False
    start_local = _to_user_timezone(start_at, timezone_name)
    end_local = _to_user_timezone(end_at, timezone_name) if end_at else None
    if start_local is None:
        return start_at, end_at, False
    all_day = force_all_day or _is_all_day_window(start_at, end_at, timezone_name)
    if all_day:
        normalized_end = end_at
        if end_local and end_local.date() == start_local.date():
            normalized_end = (start_local + timedelta(days=1)).astimezone(timezone.utc)
        return start_at, normalized_end, True
    if (
        _looks_like_deadline_text(title, reason)
        and start_local.hour == 0
        and start_local.minute == 0
        and start_local.second == 0
        and end_local is not None
        and end_local.date() == start_local.date()
        and (end_at - start_at) <= timedelta(hours=1)
    ):
        return start_at, (start_local + timedelta(days=1)).astimezone(timezone.utc), True
    return start_at, end_at, False


def _followup_summary_items(summary_result, actionable_items: list[dict]) -> list[dict]:
    actionable_by_title = {
        (_normalize_followup_text(str(item.get("title") or "")), str(item.get("date_sort_key") or "")): item
        for item in actionable_items
        if str(item.get("title") or "").strip()
    }
    summary_items: list[dict] = []
    for item in [
        *[line.as_dict() for line in summary_result.important_info],
        *[line.as_dict() for line in summary_result.other_dates],
        *[line.as_dict() for line in summary_result.other_topics],
    ]:
        normalized_text = _normalize_followup_text(str(item.get("text") or ""))
        date_sort_key = str(item.get("date_sort_key") or "")
        match = actionable_by_title.get((normalized_text, date_sort_key)) or actionable_by_title.get((normalized_text, ""))
        if match is None:
            for candidate in actionable_items:
                normalized_title = _normalize_followup_text(str(candidate.get("title") or ""))
                if normalized_title and (normalized_title in normalized_text or normalized_text in normalized_title):
                    if date_sort_key and str(candidate.get("date_sort_key") or "") not in {"", date_sort_key}:
                        continue
                    match = candidate
                    break
        payload = dict(item)
        if match is not None:
            payload["item_id"] = match.get("item_id")
            payload["aliases"] = list(match.get("aliases") or [])
            payload["kind"] = match.get("kind")
            payload["action_capabilities"] = dict(match.get("action_capabilities") or {})
        summary_items.append(payload)
    return summary_items


def _followup_actionable_items_from_extracted_events(
    *,
    extracted_events: list[ExtractedEvent],
    source_message_id: int,
    timezone_name: str,
) -> list[dict]:
    items: list[dict] = []
    for idx, event in enumerate(extracted_events, start=1):
        title = (event.title or "").strip()
        if not title:
            continue
        normalized_start, normalized_end, all_day = _normalize_deadline_window(
            event.start_at,
            event.end_at,
            timezone_name,
            title=title,
            reason=event.model_reason,
        )
        start_at = _serialize_dt(normalized_start)
        end_at = _serialize_dt(normalized_end)
        can_add = bool(normalized_start and normalized_end)
        kind = "deadline" if can_add and all_day else "event" if can_add else "topic"
        applies_to = [
            *list(event.mentioned_names or []),
            *[f"Gr {grade}" for grade in list(event.target_grades or [])],
        ]
        items.append(
            {
                "item_id": f"msg-{source_message_id}-item-{idx}",
                "title": title,
                "text": title,
                "aliases": [title, *applies_to],
                "kind": kind,
                "start_at": start_at,
                "end_at": end_at,
                "date_sort_key": start_at,
                "all_day": all_day,
                "source_message_id": source_message_id,
                "reason": event.model_reason,
                "applies_to": applies_to,
                "action_capabilities": {
                    "can_add": can_add,
                    "can_explain": True,
                },
            }
        )
    return items


def _candidate_from_followup_item(item: dict, timezone_name: str) -> Optional[ExtractedEvent]:
    capabilities = dict(item.get("action_capabilities") or {})
    if capabilities and not capabilities.get("can_add"):
        return None
    title = str(item.get("title") or item.get("text") or "").strip()
    if not title:
        return None
    start_at = _coerce_datetime(item.get("start_at") or item.get("date_sort_key"))
    end_at = _coerce_datetime(item.get("end_at"))
    start_at, end_at, _ = _normalize_deadline_window(
        start_at,
        end_at,
        timezone_name,
        title=title,
        reason=str(item.get("reason") or ""),
        force_all_day=bool(item.get("all_day")),
    )
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
    created = _create_event_from_candidate_result(
        db=db,
        request_id=request_id,
        user=user,
        source=source,
        provider_message_id=provider_message_id,
        candidate=candidate,
    )
    status = str(created.get("status") or "command_needs_clarification")
    if status == "command_completed":
        validation_payload = {"valid": True}
        policy_outcome = {"status": "command_completed", "reason": str(created.get("reason") or "event_added")}
        committed_actions = {"event_created": created.get("title")} if created.get("mutation_executed") else {}
        template = "event_created"
        webhook_status = WebhookStatus.COMMAND_COMPLETED
    elif status == "command_noop_past_event":
        validation_payload = {"valid": False, "issues": ["event_in_past"]}
        policy_outcome = {"status": "command_noop_past_event", "reason": str(created.get("reason") or "event_in_past")}
        committed_actions = {}
        template = "add_past_event"
        webhook_status = WebhookStatus.COMMAND_NOOP_PAST_EVENT
    else:
        validation_payload = {"valid": False, "issues": list(created.get("issues") or ["no_actionable_event"])}
        policy_outcome = {"status": "command_needs_clarification", "reason": str(created.get("reason") or "no_actionable_event")}
        committed_actions = {}
        template = "add_clarification"
        webhook_status = _tool_status_to_webhook(status)

    _audit(
        db,
        request_id,
        user.household_id,
        "info",
        inputs,
        model_output,
        validation_payload,
        policy_outcome,
        committed_actions,
    )
    _mark_receipt_processed(receipt)
    db.commit()
    return _command_reply(
        db=db,
        user=user,
        channel=response_channel,
        template=template,
        subject=response_subject,
        status=webhook_status,
        message=str(created.get("message") or "I couldn't add that event right now."),
        request_id=request_id,
        mutation_executed=bool(created.get("mutation_executed")),
    )


def _create_event_from_candidate_result(
    *,
    db: Session,
    request_id: str,
    user: User,
    source: SourceMessage,
    provider_message_id: str,
    candidate: ExtractedEvent,
    all_day_override: Optional[bool] = None,
) -> dict[str, Any]:
    validation = validate_candidate(candidate)
    if not validation["valid"]:
        issues = set(validation.get("issues") or [])
        if "event_in_past" in issues and not (issues - {"event_in_past", "low_confidence"}):
            return {
                "ok": False,
                "status": "command_noop_past_event",
                "message": _build_past_event_message([candidate], user.timezone),
                "mutation_executed": False,
                "reason": "event_in_past",
                "issues": validation.get("issues") or ["event_in_past"],
                "title": candidate.title,
            }
        return {
            "ok": False,
            "status": "command_needs_clarification",
            "message": "I couldn't find a clear future event to add. Please include the event name and date.",
            "mutation_executed": False,
            "reason": "no_actionable_event",
            "issues": validation.get("issues") or ["no_actionable_event"],
            "title": candidate.title,
        }

    idem_key = f"{provider_message_id}:add:{candidate.title}:{candidate.start_at}:{candidate.end_at}"
    existing_idem = db.scalar(
        select(IdempotencyKey).where(
            IdempotencyKey.key == idem_key,
            IdempotencyKey.scope == "command",
            IdempotencyKey.household_id == user.household_id,
        )
    )
    if existing_idem:
        return {
            "ok": True,
            "status": "command_completed",
            "message": "Command already processed",
            "mutation_executed": False,
            "reason": "already_processed",
            "title": candidate.title,
        }

    gate_response = _tenant_gate_for_calendar_mutation(db, request_id, user.household_id, user.household_id)
    if gate_response:
        return {
            "ok": False,
            "status": str(gate_response.status),
            "message": str(gate_response.message),
            "mutation_executed": False,
            "reason": "tenant_gate_blocked",
            "title": candidate.title,
        }

    all_day = _is_all_day_window(candidate.start_at, candidate.end_at, user.timezone) if all_day_override is None else bool(all_day_override)
    try:
        credential, binding = _resolve_calendar_context_with_refresh(db, user.household_id)
        calendar_result = calendar_provider.create_event(
            access_token=credential.access_token,
            calendar_id=binding.calendar_id,
            title=candidate.title,
            start_at=candidate.start_at,
            end_at=candidate.end_at,
            timezone=user.timezone,
            all_day=all_day,
        )
        event = Event(
            household_id=user.household_id,
            source_message_id=source.id,
            title=candidate.title,
            start_at=candidate.start_at,
            end_at=candidate.end_at,
            timezone=user.timezone,
            all_day=all_day,
            status="calendar_synced",
            calendar_event_id=calendar_result.calendar_event_id,
        )
        db.add(event)
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
        db.flush()
    except CalendarMutationError:
        return {
            "ok": False,
            "status": "command_needs_clarification",
            "message": "I found the event, but I couldn't add it to the calendar right now. Please try again.",
            "mutation_executed": False,
            "reason": "calendar_write_failed",
            "title": candidate.title,
        }

    return {
        "ok": True,
        "status": "command_completed",
        "message": "Added to calendar.",
        "mutation_executed": True,
        "reason": "event_added",
        "event_id": event.id,
        "calendar_event_id": event.calendar_event_id,
        "title": event.title,
    }


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
    command_parse_metadata: Optional[dict] = None,
    forwarded_subject: str = "",
    forwarded_date: str = "",
) -> InboundResponse:
    content_body_text = analysis_body_text or raw_body_text
    priority_preferences = load_priority_preferences(db, user.household_id)
    reference_datetime_hint = _serialize_dt(_parse_forwarded_reference_datetime(forwarded_date, user.timezone)) or ""
    extraction_result = extract_context_documents(
        content_body_text=content_body_text,
        reference_datetime_hint=reference_datetime_hint,
    )
    result = resolve_add_request_from_context(
        raw_body_text=raw_body_text,
        subject=subject,
        timezone_name=user.timezone,
        response_channel=response_channel,
        command_topic=command_topic,
        followup_context=followup_context,
        forwarded_subject=forwarded_subject,
        forwarded_date=forwarded_date,
        preference_text=priority_preferences["raw_text"],
        extraction_result=extraction_result,
        fallback_command_topic_fn=_fallback_command_topic,
        extract_direct_add_candidate_fn=_extract_direct_add_candidate,
        candidate_from_followup_item_fn=_candidate_from_followup_item,
        resolve_forwarded_add_candidates_fn=_resolve_forwarded_add_candidates,
        collect_extraction_results_fn=_collect_extraction_results,
        validate_candidate_fn=validate_candidate,
        serialize_dt_fn=_serialize_dt,
        build_candidate_clarification_fn=_build_candidate_clarification,
        build_past_event_message_fn=_build_past_event_message,
        past_only_candidates_fn=_past_only_candidates,
        allows_multiple_add_fn=_allows_multiple_add,
        create_candidate_event_fn=lambda candidate: _create_event_from_candidate_result(
            db=db,
            request_id=request_id,
            user=user,
            source=source,
            provider_message_id=provider_message_id,
            candidate=candidate,
        ),
    )

    inputs = {
        "subject": subject,
        "body_text": raw_body_text,
        "command_topic": (command_topic or "").strip() or _fallback_command_topic(raw_body_text) or None,
        "response_channel": response_channel,
    }
    audit_model_output = {
        "llm": engine_llm.metadata(),
        "command_parse": command_parse_metadata or None,
    }
    audit_model_output.update(result.audit_payload)

    template = "event_created"
    if result.status == "command_needs_clarification":
        template = "add_clarification"
    elif result.status == "command_noop_past_event":
        template = "add_past_event"

    _audit(
        db,
        request_id,
        user.household_id,
        "info",
        inputs,
        audit_model_output,
        result.audit_validation,
        result.audit_policy_outcome,
        result.audit_committed_actions,
    )
    _mark_receipt_processed(receipt)
    db.commit()
    return _command_reply(
        db=db,
        user=user,
        channel=response_channel,
        template=template,
        subject=response_subject,
        status=_tool_status_to_webhook(result.status),
        message=result.message,
        request_id=request_id,
        mutation_executed=result.mutation_executed,
    )


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _build_oauth_state(household_id: int = 1, next_url: str | None = None) -> str:
    payload: dict = {"household_id": household_id, "nonce": str(uuid.uuid4())}
    if next_url and next_url.startswith("/"):
        payload["next_url"] = next_url
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

    attribution = resolve_admin_sender(db, payload.sender)
    receipt = WebhookReceipt(
        provider=payload.provider,
        provider_event_id=payload.provider_event_id,
        provider_message_id=payload.provider_message_id,
        status="received",
    )
    db.add(receipt)
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

    email_intent = _classify_email_intent(payload.body_text)
    content_body_text = email_intent.forwarded_body_text or payload.body_text
    candidate_links = extract_candidate_links(content_body_text)
    link_report = resolve_and_download_links(candidate_links)
    resend_attachments = (
        _download_resend_pdf_attachments(payload.email_id)
        if payload.provider == "resend" and payload.email_id
        else []
    )
    analysis_attachments = list(link_report.attachments) + list(resend_attachments)
    thread_key = str(payload.thread_key or "").strip() or resolve_email_thread_key(
        db,
        household_id=user.household_id,
        internet_message_id=payload.internet_message_id or payload.provider_message_id,
        in_reply_to_message_id=payload.in_reply_to_message_id,
        references_header=payload.references_header,
        fallback_key=payload.provider_message_id,
    )
    source = SourceMessage(
        provider=payload.provider,
        provider_message_id=payload.provider_message_id,
        source_channel="email",
        sender=payload.sender.lower(),
        household_id=user.household_id,
        subject=payload.subject,
        body_text=payload.body_text,
        internet_message_id=(payload.internet_message_id or payload.provider_message_id).strip() or None,
        in_reply_to_message_id=(payload.in_reply_to_message_id or "").strip() or None,
        references_header=(payload.references_header or "").strip(),
        thread_key=thread_key,
    )
    db.add(source)
    db.flush()
    db.commit()
    if analysis_attachments:
        persist_thread_documents(
            db,
            household_id=user.household_id,
            source_message_id=source.id,
            thread_key=source.thread_key,
            attachments=analysis_attachments,
            openai_api_key=settings.openai_api_key,
            openai_base_url=settings.openai_base_url,
            openai_timeout_sec=settings.openai_timeout_sec,
        )
        db.commit()
    thread_documents = load_thread_documents(db, household_id=user.household_id, thread_key=source.thread_key)
    household = db.scalar(select(Household).where(Household.id == user.household_id))
    priority_preferences = load_priority_preferences(db, user.household_id)
    preference_text = priority_preferences["raw_text"]
    children = db.scalars(select(Child).where(Child.household_id == user.household_id, Child.status == "active")).all()
    agent_household_context = _build_agent_household_context(
        db,
        user=user,
        household=household,
        children=children,
        priority_preferences=priority_preferences,
    )
    db.commit()

    parsed_email_command = None
    forwarded_preface_intent = None
    command_body_text = email_intent.user_preface_text
    with _conversation_runtime_scope(
        source=source,
        thread_documents=thread_documents,
        household_context=agent_household_context,
    ):
        if email_intent.mode == "command_candidate":
            parse_source_text = email_intent.user_preface_text or payload.body_text
            try:
                parsed_email_command = engine_llm.parse_command(parse_source_text)
            except Exception:
                parsed_email_command = None
        elif email_intent.mode == "forwarded_preface_candidate":
            parse_source_text = email_intent.user_preface_text or ""
            if parse_source_text:
                try:
                    parsed_email_command = engine_llm.parse_command(parse_source_text)
                except Exception:
                    parsed_email_command = None
            try:
                forwarded_preface_intent = engine_llm.parse_forwarded_preface_intent(
                    user_preface=email_intent.user_preface_text,
                    forwarded_subject=email_intent.forwarded_subject,
                    forwarded_sender=email_intent.forwarded_sender,
                    forwarded_date=email_intent.forwarded_date,
                )
            except Exception as exc:
                if parsed_email_command is not None and _should_accept_plain_email_command(parsed_email_command):
                    forwarded_preface_intent = None
                else:
                    response_channel = resolve_response_channel(
                        origin_channel="email",
                        email_intent_mode="command",
                        admin_phone=user.phone,
                    )
                    response_subject = _reply_subject(payload.subject)
                    _audit(
                        db,
                        request_id,
                        user.household_id,
                        "high",
                        payload.model_dump(),
                        {
                            "llm": engine_llm.metadata(),
                            "email_intent": _email_intent_metadata(email_intent),
                            "conversation": {
                                "session_id": _source_session_id(source),
                                "thread_key": source.thread_key,
                                "thread_document_count": len(thread_documents),
                            },
                        },
                        {"valid": False, "issues": ["llm_forwarded_intent_parse_error"]},
                        {"status": "command_parse_error", "detail": exc.__class__.__name__},
                        {},
                    )
                    _mark_receipt_processed(receipt)
                    return _command_reply(
                        db=db,
                        user=user,
                        channel=response_channel,
                        template="command_clarification",
                        subject=response_subject,
                        status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                        message="I saw your note above the forwarded email, but I couldn't interpret the request. Reply with what you'd like me to do.",
                        request_id=request_id,
                        mutation_executed=False,
                    )

    if (
        email_intent.mode == "forwarded_preface_candidate"
        and forwarded_preface_intent is not None
        and forwarded_preface_intent.get("mode") == "clarification"
        and float(forwarded_preface_intent.get("confidence") or 0.0) >= _FORWARDED_CLARIFICATION_MIN_CONFIDENCE
    ):
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            payload.model_dump(),
            {
                "llm": engine_llm.metadata(),
                "email_intent": _email_intent_metadata(email_intent),
                "forwarded_preface_intent": forwarded_preface_intent,
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

    prefer_direct_preface_command = (
        email_intent.mode == "forwarded_preface_candidate"
        and _should_prefer_direct_preface_command(parsed_email_command)
    )

    should_run_command = (
        email_intent.mode == "command_candidate"
        and parsed_email_command is not None
        and _should_accept_plain_email_command(parsed_email_command)
    ) or (
        prefer_direct_preface_command
    ) or (
        email_intent.mode == "forwarded_preface_candidate"
        and forwarded_preface_intent is not None
        and forwarded_preface_intent.get("mode") == "command"
        and _should_accept_forwarded_command(forwarded_preface_intent)
    )

    if should_run_command:
        with _conversation_runtime_scope(
            source=source,
            thread_documents=thread_documents,
            household_context=agent_household_context,
        ):
            response_channel = resolve_response_channel(
                origin_channel="email",
                email_intent_mode="command",
                admin_phone=user.phone,
            )
            response_subject = _reply_subject(payload.subject)
            if email_intent.mode == "command_candidate" or prefer_direct_preface_command:
                command = parsed_email_command or {}
            else:
                command = forwarded_preface_intent or parsed_email_command or {}
            strategy = _command_execution_strategy(command)

            if command.get("action") in {"more_info", "update", "delete", "remind", "set_preference"} or (
                command.get("action") == "add" and email_intent.mode == "command_candidate"
            ):
                tool_response = _run_command_tools(
                    db=db,
                    request_id=request_id,
                    user=user,
                    source=source,
                    receipt=receipt,
                    response_channel=response_channel,
                    response_subject=response_subject,
                    provider_message_id=payload.provider_message_id,
                    subject=payload.subject,
                    raw_message_text=command_body_text or payload.body_text,
                    analysis_message_text=email_intent.forwarded_body_text or payload.body_text,
                    forwarded_subject=email_intent.forwarded_subject,
                    forwarded_date=email_intent.forwarded_date,
                    command=command,
                    allow_add_fallback=command.get("action") == "add",
                )
                if tool_response is not None:
                    return tool_response

            if strategy == "deterministic" and command["action"] == "add":
                followup_context = load_active_followup_context(
                    db,
                    household_id=user.household_id,
                    response_channel=response_channel,
                    thread_or_conversation_key=_followup_context_key(source),
                )
                return _handle_add_command(
                    db=db,
                    request_id=request_id,
                    user=user,
                    source=source,
                    receipt=receipt,
                    provider_message_id=payload.provider_message_id,
                    subject=payload.subject,
                    raw_body_text=command_body_text or payload.body_text,
                    analysis_body_text=email_intent.forwarded_body_text or payload.body_text,
                    response_channel=response_channel,
                    response_subject=response_subject,
                    followup_context=followup_context,
                    command_topic=command.get("topic"),
                    command_parse_metadata=command,
                    forwarded_subject=email_intent.forwarded_subject,
                    forwarded_date=email_intent.forwarded_date,
                )
            if strategy == "semantic" and command["action"] == "more_info":
                context = load_active_followup_context(
                    db,
                    household_id=user.household_id,
                    response_channel=response_channel,
                    thread_or_conversation_key=_followup_context_key(source),
                )
                _mark_receipt_processed(receipt)
                db.commit()
                more_info = _handle_more_info_command(
                    db=db,
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

            if strategy == "deterministic" and command["action"] == "set_preference":
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

            if strategy == "deterministic" and command["action"] == "delete":
                event_id = command["event_id"]
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

            if strategy == "deterministic" and command["action"] == "remind":
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
                message="I understood the request, but this command needs a different follow-up flow.",
                request_id=request_id,
                mutation_executed=False,
            )

    analysis_text = build_analysis_text(content_body_text, analysis_attachments)
    if not analysis_text:
        analysis_text = content_body_text
    reference_datetime_hint = _serialize_dt(_parse_forwarded_reference_datetime(email_intent.forwarded_date, user.timezone)) or ""

    # Complexity routing: fast path (unified extraction) vs thorough path (multi-pass)
    use_fast_path = (
        hasattr(engine_llm, "unified_extract")
        and len(analysis_text) <= getattr(settings, "unified_extraction_char_limit", 30000)
        and len(analysis_attachments) <= getattr(settings, "unified_extraction_max_attachments", 1)
    )

    document_understanding = None
    extracted_events: list = []
    sections: list = []
    chunk_summaries: list = []
    chunk_notes: list[str] = []
    chunk_failures: list = []
    section_summaries: list = []
    used_unified_extraction = False

    if use_fast_path:
        # Fast path: single LLM call replaces understand_document + chunking + extract_events
        with _conversation_runtime_scope(
            source=source,
            thread_documents=thread_documents,
            household_context=agent_household_context,
        ):
            try:
                unified_result = engine_llm.unified_extract(
                    analysis_text=analysis_text,
                    subject=payload.subject,
                    household_preferences=preference_text,
                    timezone_hint=user.timezone,
                    reference_datetime_hint=reference_datetime_hint,
                    forwarded_subject=email_intent.forwarded_subject,
                    forwarded_sender=email_intent.forwarded_sender,
                    forwarded_date=email_intent.forwarded_date,
                )
                extracted_events = unified_result["events"]
                document_understanding = unified_result["document_understanding"]
                chunk_notes = [unified_result["email_level_notes"]] if unified_result.get("email_level_notes") else []
                used_unified_extraction = True
            except Exception:
                # Fall back to thorough path on failure
                use_fast_path = False

    if not use_fast_path:
        # Thorough path: separate understand_document + chunked extraction
        with _conversation_runtime_scope(
            source=source,
            thread_documents=thread_documents,
            household_context=agent_household_context,
        ):
            try:
                document_understanding = engine_llm.understand_document(
                    analysis_text=analysis_text,
                    subject=payload.subject,
                    household_preferences=preference_text,
                    timezone_hint=user.timezone,
                    reference_datetime_hint=reference_datetime_hint,
                    forwarded_subject=email_intent.forwarded_subject,
                    forwarded_sender=email_intent.forwarded_sender,
                    forwarded_date=email_intent.forwarded_date,
                )
            except Exception:
                document_understanding = None
        sections, prioritized_chunks = build_prioritized_chunks(
            content_body_text,
            analysis_attachments,
            priority_terms=_document_understanding_priority_terms(document_understanding),
        )
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

    sender_email_hint, sender_name_hint = _normalized_sender_identity(email_intent.forwarded_sender or payload.sender)
    _append_user_turn_to_session(db, source)
    db.commit()

    if not use_fast_path:
        with _conversation_runtime_scope(
            source=source,
            thread_documents=thread_documents,
            household_context=agent_household_context,
        ):
            extracted_events, chunk_summaries, chunk_notes, chunk_failures = _collect_extraction_results(
                prioritized_chunks,
                payload.subject,
                preference_text,
                user.timezone,
                reference_datetime_hint=reference_datetime_hint,
                document_understanding=document_understanding,
            )

    analysis_audit = {
        "links": candidate_links,
        "link_attempts": [attempt.__dict__ for attempt in link_report.attempts],
        "attachment_count": len(analysis_attachments),
        "analysis_char_count": len(analysis_text),
        "extraction_path": "unified" if used_unified_extraction else "thorough",
        "document_understanding": document_understanding,
        "section_summaries": section_summaries,
        "chunk_summaries": chunk_summaries,
        "chunk_failures": chunk_failures,
    }
    if not extracted_events:
        with _conversation_runtime_scope(
            source=source,
            thread_documents=thread_documents,
            household_context=agent_household_context,
        ):
            summary_result, summary_audit = build_brief_summary(
                engine=engine_llm,
                subject=payload.subject,
                timezone_name=user.timezone,
                household_preferences=preference_text,
                system_defaults={item["key"]: bool(item["enabled"]) for item in priority_preferences["system_defaults"]},
                user_priority_topics=list(priority_preferences["user_priority_topics"]),
                suppressed_priority_topics=list(priority_preferences["effective_suppressed_priority_topics"]),
                children=children,
                extracted_events=[],
                per_event_outcomes=[],
                sections=sections,
                analysis_text=analysis_text,
                chunk_notes=chunk_notes,
                informational_only=None,
                reference_datetime_hint=reference_datetime_hint,
                document_understanding=document_understanding,
                skip_candidate_extraction=used_unified_extraction,
            )
        if _summary_has_content(summary_result):
            validator_issues = ["empty_extraction_informational_fallback"]
            if chunk_failures:
                validator_issues.insert(0, "llm_extraction_error")
            response_channel = resolve_response_channel(
                origin_channel="email",
                email_intent_mode=email_intent.mode,
                admin_phone=user.phone,
            )
            db.add(
                InformationalItem(
                    household_id=user.household_id,
                    source_message_id=source.id,
                    title=summary_result.title or _display_subject(payload.subject),
                    details=summary_result.rendered_message,
                    priority=0,
                    status="stored",
                )
            )
            _mark_receipt_processed(receipt)
            document_followup_items = _document_topics_to_followup_items(
                document_understanding,
                source_message_id=source.id,
                user_priority_topics=list(priority_preferences["user_priority_topics"]),
                suppressed_priority_topics=list(priority_preferences["effective_suppressed_priority_topics"]),
            )
            persist_followup_context(
                db,
                household_id=user.household_id,
                source_message_id=source.id,
                origin_channel="email",
                response_channel=response_channel,
                thread_or_conversation_key=_followup_context_key(source),
                summary_title=summary_result.title,
                summary_items_shown=_followup_summary_items(summary_result, document_followup_items),
                actionable_items=document_followup_items,
                section_snippets=(
                    [
                        {
                            "label": item.get("label"),
                            "text": item.get("text"),
                        }
                        for item in list(summary_audit.get("prefilter", {}).get("kept_sections") or [])
                        if item.get("text")
                    ]
                    + _document_understanding_section_snippets(document_understanding)
                ),
            )
            _audit(
                db,
                request_id,
                user.household_id,
                "info",
                payload.model_dump(),
                {
                    "llm": engine_llm.metadata(),
                    "email_intent": _email_intent_metadata(email_intent),
                    "forwarded_preface_intent": forwarded_preface_intent,
                    "conversation": {
                        "session_id": _source_session_id(source),
                        "thread_key": source.thread_key,
                        "thread_document_count": len(thread_documents),
                    },
                    "analysis": analysis_audit,
                    "summary": summary_audit,
                },
                {"valid": True, "issues": validator_issues},
                {"status": "processed", "counts": {"event_created": 0, "followup_available": 0, "info_stored": 1}},
                {"informational_fallback": "stored"},
            )
            with _conversation_runtime_scope(
                source=source,
                thread_documents=thread_documents,
                household_context=agent_household_context,
            ):
                _commit_then_send_user_response(
                    db=db,
                    user=user,
                    channel=response_channel,
                    template="email_analysis_recap",
                    subject=f"LovelyChaos: {_display_subject(payload.subject)}",
                    message=summary_result.rendered_message,
                )
            return InboundResponse(
                status=WebhookStatus.INGESTION_ACCEPTED.value,
                message="Relevant school updates were summarized for follow-up.",
                request_id=request_id,
                mutation_executed=False,
                processing_state=ProcessingState.COMPLETED.value,
            )
        _audit(
            db,
            request_id,
            user.household_id,
            "info",
            payload.model_dump(),
            {
                "llm": engine_llm.metadata(),
                "email_intent": _email_intent_metadata(email_intent),
                "forwarded_preface_intent": forwarded_preface_intent,
                "analysis": analysis_audit,
            },
            {"valid": False, "issues": ["empty_extraction"]},
            {"status": "rejected_validation"},
            {},
        )
        _mark_receipt_processed(receipt)
        db.commit()
        return _safe_error(WebhookStatus.REJECTED_VALIDATION, request_id, "Could not validate event details.")

    mutation_executed = False
    outcome_counts = {"event_created": 0, "followup_available": 0, "info_stored": 0}
    per_event_outcomes: list[dict] = []
    summary_result = None
    summary_audit = None
    evaluation_datetime_utc = _serialize_dt(datetime.now(timezone.utc)) or ""
    with _conversation_runtime_scope(
        source=source,
        thread_documents=thread_documents,
        household_context=agent_household_context,
    ):
        routing_decisions = _route_extracted_events(
            extracted_events=extracted_events,
            children=children,
            priority_preferences=priority_preferences,
            sender_email_hint=sender_email_hint,
            sender_name_hint=sender_name_hint,
            timezone_hint=user.timezone,
            evaluation_datetime_utc=evaluation_datetime_utc,
            document_understanding=document_understanding,
        )
    any_valid = any(bool(item.get("validation", {}).get("valid")) for item in routing_decisions)
    has_relevant_event = any(
        _relevancy_payload_is_relevant(dict(item.get("relevancy_evidence") or {}))
        for item in routing_decisions
    )

    for idx, candidate in enumerate(extracted_events, start=1):
        route_decision = routing_decisions[idx - 1]
        validation = dict(route_decision.get("validation") or {})
        relevancy_payload = dict(route_decision.get("relevancy_evidence") or {})
        suppressed_match = bool(route_decision.get("suppressed_match"))
        auto_add_decision = dict(route_decision.get("auto_add_decision") or {})
        execution_disposition = str(route_decision.get("execution_disposition") or "").strip()
        final_reason = str(route_decision.get("final_reason") or "not_relevant").strip()
        is_relevant = _relevancy_payload_is_relevant(relevancy_payload)
        target_scope = (candidate.target_scope or "unknown").strip()
        is_school_global = target_scope == "school_global"

        idempotency_key = None
        if execution_disposition in {"create_event", "informational_item"}:
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
                    "relevancy_evidence": relevancy_payload,
                    "suppressed_match": suppressed_match,
                    "auto_add_decision": auto_add_decision,
                    "execution_disposition": execution_disposition or None,
                    "final_reason": final_reason,
                    "action": {"idempotent_skip": True},
                }
            )
            if execution_disposition == "create_event":
                outcome_counts["event_created"] += 1
            elif execution_disposition == "informational_item":
                outcome_counts["info_stored"] += 1
            elif execution_disposition == "followup_available":
                outcome_counts["followup_available"] += 1
            continue

        action: dict = {}
        if execution_disposition == "create_event":
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
                all_day = _is_all_day_window(candidate.start_at, candidate.end_at, user.timezone)
                calendar_result = calendar_provider.create_event(
                    access_token=credential.access_token,
                    calendar_id=binding.calendar_id,
                    title=candidate.title,
                    start_at=candidate.start_at,
                    end_at=candidate.end_at,
                    timezone=user.timezone,
                    all_day=all_day,
                )
                db.add(
                    Event(
                        household_id=user.household_id,
                        source_message_id=source.id,
                        title=candidate.title,
                        start_at=candidate.start_at,
                        end_at=candidate.end_at,
                        timezone=user.timezone,
                        all_day=all_day,
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
                    channel=resolve_response_channel(origin_channel="email"),
                )
            except CalendarMutationError:
                action = {"event": "not_created", "calendar_sync": "failed"}
                final_reason = "calendar_write_failed_followup_available"
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
        elif execution_disposition == "followup_available":
            action = {"followup_available": True}
        else:
            action = {"followup_available": bool(is_relevant), "ignored": not is_relevant}

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
        if action.get("event") == "created":
            outcome_counts["event_created"] += 1
        elif action.get("informational") == "stored":
            outcome_counts["info_stored"] += 1
        elif execution_disposition == "followup_available":
            outcome_counts["followup_available"] += 1
        per_event_outcomes.append(
            {
                "index": idx,
                    "title": candidate.title,
                    "start_at": _serialize_dt(candidate.start_at),
                    "end_at": _serialize_dt(candidate.end_at),
                    "validation": validation,
                    "relevancy_evidence": relevancy_payload,
                    "suppressed_match": suppressed_match,
                    "auto_add_decision": auto_add_decision,
                    "execution_disposition": execution_disposition or None,
                    "final_reason": final_reason,
                    "model_reason": candidate.model_reason,
                    "action": action,
                }
        )

    with _conversation_runtime_scope(
        source=source,
        thread_documents=thread_documents,
        household_context=agent_household_context,
    ):
        summary_result, summary_audit = build_brief_summary(
            engine=engine_llm,
            subject=payload.subject,
            timezone_name=user.timezone,
            household_preferences=preference_text,
            system_defaults={item["key"]: bool(item["enabled"]) for item in priority_preferences["system_defaults"]},
            user_priority_topics=list(priority_preferences["user_priority_topics"]),
            suppressed_priority_topics=list(priority_preferences["effective_suppressed_priority_topics"]),
            children=children,
            extracted_events=extracted_events,
            per_event_outcomes=per_event_outcomes,
            sections=sections,
            analysis_text=analysis_text,
            chunk_notes=chunk_notes,
            informational_only=outcome_counts["followup_available"] == 0 and outcome_counts["event_created"] == 0,
            reference_datetime_hint=reference_datetime_hint,
            document_understanding=document_understanding,
            skip_candidate_extraction=used_unified_extraction,
        )
    response_channel = resolve_response_channel(
        origin_channel="email",
        email_intent_mode=email_intent.mode,
        admin_phone=user.phone,
    )
    actionable_items = _followup_actionable_items_from_extracted_events(
        extracted_events=extracted_events,
        source_message_id=source.id,
        timezone_name=user.timezone,
    )
    actionable_items.extend(
        _document_topics_to_followup_items(
            document_understanding,
            source_message_id=source.id,
            user_priority_topics=list(priority_preferences["user_priority_topics"]),
            suppressed_priority_topics=list(priority_preferences["effective_suppressed_priority_topics"]),
        )
    )
    summary_items_shown = _followup_summary_items(summary_result, actionable_items)
    model_output = {
        "llm": engine_llm.metadata(),
        "email_intent": _email_intent_metadata(email_intent),
        "forwarded_preface_intent": forwarded_preface_intent,
        "conversation": {
            "session_id": _source_session_id(source),
            "thread_key": source.thread_key,
            "thread_document_count": len(thread_documents),
        },
        "events": [{"title": e.title, "confidence": e.confidence} for e in extracted_events],
        "document_understanding": document_understanding,
        "routing": routing_decisions,
        "email_level_notes": "\n".join(chunk_notes) if chunk_notes else None,
        "analysis": analysis_audit,
        "summary": summary_audit,
    }

    if not any_valid and has_relevant_event:
        _mark_receipt_processed(receipt)
        persist_followup_context(
            db,
            household_id=user.household_id,
            source_message_id=source.id,
            origin_channel="email",
            response_channel=response_channel,
            thread_or_conversation_key=_followup_context_key(source),
            summary_title=summary_result.title,
            summary_items_shown=summary_items_shown,
            actionable_items=actionable_items,
            section_snippets=(
                [
                    {
                        "label": item.get("label"),
                        "text": item.get("text"),
                    }
                    for item in list(summary_audit.get("prefilter", {}).get("kept_sections") or [])
                    if item.get("text")
                ]
                + _document_understanding_section_snippets(document_understanding)
            ),
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
        with _conversation_runtime_scope(
            source=source,
            thread_documents=thread_documents,
            household_context=agent_household_context,
        ):
            _commit_then_send_user_response(
                db=db,
                user=user,
                channel=response_channel,
                template="email_analysis_recap",
                subject=f"LovelyChaos: {_display_subject(payload.subject)}",
                message=summary_result.rendered_message,
            )
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
    persist_followup_context(
        db,
        household_id=user.household_id,
        source_message_id=source.id,
        origin_channel="email",
        response_channel=response_channel,
        thread_or_conversation_key=_followup_context_key(source),
        summary_title=summary_result.title,
        summary_items_shown=summary_items_shown,
        actionable_items=actionable_items,
        section_snippets=(
            [
                {
                    "label": item.get("label"),
                    "text": item.get("text"),
                }
                for item in list(summary_audit.get("prefilter", {}).get("kept_sections") or [])
                if item.get("text")
            ]
            + _document_understanding_section_snippets(document_understanding)
        ),
    )
    with _conversation_runtime_scope(
        source=source,
        thread_documents=thread_documents,
        household_context=agent_household_context,
    ):
        _commit_then_send_user_response(
            db=db,
            user=user,
            channel=response_channel,
            template="email_analysis_recap",
            subject=f"LovelyChaos: {_display_subject(payload.subject)}",
            message=summary_result.rendered_message,
        )
    return InboundResponse(
        status=WebhookStatus.INGESTION_ACCEPTED.value,
        message=(
            f"Processed {len(extracted_events)} item(s): "
            f"{outcome_counts['event_created']} calendar updates, "
            f"{outcome_counts['info_stored']} stored informational topics."
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


def _retrieve_resend_received_attachments(email_id: str) -> list[dict]:
    if not email_id or not settings.resend_api_key:
        return []
    try:
        with httpx.Client(timeout=10) as client:
            response = client.get(
                f"https://api.resend.com/emails/receiving/{email_id}/attachments",
                headers={
                    "Authorization": f"Bearer {settings.resend_api_key}",
                    "Content-Type": "application/json",
                },
            )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _download_resend_pdf_attachments(email_id: str) -> list[DownloadedAttachment]:
    attachments: list[DownloadedAttachment] = []
    for item in _retrieve_resend_received_attachments(email_id):
        filename = str(item.get("filename") or item.get("name") or "attachment.pdf").strip() or "attachment.pdf"
        content_type = str(item.get("content_type") or item.get("contentType") or "").strip().lower()
        download_url = str(item.get("download_url") or item.get("downloadUrl") or item.get("url") or "").strip()
        if "pdf" not in content_type and not filename.lower().endswith(".pdf"):
            continue
        if not download_url:
            continue
        try:
            with httpx.Client(timeout=20, follow_redirects=True) as client:
                response = client.get(download_url)
            response.raise_for_status()
        except Exception:
            continue
        attachment_content_type = (response.headers.get("content-type") or content_type or "application/pdf").split(";")[0].strip().lower()
        attachments.append(
            DownloadedAttachment(
                filename=filename,
                content_type=attachment_content_type,
                content=response.content,
                source_url=download_url,
                status_reason="downloaded_via_resend_attachments_api",
                extracted_text=maybe_extract_pdf_text(response.content),
            )
        )
    return attachments


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
    email_id = str(data.get("email_id") or data.get("emailId") or "")
    fetched = _retrieve_resend_received_email(email_id) if email_id else {}
    effective = fetched if isinstance(fetched, dict) and fetched else data
    sender = _extract_email_address(str(data.get("from") or data.get("from_email") or ""))
    recipient_alias = _extract_email_address(
        _first_recipient(
            effective.get("to")
            or effective.get("to_email")
            or effective.get("recipients")
            or effective.get("delivered_to")
            or payload.get("to")
            or payload.get("recipient")
        )
    )
    subject = str(effective.get("subject") or data.get("subject") or "")
    body_text = _extract_best_body_text(effective, payload)
    headers = effective.get("headers") if isinstance(effective.get("headers"), (dict, list)) else data.get("headers")
    internet_message_id = str(
        effective.get("message_id")
        or effective.get("messageId")
        or extract_header_value(headers, "Message-ID")
        or provider_message_id
    ).strip()
    in_reply_to_message_id = str(
        effective.get("in_reply_to")
        or effective.get("inReplyTo")
        or extract_header_value(headers, "In-Reply-To")
    ).strip()
    references_header = str(
        effective.get("references")
        or effective.get("references_header")
        or extract_header_value(headers, "References")
    ).strip()

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
        email_id=email_id,
        internet_message_id=internet_message_id,
        in_reply_to_message_id=in_reply_to_message_id,
        references_header=references_header,
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
    def _run_sync_inbound() -> InboundResponse:
        with Session(bind=engine) as thread_db:
            return inbound_email(payload=inbound, x_signature=settings.webhook_secret, db=thread_db)

    return await run_in_threadpool(_run_sync_inbound)


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
        thread_key=sms_session_id(household_id=user.household_id),
    )
    db.add(source)
    db.flush()
    active_sms_state = load_active_sms_conversation_state(db, household_id=user.household_id, channel="sms")
    household = db.scalar(select(Household).where(Household.id == user.household_id))
    priority_preferences = load_priority_preferences(db, user.household_id)
    children = db.scalars(select(Child).where(Child.household_id == user.household_id, Child.status == "active")).all()
    agent_household_context = _build_agent_household_context(
        db,
        user=user,
        household=household,
        children=children,
        priority_preferences=priority_preferences,
    )
    db.commit()
    with _conversation_runtime_scope(
        source=source,
        thread_documents=[],
        household_context=agent_household_context,
    ):
        try:
            command = engine_llm.parse_command(payload.body_text)
        except Exception:
            fallback_command = {
                "action": "none",
                "topic": None,
            }
            contextual_response = _try_contextual_sms_assistant(
                db=db,
                request_id=request_id,
                user=user,
                source=source,
                receipt=receipt,
                provider_message_id=payload.provider_message_id,
                raw_body_text=payload.body_text,
                command=fallback_command,
                active_state=active_sms_state,
            )
            if contextual_response is not None:
                return contextual_response
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
        contextual_response = _try_contextual_sms_assistant(
            db=db,
            request_id=request_id,
            user=user,
            source=source,
            receipt=receipt,
            provider_message_id=payload.provider_message_id,
            raw_body_text=payload.body_text,
            command=command,
            active_state=active_sms_state,
        )
        if contextual_response is not None:
            return contextual_response
        if command["action"] not in {"delete", "add", "more_info", "update", "remind", "set_preference"}:
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel="sms",
                template="command_clarification",
                subject="LovelyChaos SMS",
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="I couldn't match that to a school-update action. Try 'add Mar 19 to calendar', 'tell me more about science club', 'move pizza day to Friday', or reply with the number from my last message.",
                request_id=request_id,
                mutation_executed=False,
            )

        strategy = _command_execution_strategy(command)
        if not _command_strategy_matches_action(command["action"], strategy):
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel="sms",
                template="command_clarification",
                subject="LovelyChaos SMS",
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="I understood the request, but this command needs a different follow-up flow.",
                request_id=request_id,
                mutation_executed=False,
            )

        if command.get("action") in {"more_info", "update", "delete", "remind", "set_preference", "add"}:
            tool_response = _run_command_tools(
                db=db,
                request_id=request_id,
                user=user,
                source=source,
                receipt=receipt,
                response_channel="sms",
                response_subject="LovelyChaos SMS",
                provider_message_id=payload.provider_message_id,
                subject="LovelyChaos SMS",
                raw_message_text=payload.body_text,
                analysis_message_text=payload.body_text,
                command=command,
                allow_add_fallback=command.get("action") == "add",
            )
            if tool_response is not None:
                return tool_response

        if strategy == "semantic" and command["action"] == "more_info":
            context = load_active_followup_context(
                db,
                household_id=user.household_id,
                response_channel="sms",
                thread_or_conversation_key=_followup_context_key(source),
            )
            _mark_receipt_processed(receipt)
            db.commit()
            more_info = _handle_more_info_command(
                db=db,
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

        if strategy == "deterministic" and command["action"] == "set_preference":
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

        if strategy == "deterministic" and command["action"] == "delete":
            event_id = command["event_id"]
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

        if strategy == "deterministic" and command["action"] == "remind":
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

        if strategy == "deterministic" and command["action"] == "update":
            _mark_receipt_processed(receipt)
            db.commit()
            return _command_reply(
                db=db,
                user=user,
                channel="sms",
                template="command_clarification",
                subject="LovelyChaos SMS",
                status=WebhookStatus.COMMAND_NEEDS_CLARIFICATION,
                message="I couldn't safely update that event. Please include the event name and the change you want.",
                request_id=request_id,
                mutation_executed=False,
            )

        followup_context = load_active_followup_context(
            db,
            household_id=user.household_id,
            response_channel="sms",
            thread_or_conversation_key=_followup_context_key(source),
        )
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
            command_parse_metadata=command,
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
    return {
        "status": "ok",
        "process_id": os.getpid(),
        "booted_at": APP_BOOTED_AT.isoformat(),
        "llm": engine_llm.metadata(),
    }


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
def auth_google_start(household_id: int = 1, next: str | None = None):
    if not settings.google_client_id:
        raise HTTPException(status_code=500, detail="GOOGLE_CLIENT_ID is not configured")
    state = _build_oauth_state(household_id=household_id, next_url=next)
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
            calendar_id="",
            calendar_owner_email=email,
            status="active",
        )
        db.add(binding)
    binding.google_credential_id = credential.id
    # Preserve existing calendar_id — user selects calendar in onboarding/admin
    binding.calendar_owner_email = email
    binding.status = "active"

    admin_user = db.scalar(
        select(User).where(User.household_id == household_id, User.is_admin.is_(True)).order_by(User.id.asc())
    )
    if admin_user:
        admin_user.email = email

    db.commit()
    next_url = payload.get("next_url")
    if next_url and next_url.startswith("/"):
        return RedirectResponse(next_url, status_code=303)
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
    return HouseholdProfileOut(
        household_id=household.id,
        admin_email=admin.email,
        admin_phone=admin.phone or "",
        timezone=household.timezone,
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

    admin.email = payload.admin_email.strip().lower()
    admin.phone = payload.admin_phone.strip() or None

    for extra_admin in admins[1:]:
        db.delete(extra_admin)

    _set_household_timezone(db, household.id, payload.timezone.strip() or household.timezone or "UTC")
    household.spouse_phone = None
    household.spouse_notifications_enabled = False
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
    children = db.scalars(
        select(Child).where(Child.household_id == 1, Child.status == "active").order_by(Child.id)
    ).all()
    return [
        {
            "id": c.id,
            "name": c.name,
            "school_name": c.school_name,
            "grade": c.grade,
            "teacher_contacts": _serialize_teacher_contacts(c),
        }
        for c in children
    ]


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
    db.flush()
    _replace_teacher_contacts(child, [contact.model_dump() for contact in payload.teacher_contacts])
    db.commit()
    db.refresh(child)
    response = {
        "id": child.id,
        "name": child.name,
        "school_name": child.school_name,
        "grade": child.grade,
        "teacher_contacts": _serialize_teacher_contacts(child),
    }
    if resolution:
        response.update(
            {
                "resolved_timezone": resolution["timezone"],
                "school_city": resolution.get("city", ""),
                "school_source": resolution.get("source", ""),
            }
        )
    return response


@app.delete("/admin/children/{child_id}")
def admin_delete_child(child_id: int, db: Session = Depends(get_db)):
    child = db.scalar(select(Child).where(Child.id == child_id, Child.household_id == 1))
    if not child:
        raise HTTPException(status_code=404, detail="Child not found")
    child.status = "inactive"
    db.commit()
    return {"ok": True}


@app.get("/admin/preferences")
def admin_get_preferences(db: Session = Depends(get_db)):
    return load_priority_preferences(db, 1)


def _parse_topic_lines(raw_value: str) -> list[str]:
    return [
        line.strip()
        for line in str(raw_value or "").splitlines()
        if line and line.strip()
    ]


def _parse_preference_notes(raw_text: str, db: Session | None = None) -> dict:
    preset_topics = [item["label"] for item in priority_topic_catalog()]
    try:
        owns_db = db is None
        session = db or Session(bind=engine)
        try:
            user = session.scalar(select(User).where(User.household_id == 1, User.is_admin.is_(True)).order_by(User.id.asc()))
            if user is None:
                user = session.scalar(select(User).where(User.household_id == 1).order_by(User.id.asc()))
            if user is not None:
                household = session.scalar(select(Household).where(Household.id == user.household_id))
                children = session.scalars(
                    select(Child).where(Child.household_id == user.household_id, Child.status == "active")
                ).all()
                household_context = _build_agent_household_context(
                    session,
                    user=user,
                    household=household,
                    children=children,
                )
            else:
                household_context = {
                    "timezone": "UTC",
                    "household": {"household_id": 1},
                    "preferences": {"raw_text": raw_text},
                }
            with engine_llm.conversation_scope(
                workflow_name="LovelyChaos admin preferences",
                household_context=household_context,
                use_session=False,
            ):
                parsed = engine_llm.parse_preference_notes(
                    raw_text=raw_text,
                    preset_topics=preset_topics,
                )
        finally:
            if owns_db:
                session.close()
        return {
            "positive_topics": list(parsed.get("positive_topics") or []),
            "negative_topics": list(parsed.get("negative_topics") or []),
            "status": "success",
            "error": "",
        }
    except Exception:
        import traceback

        detail = traceback.format_exc(limit=1).strip().splitlines()[-1]
        try:
            fallback = MockDecisionEngine().parse_preference_notes(
                raw_text=raw_text,
                preset_topics=preset_topics,
            )
            return {
                "positive_topics": list(fallback.get("positive_topics") or []),
                "negative_topics": list(fallback.get("negative_topics") or []),
                "status": "success",
                "error": "",
            }
        except Exception:
            fallback_detail = traceback.format_exc(limit=1).strip().splitlines()[-1]
        return {
            "positive_topics": [],
            "negative_topics": [],
            "status": "error",
            "error": f"{detail[:120]} | fallback={fallback_detail[:70]}",
        }


@app.put("/admin/preferences")
def admin_put_preferences(payload: PreferenceIn, db: Session = Depends(get_db)):
    parsed_preferences = _parse_preference_notes(payload.raw_text)
    structured_json = dict(payload.structured_json or {})
    profile = save_priority_preferences(
        db,
        1,
        raw_text=payload.raw_text,
        system_defaults=payload.system_defaults,
        user_priority_topics=payload.user_priority_topics,
        parsed_priority_topics=list(parsed_preferences.get("positive_topics") or []),
        parsed_suppressed_topics=list(parsed_preferences.get("negative_topics") or []),
        admin_priority_topics=(
            list(structured_json.get("admin_priority_topics") or [])
            if "admin_priority_topics" in structured_json
            else None
        ),
        admin_suppressed_topics=(
            list(structured_json.get("admin_suppressed_priority_topics") or [])
            if "admin_suppressed_priority_topics" in structured_json
            else None
        ),
        admin_override_active=(
            bool(structured_json.get("admin_override_active"))
            if "admin_override_active" in structured_json
            else None
        ),
        parse_status=str(parsed_preferences.get("status") or "success"),
        parse_error=str(parsed_preferences.get("error") or ""),
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
        calendar_name=binding.calendar_name,
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


@app.get("/admin/calendar-list")
def admin_get_calendar_list(db: Session = Depends(get_db)):
    credential = db.scalar(
        select(GoogleCredential).where(
            GoogleCredential.household_id == 1,
            GoogleCredential.status == "active",
        )
    )
    if not credential:
        raise HTTPException(status_code=404, detail="No Google credential found")
    if getattr(settings, "google_calendar_mode", "live") == "live" and should_refresh_token(credential.token_expiry):
        if not credential.refresh_token:
            raise HTTPException(status_code=400, detail="Credential expired and no refresh token")
        try:
            new_access_token, new_expiry = refresh_google_access_token(
                refresh_token=credential.refresh_token,
                client_id=settings.google_client_id,
                client_secret=settings.google_client_secret,
                timeout_sec=settings.google_calendar_timeout_sec,
            )
            credential.access_token = new_access_token
            credential.token_expiry = new_expiry
            db.commit()
        except GoogleAuthError as exc:
            raise HTTPException(status_code=400, detail=f"Token refresh failed: {exc}") from exc
    try:
        return calendar_provider.list_calendars(credential.access_token)
    except CalendarMutationError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.put("/admin/calendar-select")
def admin_put_calendar_select(payload: CalendarSelectIn, db: Session = Depends(get_db)):
    binding = db.scalar(select(CalendarBinding).where(CalendarBinding.household_id == 1))
    if not binding:
        raise HTTPException(status_code=404, detail="Calendar binding not found")
    binding.calendar_id = payload.calendar_id
    binding.calendar_name = payload.calendar_name or None
    db.commit()
    return {"ok": True}


@app.post("/admin/calendar-new")
def admin_post_calendar_new(payload: CalendarNewIn, db: Session = Depends(get_db)):
    credential = db.scalar(
        select(GoogleCredential).where(
            GoogleCredential.household_id == 1,
            GoogleCredential.status == "active",
        )
    )
    if not credential:
        raise HTTPException(status_code=404, detail="No Google credential found")
    if getattr(settings, "google_calendar_mode", "live") == "live" and should_refresh_token(credential.token_expiry):
        if not credential.refresh_token:
            raise HTTPException(status_code=400, detail="Credential expired and no refresh token")
        try:
            new_access_token, new_expiry = refresh_google_access_token(
                refresh_token=credential.refresh_token,
                client_id=settings.google_client_id,
                client_secret=settings.google_client_secret,
                timeout_sec=settings.google_calendar_timeout_sec,
            )
            credential.access_token = new_access_token
            credential.token_expiry = new_expiry
            db.flush()
        except GoogleAuthError as exc:
            raise HTTPException(status_code=400, detail=f"Token refresh failed: {exc}") from exc
    try:
        result = calendar_provider.create_calendar(credential.access_token, payload.name)
    except CalendarMutationError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    binding = db.scalar(select(CalendarBinding).where(CalendarBinding.household_id == 1))
    if not binding:
        raise HTTPException(status_code=404, detail="Calendar binding not found")
    binding.calendar_id = result["id"]
    binding.calendar_name = result["summary"]
    db.commit()
    return {"ok": True, "calendar_id": result["id"], "calendar_name": result["summary"]}


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
    return templates.TemplateResponse(request, "onboarding.html", _onboarding_context(db))


def _onboarding_context(db: Session) -> dict:
    credential = db.scalar(
        select(GoogleCredential).where(GoogleCredential.household_id == 1, GoogleCredential.status == "active")
    )
    binding = db.scalar(select(CalendarBinding).where(CalendarBinding.household_id == 1)) if credential else None
    return {
        "profile": admin_get_profile(db),
        "settings": admin_get_settings(db),
        "children": admin_get_children(db),
        "prefs": admin_get_preferences(db),
        "calendar_connected": credential is not None,
        "calendar_email": credential.provider_user_email if credential else None,
        "calendar_id": binding.calendar_id if binding else "",
        "calendar_name": binding.calendar_name if binding else "",
    }


@app.get("/onboarding/design-gallery", response_class=HTMLResponse)
def onboarding_gallery(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "onboarding_gallery.html", _onboarding_context(db))


@app.get("/onboarding/v1", response_class=HTMLResponse)
def onboarding_v1(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "onboarding_v1.html", _onboarding_context(db))


@app.get("/onboarding/v2", response_class=HTMLResponse)
def onboarding_v2(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "onboarding_v2.html", _onboarding_context(db))


@app.get("/onboarding/v3", response_class=HTMLResponse)
def onboarding_v3(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "onboarding_v3.html", _onboarding_context(db))


@app.get("/onboarding/v4", response_class=HTMLResponse)
def onboarding_v4(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "onboarding_v4.html", _onboarding_context(db))


@app.get("/onboarding/v5", response_class=HTMLResponse)
def onboarding_v5(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "onboarding_v5.html", _onboarding_context(db))


@app.get("/onboarding/v6", response_class=HTMLResponse)
def onboarding_v6(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "onboarding_v6.html", _onboarding_context(db))


@app.get("/onboarding/v7", response_class=HTMLResponse)
def onboarding_legacy_redirect(request: Request, db: Session = Depends(get_db)):
    return RedirectResponse(url="/onboarding", status_code=307)


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


@app.get("/architecture-diagrams", response_class=HTMLResponse)
def architecture_diagrams_page(request: Request):
    return templates.TemplateResponse(request, "architecture_diagrams.html", {})


@app.get("/architecture-diagrams-agentsdk", response_class=HTMLResponse)
def architecture_diagrams_agentsdk_page(request: Request):
    return templates.TemplateResponse(request, "architecture_diagrams_agentsdk.html", {})


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
def admin_child_form(
    name: str = Form(),
    school_name: str = Form(),
    grade: str = Form(default=""),
    teacher_contacts_text: str = Form(default=""),
    db: Session = Depends(get_db),
):
    return admin_create_child(
        ChildIn(
            name=name,
            school_name=school_name,
            grade=grade,
            teacher_contacts=_parse_teacher_contacts_text(teacher_contacts_text),
        ),
        db,
    )


@app.post("/admin/preferences/form")
async def admin_pref_form(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    action = str(form.get("prefs_action") or "save")
    structured_json: dict = {}
    has_admin_topic_fields = "admin_priority_topics_text" in form or "admin_suppressed_priority_topics_text" in form
    if action == "regenerate":
        structured_json = {
            "admin_override_active": False,
            "admin_priority_topics": [],
            "admin_suppressed_priority_topics": [],
        }
    elif has_admin_topic_fields:
        structured_json = {
            "admin_override_active": True,
            "admin_priority_topics": _parse_topic_lines(str(form.get("admin_priority_topics_text") or "")),
            "admin_suppressed_priority_topics": _parse_topic_lines(
                str(form.get("admin_suppressed_priority_topics_text") or "")
            ),
        }
    admin_put_preferences(
        PreferenceIn(
            raw_text=str(form.get("raw_text") or ""),
            system_defaults={
                "school_closures": "system_default_school_closures" in form,
                "grade_relevant": "system_default_grade_relevant" in form,
            },
            user_priority_topics=[],
            structured_json=structured_json,
        ),
        db,
    )
    return RedirectResponse(url="/admin", status_code=303)


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


@app.post("/onboarding/profile/form")
def onboarding_profile_form(
    admin_email: str = Form(default=""),
    admin_phone: str = Form(default=""),
    timezone_value: str = Form(default=""),
    db: Session = Depends(get_db),
):
    household = db.scalar(select(Household).where(Household.id == 1))
    admins = db.scalars(
        select(User).where(User.household_id == 1, User.is_admin.is_(True)).order_by(User.id.asc())
    ).all()
    assert household is not None
    if admins:
        admin = admins[0]
        normalized_email = admin_email.strip().lower()
        if normalized_email:
            admin.email = normalized_email
        admin.phone = admin_phone.strip() or None
        for extra_admin in admins[1:]:
            db.delete(extra_admin)
    timezone_name = timezone_value.strip()
    if timezone_name:
        _set_household_timezone(db, household.id, timezone_name)
    db.commit()
    return {"ok": True}


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
async def onboarding_preferences_form(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    admin_put_preferences(
        PreferenceIn(
            raw_text=str(form.get("raw_text") or ""),
            system_defaults={
                "school_closures": "system_default_school_closures" in form,
                "grade_relevant": "system_default_grade_relevant" in form,
            },
            user_priority_topics=[],
            structured_json={},
        ),
        db,
    )
    return RedirectResponse(url="/onboarding", status_code=303)


@app.post("/onboarding/preferences/preview")
async def onboarding_preferences_preview(request: Request):
    form = await request.form()
    raw_text = str(form.get("raw_text") or "").strip()
    if not raw_text:
        return {"positive_topics": [], "negative_topics": [], "status": "empty"}
    parsed = _parse_preference_notes(raw_text)
    return {
        "positive_topics": parsed.get("positive_topics", []),
        "negative_topics": parsed.get("negative_topics", []),
        "status": parsed.get("status", "success"),
    }


@app.post("/onboarding/children/form")
def onboarding_child_form(
    name: str = Form(),
    school_name: str = Form(),
    grade: str = Form(default=""),
    teacher_contacts_text: str = Form(default=""),
    db: Session = Depends(get_db),
):
    return admin_create_child(
        ChildIn(
            name=name,
            school_name=school_name,
            grade=grade,
            teacher_contacts=_parse_teacher_contacts_text(teacher_contacts_text),
        ),
        db,
    )
