from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import TYPE_CHECKING, Callable, Iterable, Optional

from openai import OpenAI
from sqlalchemy import delete, select
from sqlalchemy.orm import Session as OrmSession

from agents.memory import SessionABC, SessionSettings

from app.models import AgentSessionItem, SourceMessage, ThreadDocument

if TYPE_CHECKING:
    from app.services.content_analysis import DownloadedAttachment


DbSessionFactory = Callable[[], OrmSession]

_MESSAGE_ID_PATTERN = re.compile(r"<[^>]+>")


def household_session_id(*, household_id: int) -> str:
    return f"household:{household_id}"


def email_session_id(*, household_id: int, thread_key: str) -> str:
    return f"email:{household_id}:{thread_key}"


def sms_session_id(*, household_id: int) -> str:
    return f"sms:{household_id}"


def build_text_session_item(*, role: str, text: str) -> dict:
    content_type = "output_text" if str(role or "").strip().lower() == "assistant" else "input_text"
    return {
        "role": role,
        "content": [{"type": content_type, "text": text}],
    }


def extract_header_value(headers: object, name: str) -> str:
    wanted = (name or "").strip().lower()
    if not wanted or headers is None:
        return ""
    if isinstance(headers, dict):
        for key, value in headers.items():
            if str(key).strip().lower() == wanted:
                return str(value or "").strip()
        return ""
    if isinstance(headers, list):
        for item in headers:
            if not isinstance(item, dict):
                continue
            key = str(item.get("name") or item.get("key") or "").strip().lower()
            if key == wanted:
                return str(item.get("value") or "").strip()
    return ""


def extract_message_ids(*values: Optional[str]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        matches = _MESSAGE_ID_PATTERN.findall(raw)
        candidates = matches or [raw]
        for candidate in candidates:
            cleaned = candidate.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            ids.append(cleaned)
    return ids


def resolve_email_thread_key(
    db: OrmSession,
    *,
    household_id: int,
    internet_message_id: str = "",
    in_reply_to_message_id: str = "",
    references_header: str = "",
    fallback_key: str,
) -> str:
    candidate_ids = extract_message_ids(references_header, in_reply_to_message_id, internet_message_id)
    if candidate_ids:
        matches = list(
            db.scalars(
                select(SourceMessage)
                .where(
                    SourceMessage.household_id == household_id,
                    (
                        SourceMessage.internet_message_id.in_(candidate_ids)
                        | SourceMessage.thread_key.in_(candidate_ids)
                    ),
                )
                .order_by(SourceMessage.id.asc())
            )
        )
        if matches:
            first = matches[0]
            return (
                str(first.thread_key or "").strip()
                or str(first.internet_message_id or "").strip()
                or str(first.provider_message_id or "").strip()
                or fallback_key
            )
    return (
        str(internet_message_id or "").strip()
        or (candidate_ids[0] if candidate_ids else "")
        or fallback_key
    )


def build_email_reply_headers(source: SourceMessage) -> dict[str, str]:
    references = extract_message_ids(source.references_header, source.internet_message_id)
    in_reply_to = str(source.internet_message_id or "").strip() or str(source.in_reply_to_message_id or "").strip()
    headers: dict[str, str] = {}
    if in_reply_to:
        headers["In-Reply-To"] = in_reply_to
    if references:
        headers["References"] = " ".join(references)
    return headers


@dataclass
class ThreadDocumentContext:
    filename: str
    content_type: str
    extracted_text: str
    openai_file_id: str | None = None


def load_thread_documents(db: OrmSession, *, household_id: int, thread_key: str) -> list[ThreadDocumentContext]:
    if not thread_key:
        return []
    rows = list(
        db.scalars(
            select(ThreadDocument)
            .where(
                ThreadDocument.household_id == household_id,
                ThreadDocument.thread_key == thread_key,
            )
            .order_by(ThreadDocument.id.asc())
        )
    )
    documents: list[ThreadDocumentContext] = []
    seen: set[str] = set()
    for row in rows:
        key = json.dumps(
            {
                "filename": row.filename,
                "content_type": row.content_type,
                "source_url": row.source_url,
                "openai_file_id": row.openai_file_id,
                "text_hash": _stable_text_hash(row.extracted_text or ""),
            },
            sort_keys=True,
        )
        if key in seen:
            continue
        seen.add(key)
        documents.append(
            ThreadDocumentContext(
                filename=row.filename,
                content_type=row.content_type,
                extracted_text=row.extracted_text or "",
                openai_file_id=row.openai_file_id,
            )
        )
    return documents


def load_recent_household_documents(
    db: OrmSession,
    *,
    household_id: int,
    limit: int = 3,
    max_age_days: int = 7,
) -> list[ThreadDocumentContext]:
    from datetime import datetime, timedelta, timezone as tz

    cutoff = datetime.now(tz.utc) - timedelta(days=max_age_days)
    rows = list(
        db.scalars(
            select(ThreadDocument)
            .where(
                ThreadDocument.household_id == household_id,
                ThreadDocument.created_at >= cutoff,
            )
            .order_by(ThreadDocument.created_at.desc())
            .limit(limit * 2)
        )
    )
    documents: list[ThreadDocumentContext] = []
    seen: set[str] = set()
    for row in rows:
        key = json.dumps(
            {
                "filename": row.filename,
                "content_type": row.content_type,
                "source_url": row.source_url,
                "text_hash": _stable_text_hash(row.extracted_text or ""),
            },
            sort_keys=True,
        )
        if key in seen:
            continue
        seen.add(key)
        documents.append(
            ThreadDocumentContext(
                filename=row.filename,
                content_type=row.content_type,
                extracted_text=row.extracted_text or "",
                openai_file_id=row.openai_file_id,
            )
        )
        if len(documents) >= limit:
            break
    return documents


def persist_thread_documents(
    db: OrmSession,
    *,
    household_id: int,
    source_message_id: int,
    thread_key: str,
    attachments: Iterable["DownloadedAttachment"],
    openai_api_key: str = "",
    openai_base_url: str = "https://api.openai.com/v1",
    openai_timeout_sec: int = 60,
) -> list[ThreadDocumentContext]:
    documents: list[ThreadDocumentContext] = []
    seen: set[str] = set()
    for attachment in attachments:
        extracted_text = str(attachment.extracted_text or "").strip()
        if not extracted_text and "pdf" not in str(attachment.content_type or "").lower():
            continue
        cache_key = json.dumps(
            {
                "filename": attachment.filename,
                "content_type": attachment.content_type,
                "source_url": attachment.source_url,
                "text_hash": _stable_text_hash(extracted_text),
            },
            sort_keys=True,
        )
        if cache_key in seen:
            continue
        seen.add(cache_key)
        openai_file_id = _upload_openai_file(
            content=attachment.content,
            filename=attachment.filename,
            content_type=attachment.content_type,
            api_key=openai_api_key,
            base_url=openai_base_url,
            timeout_sec=openai_timeout_sec,
        )
        row = ThreadDocument(
            household_id=household_id,
            source_message_id=source_message_id,
            thread_key=thread_key,
            filename=attachment.filename,
            content_type=attachment.content_type,
            source_url=attachment.source_url,
            extracted_text=extracted_text,
            openai_file_id=openai_file_id,
        )
        db.add(row)
        documents.append(
            ThreadDocumentContext(
                filename=attachment.filename,
                content_type=attachment.content_type,
                extracted_text=extracted_text,
                openai_file_id=openai_file_id,
            )
        )
    return documents


def append_session_message(
    session_id: str,
    *,
    role: str,
    text: str,
    db_session_factory: DbSessionFactory,
) -> None:
    if not session_id or not text.strip():
        return
    payload = build_text_session_item(role=role, text=text.strip())
    with db_session_factory() as db:
        db.add(AgentSessionItem(session_id=session_id, payload=payload))
        db.commit()


def queue_session_message(
    db: OrmSession,
    session_id: str,
    *,
    role: str,
    text: str,
) -> None:
    if not session_id or not text.strip():
        return
    db.add(AgentSessionItem(session_id=session_id, payload=build_text_session_item(role=role, text=text.strip())))


class DbBackedAgentSession(SessionABC):
    def __init__(
        self,
        session_id: str,
        *,
        db_session_factory: DbSessionFactory,
        session_settings: SessionSettings | None = None,
    ):
        self.session_id = session_id
        self.db_session_factory = db_session_factory
        self.session_settings = session_settings or SessionSettings(limit=50)

    async def get_items(self, limit: int | None = None) -> list[dict]:
        with self.db_session_factory() as db:
            query = (
                select(AgentSessionItem)
                .where(AgentSessionItem.session_id == self.session_id)
                .order_by(AgentSessionItem.id.desc())
            )
            if limit is not None:
                query = query.limit(limit)
            rows = list(db.scalars(query))
        payloads = [_normalize_session_payload(dict(row.payload or {})) for row in reversed(rows)]
        return payloads

    async def add_items(self, items: list[dict]) -> None:
        if not items:
            return
        with self.db_session_factory() as db:
            for item in items:
                db.add(AgentSessionItem(session_id=self.session_id, payload=dict(item)))
            db.commit()

    async def pop_item(self) -> dict | None:
        with self.db_session_factory() as db:
            row = db.scalar(
                select(AgentSessionItem)
                .where(AgentSessionItem.session_id == self.session_id)
                .order_by(AgentSessionItem.id.desc())
            )
            if row is None:
                return None
            payload = dict(row.payload or {})
            db.delete(row)
            db.commit()
            return payload

    async def clear_session(self) -> None:
        with self.db_session_factory() as db:
            db.execute(delete(AgentSessionItem).where(AgentSessionItem.session_id == self.session_id))
            db.commit()


def _upload_openai_file(
    *,
    content: bytes,
    filename: str,
    content_type: str,
    api_key: str,
    base_url: str,
    timeout_sec: int,
) -> Optional[str]:
    if not api_key or not content:
        return None
    if "pdf" not in str(content_type or "").lower():
        return None
    try:
        client = OpenAI(api_key=api_key, base_url=base_url.rstrip("/"), timeout=timeout_sec)
        uploaded = client.files.create(
            file=(filename or "attachment.pdf", content, content_type or "application/pdf"),
            purpose="user_data",
        )
    except Exception:
        return None
    file_id = str(getattr(uploaded, "id", "") or "").strip()
    return file_id or None


def _stable_text_hash(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _normalize_session_payload(payload: dict) -> dict:
    normalized = dict(payload or {})
    role = str(normalized.get("role") or "").strip().lower()
    content = []
    for part in list(normalized.get("content") or []):
        if not isinstance(part, dict):
            continue
        entry = dict(part)
        if role == "assistant" and entry.get("type") == "input_text":
            entry["type"] = "output_text"
        content.append(entry)
    normalized["content"] = content
    return normalized
