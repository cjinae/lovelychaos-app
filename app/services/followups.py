from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import FollowupContext


def resolve_response_channel(*, origin_channel: str, email_intent_mode: str | None = None, admin_phone: str | None = None) -> str:
    if settings.local_test_response_channel_override in {"email", "sms"}:
        return settings.local_test_response_channel_override
    if origin_channel == "sms":
        return "sms"
    if email_intent_mode == "command":
        return "email"
    return "sms" if (admin_phone or "").strip() else "email"


def persist_followup_context(
    db: Session,
    *,
    household_id: int,
    source_message_id: int,
    origin_channel: str,
    response_channel: str,
    thread_or_conversation_key: str,
    summary_title: str,
    summary_items_shown: list[dict],
    all_extracted_items: list[dict],
    section_snippets: list[dict],
) -> FollowupContext:
    ctx = FollowupContext(
        household_id=household_id,
        source_message_id=source_message_id,
        origin_channel=origin_channel,
        response_channel=response_channel,
        thread_or_conversation_key=thread_or_conversation_key,
        summary_title=summary_title,
        summary_items_shown=summary_items_shown,
        all_extracted_items=all_extracted_items,
        section_snippets=section_snippets,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db.add(ctx)
    return ctx


def load_active_followup_context(db: Session, *, household_id: int, response_channel: str) -> Optional[FollowupContext]:
    now = datetime.now(timezone.utc)
    return db.scalar(
        select(FollowupContext)
        .where(
            FollowupContext.household_id == household_id,
            FollowupContext.response_channel == response_channel,
            FollowupContext.expires_at >= now,
        )
        .order_by(FollowupContext.created_at.desc())
    )


@dataclass
class FollowupMatch:
    item: dict
    from_summary: bool
    score: int


def resolve_followup_item(context: FollowupContext, *, query_text: str = "", topic: str | None = None) -> Optional[FollowupMatch]:
    query = _normalize_text(topic or query_text)
    if not query:
        return None

    summary_match = _best_match(context.summary_items_shown or [], query, from_summary=True)
    extracted_match = _best_match(context.all_extracted_items or [], query, from_summary=False)
    matches = [item for item in [summary_match, extracted_match] if item is not None]
    if not matches:
        return None
    matches.sort(key=lambda item: (-item.score, 0 if item.from_summary else 1))
    top = matches[0]
    if len(matches) > 1 and matches[1].score == top.score and _normalize_text(matches[1].item.get("text") or matches[1].item.get("title")) != _normalize_text(top.item.get("text") or top.item.get("title")):
        return None
    return top if top.score >= 2 else None


def build_more_info_message(context: FollowupContext, match: FollowupMatch) -> str:
    item = match.item
    title = str(item.get("text") or item.get("title") or "Topic").strip()
    lines = [title]

    snippets = []
    item_terms = _keywords(title)
    for snippet in context.section_snippets or []:
        text = str(snippet.get("text") or "").strip()
        if not text:
            continue
        normalized = _normalize_text(text)
        if item_terms and any(term in normalized for term in item_terms):
            snippets.append(_compress_snippet(text))
    for snippet in snippets[:3]:
        if snippet and snippet not in lines:
            lines.append(f"- {snippet}")

    if len(lines) == 1:
        reason = str(item.get("reason") or "").strip()
        if reason:
            lines.append(f"- {reason}")
        else:
            lines.append("- I found the topic in the original school update, but I need a more specific follow-up flow to say more.")
    return "\n".join(lines[:4])


def _best_match(items: list[dict], query: str, *, from_summary: bool) -> Optional[FollowupMatch]:
    best: Optional[FollowupMatch] = None
    query_terms = _keywords(query)
    for item in items:
        haystack = _normalize_text(
            " ".join(
                [
                    str(item.get("text") or ""),
                    str(item.get("title") or ""),
                    str(item.get("reason") or ""),
                    " ".join(str(value) for value in list(item.get("applies_to") or [])),
                ]
            )
        )
        if not haystack:
            continue
        score = 0
        if query in haystack:
            score += 4
        score += sum(1 for term in query_terms if term and term in haystack)
        if best is None or score > best.score:
            best = FollowupMatch(item=item, from_summary=from_summary, score=score)
    return best


def _compress_snippet(text: str) -> str:
    compact = re.sub(r"\s+", " ", text.strip())
    parts = re.split(r"(?<=[.!?])\s+", compact)
    first = parts[0].strip() if parts else compact
    return first[:220].rstrip()


def _keywords(value: str) -> list[str]:
    tokens = [token for token in _normalize_text(value).split() if len(token) > 2]
    return tokens[:8]


def _normalize_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", (value or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()
