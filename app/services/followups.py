from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import FollowupContext, SmsConversationState, SourceMessage, ThreadDocument


def resolve_response_channel(*, origin_channel: str, email_intent_mode: str | None = None, admin_phone: str | None = None) -> str:
    if settings.local_test_response_channel_override in {"email", "sms"}:
        return settings.local_test_response_channel_override
    return origin_channel


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
    actionable_items: list[dict],
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
        all_extracted_items=actionable_items,
        section_snippets=section_snippets,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db.add(ctx)
    return ctx


def load_active_followup_context(
    db: Session,
    *,
    household_id: int,
    response_channel: str,
    thread_or_conversation_key: str | None = None,
    cross_channel: bool = False,
) -> Optional[FollowupContext]:
    now = datetime.now(timezone.utc)
    base_query = (
        select(FollowupContext)
        .where(
            FollowupContext.household_id == household_id,
            FollowupContext.response_channel == response_channel,
            FollowupContext.expires_at >= now,
        )
        .order_by(FollowupContext.created_at.desc())
    )
    if thread_or_conversation_key:
        exact_match = db.scalar(
            base_query.where(FollowupContext.thread_or_conversation_key == thread_or_conversation_key)
        )
        if exact_match is not None:
            return exact_match
    channel_match = db.scalar(base_query)
    if channel_match is not None:
        return channel_match
    if cross_channel:
        return db.scalar(
            select(FollowupContext)
            .where(
                FollowupContext.household_id == household_id,
                FollowupContext.expires_at >= now,
            )
            .order_by(FollowupContext.created_at.desc())
        )
    return None


def load_recent_followup_contexts(db: Session, *, household_id: int, limit: int = 5) -> list[FollowupContext]:
    now = datetime.now(timezone.utc)
    return list(
        db.scalars(
            select(FollowupContext)
            .where(
                FollowupContext.household_id == household_id,
                FollowupContext.expires_at >= now,
            )
            .order_by(FollowupContext.created_at.desc())
            .limit(limit)
        )
    )


def load_active_sms_conversation_state(
    db: Session,
    *,
    household_id: int,
    channel: str = "sms",
) -> Optional[SmsConversationState]:
    now = datetime.now(timezone.utc)
    return db.scalar(
        select(SmsConversationState)
        .where(
            SmsConversationState.household_id == household_id,
            SmsConversationState.channel == channel,
            SmsConversationState.status == "active",
            SmsConversationState.expires_at >= now,
        )
        .order_by(SmsConversationState.created_at.desc())
    )


def persist_sms_conversation_state(
    db: Session,
    *,
    household_id: int,
    requested_action: str,
    candidate_items: list[dict],
    source_followup_context_ids: list[int],
    prompt_message: str,
    channel: str = "sms",
    state_type: str = "followup_selection",
    ttl_minutes: int = 60,
) -> SmsConversationState:
    now = datetime.now(timezone.utc)
    active_states = list(
        db.scalars(
            select(SmsConversationState).where(
                SmsConversationState.household_id == household_id,
                SmsConversationState.channel == channel,
                SmsConversationState.status == "active",
            )
        )
    )
    for state in active_states:
        state.status = "replaced"
        state.resolved_at = now

    state = SmsConversationState(
        household_id=household_id,
        channel=channel,
        state_type=state_type,
        requested_action=requested_action,
        candidate_items=candidate_items,
        source_followup_context_ids=source_followup_context_ids,
        prompt_message=prompt_message,
        expires_at=now + timedelta(minutes=ttl_minutes),
    )
    db.add(state)
    db.flush()
    return state


def resolve_sms_conversation_state(state: SmsConversationState) -> None:
    state.status = "resolved"
    state.resolved_at = datetime.now(timezone.utc)


@dataclass
class FollowupMatch:
    item: dict
    from_summary: bool
    score: int


@dataclass(frozen=True)
class MoreInfoContextAssessment:
    weak: bool
    reason: str | None
    stored_snippets: list[str]
    stored_source_snippets: list[str]


def resolve_followup_item(context: FollowupContext, *, query_text: str = "", topic: str | None = None) -> Optional[FollowupMatch]:
    matches = resolve_followup_candidates(context, query_text=query_text, topic=topic)
    if len(matches) != 1:
        return None
    return matches[0]


def resolve_followup_candidates(context: FollowupContext, *, query_text: str = "", topic: str | None = None) -> list[FollowupMatch]:
    query_value = _query_focus_text(topic or query_text)
    query = _normalize_text(query_value)
    if not query:
        return []

    actionable_by_id = {
        str(item.get("item_id")): item
        for item in list(context.all_extracted_items or [])
        if str(item.get("item_id") or "").strip()
    }

    matches = [
        *_scored_matches(context.summary_items_shown or [], query, query_value, from_summary=True),
        *_scored_matches(context.all_extracted_items or [], query, query_value, from_summary=False),
    ]
    if not matches:
        return []
    matches.sort(key=lambda item: (-item.score, 0 if item.from_summary else 1))
    top_score = matches[0].score
    if top_score < 2:
        return []

    top_matches = [item for item in matches if item.score == top_score]
    distinct_matches: list[FollowupMatch] = []
    seen_keys: set[str] = set()
    for match in top_matches:
        merged = _merge_match(match, actionable_by_id)
        item_key = str(merged.item.get("item_id") or _normalize_text(merged.item.get("display_text") or merged.item.get("text") or merged.item.get("title") or ""))
        if item_key in seen_keys:
            continue
        seen_keys.add(item_key)
        distinct_matches.append(merged)
    return distinct_matches


def resolve_candidate_items(candidate_items: list[dict], *, query_text: str = "", topic: str | None = None) -> list[FollowupMatch]:
    query_value = _query_focus_text(topic or query_text)
    query = _normalize_text(query_value)
    if not query:
        return []
    matches = _scored_matches(candidate_items, query, query_value, from_summary=False)
    if not matches:
        return []
    top_score = matches[0].score
    if top_score < 2:
        return []
    return [match for match in matches if match.score == top_score]


def build_more_info_message(context: FollowupContext, match: FollowupMatch, *, snippets: list[str] | None = None) -> str:
    item = match.item
    title = str(item.get("display_text") or item.get("text") or item.get("title") or "Topic").strip()
    lines = [title]
    assistant_detail = str(item.get("assistant_detail") or "").strip()
    if assistant_detail:
        lines.append(f"- {assistant_detail}")

    snippet_values = snippets if snippets is not None else select_more_info_snippets(context, match)
    for snippet in snippet_values:
        if snippet and snippet not in lines:
            lines.append(f"- {snippet}")

    if len(lines) == 1:
        reason = str(item.get("reason") or "").strip()
        if reason:
            lines.append(f"- {reason}")
        else:
            lines.append("- I found the topic in the original school update, but I need a more specific follow-up flow to say more.")
    return "\n".join(lines[:4])


def select_more_info_snippets(
    context: FollowupContext,
    match: FollowupMatch,
    limit: int = 3,
    *,
    include_document_generated: bool = True,
) -> list[str]:
    title = str(
        match.item.get("display_text") or match.item.get("text") or match.item.get("title") or "Topic"
    ).strip()
    snippets: list[str] = []
    item_terms = _keywords(title)
    for snippet in context.section_snippets or []:
        meta = str(snippet.get("meta") or "").strip()
        if meta == "document_understanding":
            continue
        if not include_document_generated and meta.startswith("document_"):
            continue
        text = str(snippet.get("text") or "").strip()
        if not text:
            continue
        normalized = _normalize_text(text)
        if item_terms and any(term in normalized for term in item_terms):
            snippets.append(_compress_snippet(text))
    return snippets[: max(1, limit)]


def assess_more_info_context(context: FollowupContext, match: FollowupMatch, *, limit: int = 3) -> MoreInfoContextAssessment:
    stored_snippets = select_more_info_snippets(context, match, limit=limit)
    stored_source_snippets = select_more_info_snippets(
        context,
        match,
        limit=limit,
        include_document_generated=False,
    )
    phrases = _more_info_match_phrases(match)
    terms = _more_info_match_terms(match)
    strongest_source_snippet_score = max(
        (_score_more_info_source_block(snippet, phrases=phrases, terms=terms) for snippet in stored_source_snippets),
        default=0,
    )
    item = dict(match.item or {})
    direct_detail_chars = sum(
        len(value)
        for value in [
            str(item.get("assistant_detail") or "").strip(),
            str(item.get("timing_hint") or "").strip(),
            str(item.get("action_hint") or "").strip(),
        ]
        if value
    )
    source_refs = [str(value).strip() for value in list(item.get("source_refs") or []) if str(value).strip()]
    document_only_match = bool(source_refs) and all(ref.startswith("document_understanding:") for ref in source_refs)
    if strongest_source_snippet_score >= 6:
        return MoreInfoContextAssessment(
            weak=False,
            reason=None,
            stored_snippets=stored_snippets,
            stored_source_snippets=stored_source_snippets,
        )
    if direct_detail_chars >= 140:
        return MoreInfoContextAssessment(
            weak=False,
            reason=None,
            stored_snippets=stored_snippets,
            stored_source_snippets=stored_source_snippets,
        )
    if document_only_match and not stored_source_snippets:
        reason = "matched_topic_only_has_document_understanding_context"
    elif not stored_source_snippets:
        reason = "matched_topic_lacks_source_backed_snippets"
    else:
        reason = "matched_topic_has_thin_stored_detail"
    return MoreInfoContextAssessment(
        weak=True,
        reason=reason,
        stored_snippets=stored_snippets,
        stored_source_snippets=stored_source_snippets,
    )


def retrieve_more_info_source_snippets(
    db: Session,
    *,
    context: FollowupContext,
    match: FollowupMatch,
    query_text: str = "",
    limit: int = 3,
) -> list[str]:
    source = db.get(SourceMessage, context.source_message_id)
    if source is None:
        return []
    phrases = _more_info_match_phrases(match, query_text=query_text)
    if not phrases:
        return []
    terms = _more_info_match_terms(match, query_text=query_text)
    candidates: list[tuple[int, str]] = []
    for text in _more_info_source_texts(db, context=context, source=source):
        for block in _split_source_blocks(text):
            score = _score_more_info_source_block(block, phrases=phrases, terms=terms)
            if score < 3:
                continue
            candidates.append((score, block))
    candidates.sort(key=lambda item: (-item[0], len(item[1])))
    snippets: list[str] = []
    seen: set[str] = set()
    for _, block in candidates:
        snippet = _compress_snippet(block, max_chars=320, max_sentences=3)
        normalized = _normalize_text(snippet)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        snippets.append(snippet)
        if len(snippets) >= max(1, limit):
            break
    return snippets


def _scored_matches(items: list[dict], query: str, raw_query: str, *, from_summary: bool) -> list[FollowupMatch]:
    matches: list[FollowupMatch] = []
    query_terms = _keywords(query)
    query_date_keys = _extract_month_day_keys(raw_query)
    for item in items:
        primary_text = str(item.get("display_text") or item.get("text") or item.get("title") or "").strip()
        aliases = [str(value).strip() for value in list(item.get("aliases") or []) if str(value).strip()]
        haystack = _normalize_text(
            " ".join(
                [
                    primary_text,
                    *aliases,
                    str(item.get("reason") or ""),
                    str(item.get("kind") or ""),
                    " ".join(str(value) for value in list(item.get("applies_to") or [])),
                ]
            )
        )
        if not haystack:
            continue
        normalized_primary = _normalize_text(primary_text)
        normalized_aliases = {_normalize_text(value) for value in aliases}
        score = 0
        if query == normalized_primary or query in normalized_aliases:
            score += 10
        elif normalized_primary and normalized_primary in query:
            score += 6
        elif query in haystack:
            score += 4
        score += sum(1 for term in query_terms if term and term in haystack)
        if query_date_keys:
            score += 4 * len(query_date_keys & _item_month_day_keys(item))
        matches.append(FollowupMatch(item=item, from_summary=from_summary, score=score))
    matches.sort(key=lambda item: (-item.score, 0 if item.from_summary else 1))
    return matches


def _merge_match(match: FollowupMatch, actionable_by_id: dict[str, dict]) -> FollowupMatch:
    item_id = str(match.item.get("item_id") or "").strip()
    actionable = actionable_by_id.get(item_id)
    if not actionable:
        return match
    merged_item = dict(actionable)
    merged_item.setdefault("text", str(match.item.get("text") or match.item.get("title") or "").strip())
    merged_item["display_text"] = str(match.item.get("text") or match.item.get("title") or merged_item.get("title") or "").strip()
    merged_item["source_refs"] = list(match.item.get("source_refs") or merged_item.get("source_refs") or [])
    merged_item["applies_to"] = list(match.item.get("applies_to") or merged_item.get("applies_to") or [])
    return FollowupMatch(item=merged_item, from_summary=match.from_summary, score=match.score)


def _compress_snippet(text: str, *, max_chars: int = 220, max_sentences: int = 1) -> str:
    compact = re.sub(r"\s+", " ", text.strip())
    parts = re.split(r"(?<=[.!?])\s+", compact)
    if parts and max_sentences > 0:
        chosen = " ".join(part.strip() for part in parts[:max_sentences] if part.strip()).strip()
    else:
        chosen = compact
    if not chosen:
        chosen = compact
    return chosen[:max_chars].rstrip()


def _keywords(value: str) -> list[str]:
    tokens = [token for token in _normalize_text(value).split() if len(token) > 2]
    return tokens[:8]


def _query_focus_text(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    patterns = [
        r"^\s*(?:please\s+)?add\s+(.+?)\s+to\s+(?:the\s+)?cal(?:endar)?\s*$",
        r"^\s*(?:please\s+)?add\s+(.+?)\s*$",
        r"^\s*(?:tell me more about|more info about|more details about|what about)\s+(.+?)\s*$",
    ]
    for pattern in patterns:
        match = re.match(pattern, text, re.I)
        if not match:
            continue
        focused = match.group(1).strip(" .?!")
        if focused:
            return focused
    return text


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


def _extract_month_day_keys(value: str) -> set[str]:
    keys: set[str] = set()
    for match in re.finditer(
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\.?\s+(\d{1,2})\b",
        value or "",
        re.I,
    ):
        month_key = match.group(1).lower().rstrip(".")
        month = MONTH_NAME_MAP.get(month_key)
        if month is None:
            continue
        keys.add(f"{month:02d}-{int(match.group(2)):02d}")
    return keys


def _item_month_day_keys(item: dict) -> set[str]:
    keys = set()
    for value in [
        item.get("date_sort_key"),
        item.get("start_at"),
        item.get("end_at"),
    ]:
        if isinstance(value, str) and value:
            match = re.match(r"(\d{4})-(\d{2})-(\d{2})", value)
            if match:
                keys.add(f"{int(match.group(2)):02d}-{int(match.group(3)):02d}")
    for value in [
        item.get("display_text"),
        item.get("text"),
        item.get("title"),
    ]:
        if isinstance(value, str):
            keys.update(_extract_month_day_keys(value))
    return keys


def _normalize_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", (value or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _more_info_match_phrases(match: FollowupMatch, *, query_text: str = "") -> list[str]:
    item = dict(match.item or {})
    values = [
        _query_focus_text(query_text),
        str(item.get("display_text") or "").strip(),
        str(item.get("title") or "").strip(),
        *(str(value).strip() for value in list(item.get("aliases") or [])),
    ]
    phrases: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _normalize_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        phrases.append(value)
    return phrases[:6]


def _more_info_match_terms(match: FollowupMatch, *, query_text: str = "") -> list[str]:
    item = dict(match.item or {})
    terms: list[str] = []
    seen: set[str] = set()
    for value in [
        _query_focus_text(query_text),
        str(item.get("display_text") or "").strip(),
        str(item.get("title") or "").strip(),
        " ".join(str(value).strip() for value in list(item.get("aliases") or []) if str(value).strip()),
    ]:
        for term in _keywords(value):
            if term in seen:
                continue
            seen.add(term)
            terms.append(term)
    return terms[:12]


def _more_info_source_texts(db: Session, *, context: FollowupContext, source: SourceMessage) -> list[str]:
    texts: list[str] = []
    if str(source.body_text or "").strip():
        texts.append(str(source.body_text or ""))
    thread_documents: list[ThreadDocument] = []
    if str(source.thread_key or "").strip():
        thread_documents = list(
            db.scalars(
                select(ThreadDocument)
                .where(
                    ThreadDocument.household_id == context.household_id,
                    ThreadDocument.thread_key == source.thread_key,
                )
                .order_by(ThreadDocument.id.asc())
            )
        )
    if not thread_documents:
        thread_documents = list(
            db.scalars(
                select(ThreadDocument)
                .where(ThreadDocument.source_message_id == context.source_message_id)
                .order_by(ThreadDocument.id.asc())
            )
        )
    seen: set[str] = {_normalize_text(text) for text in texts if _normalize_text(text)}
    for document in thread_documents:
        extracted_text = str(document.extracted_text or "").strip()
        normalized = _normalize_text(extracted_text)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        texts.append(extracted_text)
    return texts


def _split_source_blocks(text: str, *, max_chars: int = 700) -> list[str]:
    cleaned = re.sub(r"\r\n?", "\n", text or "").strip()
    if not cleaned:
        return []
    raw_blocks = [block.strip() for block in re.split(r"\n\s*\n", cleaned) if block.strip()]
    if len(raw_blocks) == 1:
        raw_blocks = [block.strip() for block in cleaned.splitlines() if block.strip()]
    blocks: list[str] = []
    for raw_block in raw_blocks:
        compact = re.sub(r"\s+", " ", raw_block).strip()
        if not compact:
            continue
        if len(compact) <= max_chars:
            blocks.append(compact)
            continue
        sentence_buffer = ""
        for sentence in re.split(r"(?<=[.!?])\s+", compact):
            sentence = sentence.strip()
            if not sentence:
                continue
            candidate = f"{sentence_buffer} {sentence}".strip()
            if sentence_buffer and len(candidate) > max_chars:
                blocks.append(sentence_buffer)
                sentence_buffer = sentence
            else:
                sentence_buffer = candidate
        if sentence_buffer:
            blocks.append(sentence_buffer)
    return blocks


def _score_more_info_source_block(block: str, *, phrases: list[str], terms: list[str]) -> int:
    normalized = _normalize_text(block)
    if not normalized:
        return 0
    score = 0
    for phrase in phrases:
        normalized_phrase = _normalize_text(phrase)
        if not normalized_phrase:
            continue
        if normalized_phrase == normalized:
            score += 18
        elif normalized_phrase in normalized:
            score += 12 if " " in normalized_phrase else 4
    term_hits = {term for term in terms if term and term in normalized}
    score += len(term_hits)
    if len(term_hits) >= 2:
        score += 3
    return score
