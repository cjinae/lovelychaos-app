from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from typing import Optional

import httpx


EXTRACTION_PROMPT_VERSION = "lovelychaos-extract-v4"
COMMAND_PROMPT_VERSION = "lovelychaos-command-v3"
SUMMARY_EXTRACTION_PROMPT_VERSION = "lovelychaos-summary-extract-v2"
SUMMARY_COMPRESSION_PROMPT_VERSION = "lovelychaos-summary-compress-v3"

EXTRACTION_SYSTEM_PROMPT = """You are LovelyChaos event extraction engine.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
- `events` must contain one object per distinct event candidate.
- If no event candidates are present, return `{"events":[],"email_level_notes":null}`.
</output_contract>

<task>
Identify every distinct event candidate mentioned in the inbound email content.
Interpret imprecise wording like a careful human would, but never invent facts.
</task>

<grounding_rules>
- Base every field only on the provided email subject, body, forwarded metadata, and household preferences.
- Prefer explicit facts over inference.
- Do not fabricate dates, times, schools, names, grades, or relevance.
- If a statement is inferred rather than explicit, lower confidence and explain the inference briefly in `model_reason`.
</grounding_rules>

<completeness_contract>
- Extract every distinct event mention. If one email mentions 4 different events, return 4 event objects.
- Do not merge separate events into one object.
- Include school-wide or general school events even when they may not be household-relevant.
- If an event is mentioned but date, time, or details are missing or unclear, still return it with null fields where needed.
- Treat newsletter lists, schedules, and repeated date blocks as coverage tasks: keep going until all distinct event candidates in the list are covered.
</completeness_contract>

<missing_context_gating>
- Do not use household preferences as a filter for whether to emit an event.
- Household preferences are context for interpreting ambiguous wording, not a reason to drop school-global or incomplete events.
- If uncertain, lower confidence and leave uncertain fields null.
- Normalize timestamps to ISO-8601 UTC when the date and time are supported by the provided context.
</missing_context_gating>

<inference_rules>
- When a newsletter or attachment clearly establishes the publication date or year in the subject, filename, or body, you may infer the same year for schedule entries that omit the year but belong to that same schedule block.
- Treat forwarded original-email metadata as valid document context. If the forwarded email includes lines like `Date:` or `Subject:` that clearly establish the newsletter date or year, you may use that context for events extracted from linked or attached newsletter files.
- For newsletter-style sections like `UPCOMING DATES`, infer the year from nearby document context when the month/day entries are clearly part of the same newsletter and no conflicting year is present.
- If you infer a year from newsletter context, keep the event but lower confidence slightly and explain the inference in `model_reason`.
</inference_rules>

<field_rules>
- `model_batch` is only a tentative guess:
  - A = explicit event with enough scheduling detail
  - B = event seems important but is missing details or unclear
  - C = informational, school-global, or likely not household-relevant
- `preference_match` should reflect whether the event appears relevant to the household preferences, but it must not control whether the event is emitted.
- Keep `model_reason` concise and specific.
- Keep `email_level_notes` concise and use null when nothing noteworthy needs to be added.
</field_rules>

<verification_loop>
Before finalizing:
- Check that every distinct event candidate found in the email is represented or intentionally omitted because it is not an event.
- Check that unsupported facts were not invented.
- Check that uncertain values are null rather than guessed.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

COMMAND_SYSTEM_PROMPT = """You are LovelyChaos command parser.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Allowed actions only: add, more_info, delete, remind, set_preference, none.
- Output must conform exactly to the provided schema.
</output_contract>

<task>
Interpret whether the message is issuing a supported follow-up command after a LovelyChaos summary.
</task>

<decision_rules>
- Choose the single best supported action from the allowed set.
- If the message is ambiguous, unsupported, conflicting, or lacks enough evidence for a supported action, return `none`.
- Only set `pending_id` when the message explicitly provides it or it is otherwise unambiguous from the message text.
- Only set `topic` when the user clearly asks for more information about a specific topic.
- Only set `preference_behavior` when the user is clearly asking to change a future preference or default handling for a topic.
- Only set `minutes_before` when the user explicitly requests a reminder offset; otherwise return null.
- Only set `reminder_channel` when the user specifies `sms` or `calendar`, or the message clearly asks for one of those channels; otherwise return null.
- Set `async_requested` to true only when the user explicitly requests deferred or later handling.
- Keep confidence calibrated: lower it when intent is weak, partial, or ambiguous.
</decision_rules>

<missing_context_gating>
- Do not invent IDs, reminder offsets, channels, or unsupported actions.
- If required context is missing, prefer `none` or null fields over guessing.
</missing_context_gating>

<verification_loop>
Before finalizing:
- Check that the chosen action is one of the allowed actions.
- Check that every populated field is directly supported by the message text.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

SUMMARY_EXTRACTION_SYSTEM_PROMPT = """You are LovelyChaos school-to-parent summary extraction engine.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
- Return only consolidated summary items relevant to the specific household context.
</output_contract>

<task>
Act as a parent with the provided children, grades, schools, enabled system defaults, user priority topics, and timezone.
Consolidate the supplied items into one final relevance judgment per item.
</task>

<grounding_rules>
- Base all output only on the supplied household profile, event facts, section snippets, and fallback candidates.
- Do not invent dates, times, schools, grades, actions, or priority matches.
- Preserve matched system defaults and matched user priorities only when they are supported by the inputs.
</grounding_rules>

<priority_rules>
- `important` means the family should see this prominently.
- `mentioned` means include it only as a compressed mention.
- `ignore` means drop it from the final summary.
- School closures and grade-relevant items should usually stay `important` when supported.
- Low-value admin details, duplicate operational details, and filler should become `mentioned` or `ignore`.
</priority_rules>

<format_rules>
- Emit one candidate per distinct summary-worthy item.
- Each candidate must include `consolidated_priority`, `matched_system_defaults`, `matched_user_priorities`, `has_date`, and `reason`.
- Keep text concise and non-duplicative.
- Include grade or child qualifiers only when they materially narrow relevance.
</format_rules>

<verification_loop>
Before finalizing:
- Check that every supported school-closure or grade-relevant important item is represented.
- Check that low-value admin items are not promoted to `important` without strong support.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

SUMMARY_COMPRESSION_SYSTEM_PROMPT = """You are LovelyChaos brevity-compression engine.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
- Do not introduce facts that are not present in the supplied summary candidates.
</output_contract>

<task>
Rewrite the supplied summary candidates into a short parent-facing brief.
The result must be concise, high-signal, and optimized for a fast email or SMS-style read.
</task>

<format_rules>
- Produce a short `title`.
- Put `important` dated items into `important_dates`.
- Put `important` undated items into `important_items`.
- Put `mentioned` items into `other_topics`.
- Use date-first wording when possible.
- Keep bullets compact and information-dense.
- Compress awareness/admin items to topic-like mentions.
- Remove greetings, filler, and generic adjectives.
- Include grade or child qualifiers only when they materially improve relevance.
- Keep the rendered content compatible with a closing conversational CTA about adding items to the calendar or asking for more info.
</format_rules>

<verification_loop>
Before finalizing:
- Check that all rendered lines come from supplied candidates.
- Check that no filler prose remains.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

EXTRACTION_JSON_SCHEMA = {
    "name": "event_extraction",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["events", "email_level_notes"],
        "properties": {
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "title",
                        "start_at",
                        "end_at",
                        "category",
                        "confidence",
                        "target_scope",
                        "mentioned_names",
                        "mentioned_schools",
                        "target_grades",
                        "preference_match",
                        "model_batch",
                        "model_reason",
                    ],
                    "properties": {
                        "title": {"type": "string"},
                        "start_at": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                        "end_at": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                        "category": {"type": "string"},
                        "confidence": {"type": "number"},
                        "target_scope": {
                            "type": "string",
                            "enum": [
                                "child_specific",
                                "grade_specific",
                                "school_specific",
                                "school_global",
                                "unknown",
                            ],
                        },
                        "mentioned_names": {"type": "array", "items": {"type": "string"}},
                        "mentioned_schools": {"type": "array", "items": {"type": "string"}},
                        "target_grades": {"type": "array", "items": {"type": "string"}},
                        "preference_match": {"type": "boolean"},
                        "model_batch": {"type": "string", "enum": ["A", "B", "C"]},
                        "model_reason": {"type": "string"},
                    },
                },
            },
            "email_level_notes": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        },
    },
    "strict": True,
}

COMMAND_JSON_SCHEMA = {
    "name": "command_parse",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "action",
            "pending_id",
            "topic",
            "preference_behavior",
            "minutes_before",
            "reminder_channel",
            "async_requested",
            "confidence",
        ],
        "properties": {
            "action": {"type": "string", "enum": ["add", "more_info", "delete", "remind", "set_preference", "none"]},
            "pending_id": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            "topic": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            "preference_behavior": {
                "anyOf": [{"type": "string", "enum": ["auto_add", "mention", "suppress"]}, {"type": "null"}]
            },
            "minutes_before": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            "reminder_channel": {"anyOf": [{"type": "string", "enum": ["sms", "calendar"]}, {"type": "null"}]},
            "async_requested": {"type": "boolean"},
            "confidence": {"type": "number"},
        },
    },
    "strict": True,
}

SUMMARY_EXTRACTION_JSON_SCHEMA = {
    "name": "summary_candidate_extraction",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["title", "candidates", "notes", "missing_requested_topics"],
        "properties": {
            "title": {"type": "string"},
            "candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "text",
                        "consolidated_priority",
                        "matched_system_defaults",
                        "matched_user_priorities",
                        "source_refs",
                        "applies_to",
                        "date_sort_key",
                        "has_date",
                        "reason",
                    ],
                    "properties": {
                        "text": {"type": "string"},
                        "consolidated_priority": {"type": "string", "enum": ["important", "mentioned", "ignore"]},
                        "matched_system_defaults": {"type": "array", "items": {"type": "string"}},
                        "matched_user_priorities": {"type": "array", "items": {"type": "string"}},
                        "source_refs": {"type": "array", "items": {"type": "string"}},
                        "applies_to": {"type": "array", "items": {"type": "string"}},
                        "date_sort_key": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                        "has_date": {"type": "boolean"},
                        "reason": {"type": "string"},
                    },
                },
            },
            "notes": {"type": "array", "items": {"type": "string"}},
            "missing_requested_topics": {"type": "array", "items": {"type": "string"}},
        },
    },
    "strict": True,
}

SUMMARY_COMPRESSION_JSON_SCHEMA = {
    "name": "summary_compression",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["title", "important_dates", "important_items", "other_topics", "missing_requested_topics", "notes"],
        "properties": {
            "title": {"type": "string"},
            "important_dates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["text", "source_refs", "applies_to", "date_sort_key"],
                    "properties": {
                        "text": {"type": "string"},
                        "source_refs": {"type": "array", "items": {"type": "string"}},
                        "applies_to": {"type": "array", "items": {"type": "string"}},
                        "date_sort_key": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    },
                },
            },
            "important_items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["text", "source_refs", "applies_to", "date_sort_key"],
                    "properties": {
                        "text": {"type": "string"},
                        "source_refs": {"type": "array", "items": {"type": "string"}},
                        "applies_to": {"type": "array", "items": {"type": "string"}},
                        "date_sort_key": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    },
                },
            },
            "other_topics": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["text", "source_refs", "applies_to", "date_sort_key"],
                    "properties": {
                        "text": {"type": "string"},
                        "source_refs": {"type": "array", "items": {"type": "string"}},
                        "applies_to": {"type": "array", "items": {"type": "string"}},
                        "date_sort_key": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    },
                },
            },
            "missing_requested_topics": {"type": "array", "items": {"type": "string"}},
            "notes": {"type": "array", "items": {"type": "string"}},
        },
    },
    "strict": True,
}


@dataclass
class ExtractedEvent:
    title: str
    start_at: Optional[datetime]
    end_at: Optional[datetime]
    category: str
    confidence: float
    target_scope: str = "unknown"
    mentioned_names: list[str] | None = None
    mentioned_schools: list[str] | None = None
    target_grades: list[str] | None = None
    preference_match: bool = False
    model_batch: str = "C"
    model_reason: str = ""


class DecisionEngine:
    def extract_events(
        self,
        body_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
    ) -> dict:
        raise NotImplementedError

    def parse_command(self, body_text: str) -> dict:
        raise NotImplementedError

    def extract_summary_candidates(self, summary_context: dict) -> dict:
        raise NotImplementedError

    def compress_summary(self, summary_context: dict) -> dict:
        raise NotImplementedError

    def metadata(self) -> dict:
        return {"provider": "mock", "model": "mock", "prompt_versions": {}}


class MockDecisionEngine(DecisionEngine):
    def extract_events(
        self,
        body_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
    ) -> dict:
        lowered = body_text.lower()
        category = "school_closure" if "closure" in lowered else "general"
        confidence = 0.4 if "unclear" in lowered else 0.92
        title = subject.strip() or "School Update"
        pref_match = "closure" in lowered and "closure" in (household_preferences or "").lower()

        match = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})", body_text)
        if match:
            start = datetime.fromisoformat(f"{match.group(1)}T{match.group(2)}:00+00:00")
        else:
            start = datetime.now(timezone.utc) + timedelta(days=1)
        end = start + timedelta(hours=1)
        event = ExtractedEvent(
            title=title,
            start_at=start,
            end_at=end,
            category=category,
            confidence=confidence,
            target_scope="school_global" if category == "school_closure" else "unknown",
            mentioned_names=[],
            mentioned_schools=[],
            target_grades=[],
            preference_match=pref_match,
            model_batch="A" if category == "school_closure" else "B",
            model_reason="mock_heuristic",
        )
        return {
            "events": [event],
            "email_level_notes": None,
        }

    def parse_command(self, body_text: str) -> dict:
        txt = body_text.lower()
        action = "none"
        preference_behavior = self._extract_preference_behavior(txt)
        if preference_behavior:
            action = "set_preference"
        elif "more info" in txt or "tell me more" in txt or "more details" in txt or "summarize this" in txt or "summarize " in txt:
            action = "more_info"
        elif "add event" in txt or "add to the calendar" in txt or "add this" in txt or "add" in txt:
            action = "add"
        elif "delete" in txt:
            action = "delete"
        elif "remind" in txt or "reminder" in txt:
            action = "remind"
        return {
            "action": action,
            "async_requested": "later" in txt,
            "pending_id": self._extract_int(txt),
            "topic": self._extract_topic(body_text),
            "preference_behavior": preference_behavior,
            "minutes_before": self._extract_minutes(txt),
            "reminder_channel": self._extract_reminder_channel(txt),
            "confidence": 0.9 if action != "none" else 0.4,
        }

    def extract_summary_candidates(self, summary_context: dict) -> dict:
        candidates = list(summary_context.get("fallback_candidates") or [])
        return {
            "title": summary_context.get("title_hint") or "School Update",
            "candidates": candidates,
            "notes": [],
            "missing_requested_topics": list(summary_context.get("missing_requested_topics") or []),
        }

    def compress_summary(self, summary_context: dict) -> dict:
        candidates = list(summary_context.get("candidates") or [])
        important_dates = [
            item for item in candidates if item.get("consolidated_priority") == "important" and item.get("has_date")
        ]
        important_items = [
            item for item in candidates if item.get("consolidated_priority") == "important" and not item.get("has_date")
        ]
        other_topics = [item for item in candidates if item.get("consolidated_priority") == "mentioned"]
        return {
            "title": summary_context.get("title_hint") or "School Update",
            "important_dates": important_dates[:8],
            "important_items": important_items[:5],
            "other_topics": other_topics[:4],
            "missing_requested_topics": list(summary_context.get("missing_requested_topics") or []),
            "notes": list(summary_context.get("notes") or []),
        }

    def metadata(self) -> dict:
        return {
            "provider": "mock",
            "model": "mock",
            "prompt_versions": {
                "extraction": EXTRACTION_PROMPT_VERSION,
                "command": COMMAND_PROMPT_VERSION,
                "summary_extract": SUMMARY_EXTRACTION_PROMPT_VERSION,
                "summary_compress": SUMMARY_COMPRESSION_PROMPT_VERSION,
            },
        }

    @staticmethod
    def _extract_int(text: str) -> Optional[int]:
        m = re.search(r"(\d+)", text)
        return int(m.group(1)) if m else None

    @staticmethod
    def _extract_minutes(text: str) -> int:
        m = re.search(r"(\d+)\s*(m|min|mins|minute|minutes)\b", text)
        if m:
            return int(m.group(1))
        return 60

    @staticmethod
    def _extract_reminder_channel(text: str) -> str:
        if "calendar" in text:
            return "calendar"
        return "sms"

    @staticmethod
    def _extract_topic(text: str) -> Optional[str]:
        raw = (text or "").strip()
        patterns = [
            r"(?:more info about|tell me more about|more details about)\s+(.+)$",
            r"(?:summarize|summary of)\s+(.+)$",
            r"(?:always add|always include|always mention)\s+(.+)$",
            r"(?:i care about)\s+(.+)$",
            r"(?:i don t care about|i don't care about|don t update me about|don't update me about|i don t need updates on|i don't need updates on)\s+(.+)$",
        ]
        for pattern in patterns:
            match = re.search(pattern, raw, re.IGNORECASE)
            if match:
                topic = match.group(1).strip().strip(".?!")
                topic = re.sub(r"\b(to the calendar|to cal)\b", "", topic, flags=re.IGNORECASE).strip()
                return topic or None
        return None

    @staticmethod
    def _extract_preference_behavior(text: str) -> Optional[str]:
        if re.search(r"\balways (?:add|include)\b", text):
            return "auto_add"
        if re.search(r"\b(?:i care about|always mention)\b", text):
            return "mention"
        if re.search(r"\b(?:i don t care about|i don't care about|don t update me about|don't update me about|i don t need updates on|i don't need updates on)\b", text):
            return "suppress"
        return None


class OpenAIDecisionEngine(MockDecisionEngine):
    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4.1-mini",
        timeout_sec: int = 60,
        base_url: str = "https://api.openai.com/v1",
    ):
        self.api_key = api_key
        self.model = model
        self.timeout_sec = timeout_sec
        self.base_url = base_url.rstrip("/")

    def extract_events(
        self,
        body_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
    ) -> dict:
        user_payload = (
            "household_preferences:\n"
            f"{household_preferences or ''}\n\n"
            "email_subject:\n"
            f"{subject}\n\n"
            "email_body:\n"
            f"{body_text}\n\n"
            "timezone_hint:\n"
            f"{timezone_hint}\n"
        )
        parsed = self._call_openai_json(
            system_prompt=EXTRACTION_SYSTEM_PROMPT,
            user_payload=user_payload,
            response_schema=EXTRACTION_JSON_SCHEMA,
        )
        events_raw = parsed.get("events") or []
        events: list[ExtractedEvent] = []
        for item in events_raw:
            start = self._parse_iso_or_none(item.get("start_at"))
            end = self._parse_iso_or_none(item.get("end_at"))
            if start and not end:
                end = start + timedelta(hours=1)
            if start and end and end < start:
                end = start + timedelta(hours=1)
            events.append(
                ExtractedEvent(
                    title=(item.get("title") or subject or "School Update").strip(),
                    start_at=start,
                    end_at=end,
                    category=(item.get("category") or "general").strip() or "general",
                    confidence=float(item.get("confidence") or 0.0),
                    target_scope=(item.get("target_scope") or "unknown"),
                    mentioned_names=list(item.get("mentioned_names") or []),
                    mentioned_schools=list(item.get("mentioned_schools") or []),
                    target_grades=list(item.get("target_grades") or []),
                    preference_match=bool(item.get("preference_match")),
                    model_batch=(item.get("model_batch") or "C"),
                    model_reason=(item.get("model_reason") or ""),
                )
            )
        return {
            "events": events,
            "email_level_notes": parsed.get("email_level_notes") if events else "empty_model_events",
        }

    def parse_command(self, body_text: str) -> dict:
        user_payload = f"message_body:\n{body_text}\n"
        parsed = self._call_openai_json(
            system_prompt=COMMAND_SYSTEM_PROMPT,
            user_payload=user_payload,
            response_schema=COMMAND_JSON_SCHEMA,
        )
        action = parsed.get("action") or "none"
        if action not in {"add", "more_info", "delete", "remind", "set_preference", "none"}:
            action = "none"
        minutes_before = parsed.get("minutes_before")
        if minutes_before is None:
            minutes_before = super()._extract_minutes(body_text.lower())
        reminder_channel = parsed.get("reminder_channel")
        if reminder_channel not in {"sms", "calendar"}:
            reminder_channel = super()._extract_reminder_channel(body_text.lower())
        topic = parsed.get("topic")
        if topic is None:
            topic = super()._extract_topic(body_text)
        preference_behavior = parsed.get("preference_behavior")
        if preference_behavior not in {"auto_add", "mention", "suppress"}:
            preference_behavior = super()._extract_preference_behavior(body_text.lower())
        return {
            "action": action,
            "pending_id": parsed.get("pending_id"),
            "topic": (str(topic).strip() if topic is not None else None) or None,
            "preference_behavior": preference_behavior,
            "minutes_before": int(minutes_before),
            "reminder_channel": reminder_channel,
            "async_requested": bool(parsed.get("async_requested", "later" in body_text.lower())),
            "confidence": float(parsed.get("confidence") or 0.0),
        }

    def extract_summary_candidates(self, summary_context: dict) -> dict:
        parsed = self._call_openai_json(
            system_prompt=SUMMARY_EXTRACTION_SYSTEM_PROMPT,
            user_payload=json.dumps(summary_context, ensure_ascii=True, indent=2),
            response_schema=SUMMARY_EXTRACTION_JSON_SCHEMA,
        )
        return {
            "title": (parsed.get("title") or "").strip(),
            "candidates": list(parsed.get("candidates") or []),
            "notes": [str(note) for note in list(parsed.get("notes") or []) if str(note).strip()],
            "missing_requested_topics": [
                str(topic) for topic in list(parsed.get("missing_requested_topics") or []) if str(topic).strip()
            ],
        }

    def compress_summary(self, summary_context: dict) -> dict:
        parsed = self._call_openai_json(
            system_prompt=SUMMARY_COMPRESSION_SYSTEM_PROMPT,
            user_payload=json.dumps(summary_context, ensure_ascii=True, indent=2),
            response_schema=SUMMARY_COMPRESSION_JSON_SCHEMA,
        )
        return {
            "title": (parsed.get("title") or "").strip(),
            "important_dates": list(parsed.get("important_dates") or []),
            "important_items": list(parsed.get("important_items") or []),
            "other_topics": list(parsed.get("other_topics") or []),
            "missing_requested_topics": [
                str(topic) for topic in list(parsed.get("missing_requested_topics") or []) if str(topic).strip()
            ],
            "notes": [str(note) for note in list(parsed.get("notes") or []) if str(note).strip()],
        }

    def metadata(self) -> dict:
        return {
            "provider": "openai",
            "model": self.model,
            "prompt_versions": {
                "extraction": EXTRACTION_PROMPT_VERSION,
                "command": COMMAND_PROMPT_VERSION,
                "summary_extract": SUMMARY_EXTRACTION_PROMPT_VERSION,
                "summary_compress": SUMMARY_COMPRESSION_PROMPT_VERSION,
            },
        }

    def _call_openai_json(self, system_prompt: str, user_payload: str, response_schema: dict) -> dict:
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is not configured")
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": response_schema,
            },
        }
        if not self.model.startswith("gpt-5"):
            payload["temperature"] = 0
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=self.timeout_sec) as client:
            response = client.post(f"{self.base_url}/chat/completions", headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        return self._loads_json(content)

    @staticmethod
    def _loads_json(content: str) -> dict:
        raw = content.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)

    @staticmethod
    def _parse_iso_or_none(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
