from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from app.services.content_analysis import AnalysisSection
from app.services.llm import DecisionEngine, ExtractedEvent
from app.services.priorities import school_closure_matches, topic_matches_text
from app.services.school_knowledge import retrieve_knowledge_context


GRADE_ALIAS_MAP = {
    "jk": ["jk", "junior kindergarten", "kindergarten", "primary"],
    "sk": ["sk", "senior kindergarten", "kindergarten", "primary"],
    "k": ["k", "kindergarten", "primary"],
    "1": ["grade 1", "gr 1", "gr1", "primary"],
    "2": ["grade 2", "gr 2", "gr2", "primary"],
    "3": ["grade 3", "gr 3", "gr3", "primary"],
    "4": ["grade 4", "gr 4", "gr4", "junior"],
    "5": ["grade 5", "gr 5", "gr5", "junior"],
    "6": ["grade 6", "gr 6", "gr6", "junior"],
    "7": ["grade 7", "gr 7", "gr7", "intermediate"],
    "8": ["grade 8", "gr 8", "gr8", "intermediate"],
}

GENERIC_SECTION_TERMS = ("hello ", "thank you", "wonderful", "incredible", "pleased to share", "dear ")
SUMMARY_MENTION_TERMS = (
    "safe arrival",
    "absence",
    "attendance",
    "weather",
    "clothing",
    "forms",
    "form",
    "school cash online",
    "volunteer",
    "school council",
    "heritage month",
    "awareness month",
    "spirit wear",
    "donation",
    "fundraising",
    "fundraiser",
    "food drive",
    "holiday hamper",
    "deadline",
    "reminder",
)
SUMMARY_IGNORE_TERMS = (
    "email address change window",
    "volunteer setup shift",
    "volunteer cleanup shift",
    "event-running shift",
    "donation drop-off window",
    "drop-off window",
)
SUMMARY_OVERLAP_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "by",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
RESCUE_EVENT_TERMS = (
    "join us",
    "save the date",
    "open house",
    "concert",
    "meeting",
    "math night",
    "movie night",
    "photo day",
    "family night",
    "night",
    "showcase",
)
SUMMARY_KEPT_SECTION_TEXT_LIMIT = 900
SUMMARY_KEPT_SECTION_LIMIT = 6
SUMMARY_NOTES_LIMIT = 4
SUMMARY_PROMPT_EXAMPLE_LIMIT = 3
SUMMARY_PROMPT_SNIPPET_LIMIT = 220

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
DATE_PATTERN = re.compile(
    r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)?\s*,?\s*"
    r"(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\.?\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?(?:,\s*(?P<year>\d{4}))?",
    re.I,
)
TIME_PATTERN = re.compile(r"\b(\d{1,2}:\d{2}\s*(?:am|pm))\b", re.I)


@dataclass
class SummaryLine:
    text: str
    source_refs: list[str]
    applies_to: list[str]
    date_sort_key: Optional[str] = None

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class SummaryResult:
    title: str
    important_info: list[SummaryLine]
    other_dates: list[SummaryLine]
    other_topics: list[SummaryLine]
    missing_requested_topics: list[str]
    notes: list[str]
    assistant_intro: str
    rendered_message: str

    def as_dict(self) -> dict:
        return {
            "title": self.title,
            "important_info": [line.as_dict() for line in self.important_info],
            "other_dates": [line.as_dict() for line in self.other_dates],
            "other_topics": [line.as_dict() for line in self.other_topics],
            "missing_requested_topics": list(self.missing_requested_topics),
            "notes": list(self.notes),
            "assistant_intro": self.assistant_intro,
            "rendered_message": self.rendered_message,
        }


def build_brief_summary(
    *,
    engine: DecisionEngine,
    subject: str,
    timezone_name: str,
    household_preferences: str,
    system_defaults: Optional[dict[str, bool]] = None,
    user_priority_topics: Optional[list[str]] = None,
    suppressed_priority_topics: Optional[list[str]] = None,
    children: list,
    extracted_events: list[ExtractedEvent],
    per_event_outcomes: list[dict],
    sections: list[AnalysisSection],
    analysis_text: str,
    chunk_notes: list[str],
    informational_only: Optional[bool] = None,
    reference_datetime_hint: str = "",
    document_understanding: Optional[dict] = None,
) -> tuple[SummaryResult, dict]:
    summary_context = _prefilter_summary_context(
        subject=subject,
        timezone_name=timezone_name,
        household_preferences=household_preferences,
        system_defaults=system_defaults or {"school_closures": True, "grade_relevant": True},
        user_priority_topics=user_priority_topics or [],
        suppressed_priority_topics=suppressed_priority_topics or [],
        children=children,
        extracted_events=extracted_events,
        per_event_outcomes=per_event_outcomes,
        sections=sections,
        analysis_text=analysis_text,
        chunk_notes=chunk_notes,
        reference_datetime_hint=reference_datetime_hint,
        document_understanding=document_understanding,
    )
    knowledge_context = retrieve_knowledge_context(
        subject=subject,
        sections=sections,
        analysis_text=analysis_text,
        extracted_events=extracted_events,
        max_matches=SUMMARY_PROMPT_EXAMPLE_LIMIT,
    )
    prompt_examples = _trim_summary_prompt_examples(knowledge_context.retrieved_examples)

    fallback_candidates = list(summary_context["fallback_candidates"])
    try:
        extracted_summary = engine.extract_summary_candidates(
            {
                "title_hint": summary_context["title_hint"],
                "household_context": summary_context["household_context"],
                "enabled_system_defaults": summary_context["enabled_system_defaults"],
                "user_priority_topics": summary_context["user_priority_topics"],
                "event_facts": summary_context["event_facts"],
                "kept_sections": summary_context["kept_sections"],
                "notes": summary_context["notes"],
                "missing_requested_topics": summary_context["missing_requested_topics"],
                "fallback_candidates": fallback_candidates,
                "domain_taxonomy_hints": knowledge_context.matched_topics,
                "retrieved_examples": prompt_examples,
                "retrieval_notes": knowledge_context.retrieval_notes,
                "matched_event_types": knowledge_context.matched_event_types,
                "commonness_hints": knowledge_context.commonness_hints,
            }
        )
    except Exception:
        extracted_summary = {
            "title": summary_context["title_hint"],
            "candidates": fallback_candidates,
            "notes": list(summary_context["notes"]),
            "missing_requested_topics": list(summary_context["missing_requested_topics"]),
        }

    merged_candidates = _prune_redundant_dated_candidates(
        _merge_summary_candidates(fallback_candidates, extracted_summary.get("candidates") or [])
    )
    merged_candidates = _filter_suppressed_candidates(merged_candidates, summary_context["suppressed_priority_topics"])
    merged_candidates = _filter_mismatched_grade_candidates(merged_candidates, summary_context["household_context"]["grades"])
    extracted_notes = [str(note) for note in list(extracted_summary.get("notes") or []) if str(note).strip()]
    combined_notes = _dedupe_strings(list(summary_context["notes"]) + extracted_notes)[:SUMMARY_NOTES_LIMIT]
    extracted_missing = [
        str(topic) for topic in list(extracted_summary.get("missing_requested_topics") or []) if str(topic).strip()
    ]

    try:
        compressed_summary = engine.compress_summary(
            {
                "title_hint": extracted_summary.get("title") or summary_context["title_hint"],
                "household_context": summary_context["household_context"],
                "suppressed_priority_topics": summary_context["suppressed_priority_topics"],
                "candidates": merged_candidates,
                "missing_requested_topics": _dedupe_strings(
                    list(summary_context["missing_requested_topics"]) + extracted_missing
                ),
                "notes": combined_notes,
                "domain_taxonomy_hints": knowledge_context.matched_topics,
                "retrieved_examples": prompt_examples,
                "retrieval_notes": knowledge_context.retrieval_notes,
                "matched_event_types": knowledge_context.matched_event_types,
                "commonness_hints": knowledge_context.commonness_hints,
            }
        )
    except Exception:
        compressed_summary = _compress_summary_deterministically(
            extracted_summary.get("title") or summary_context["title_hint"],
            merged_candidates,
            _dedupe_strings(list(summary_context["missing_requested_topics"]) + extracted_missing),
            combined_notes,
            timezone_name,
        )
    compressed_summary = _ensure_dated_candidate_coverage(compressed_summary, merged_candidates, timezone_name)
    compressed_summary = _normalize_compressed_summary(compressed_summary, timezone_name)
    compressed_summary = _upgrade_rendered_summary_with_candidates(compressed_summary, merged_candidates, timezone_name)
    compressed_summary = _normalize_compressed_summary(compressed_summary, timezone_name)

    if informational_only is None:
        informational_only = not list(compressed_summary.get("important_info") or [])

    result = _summary_result_from_dict(
        compressed_summary,
        summary_context["title_hint"],
        informational_only=informational_only,
        assistant_intro=summary_context["assistant_intro"],
    )
    audit_payload = {
        "input_context": {
            "grades": summary_context["household_context"]["grades"],
            "children": summary_context["household_context"]["children"],
            "schools": summary_context["household_context"]["schools"],
            "preferences": household_preferences,
            "user_priority_topics": summary_context["user_priority_topics"],
            "suppressed_priority_topics": summary_context["suppressed_priority_topics"],
            "enabled_system_defaults": summary_context["enabled_system_defaults"],
            "timezone": timezone_name,
            "document_understanding": document_understanding,
        },
        "prefilter": {
            "kept_sections": summary_context["kept_sections"],
            "dropped_sections": summary_context["dropped_sections"],
            "kept_event_titles": [fact["title"] for fact in summary_context["event_facts"]],
        },
        "knowledge_retrieval": knowledge_context.as_audit_dict(),
        "consolidated_priority_items": merged_candidates,
        "final_summary": result.as_dict(),
    }
    return result, audit_payload


def _prefilter_summary_context(
    *,
    subject: str,
    timezone_name: str,
    household_preferences: str,
    system_defaults: dict[str, bool],
    user_priority_topics: list[str],
    suppressed_priority_topics: list[str],
    children: list,
    extracted_events: list[ExtractedEvent],
    per_event_outcomes: list[dict],
    sections: list[AnalysisSection],
    analysis_text: str,
    chunk_notes: list[str],
    reference_datetime_hint: str = "",
    document_understanding: Optional[dict] = None,
) -> dict:
    child_context = _build_child_context(children)
    enabled_system_defaults = sorted([key for key, enabled in system_defaults.items() if enabled])
    normalized_user_topics = _dedupe_strings([item.strip() for item in user_priority_topics if item.strip()])
    normalized_suppressed_topics = _dedupe_strings([item.strip() for item in suppressed_priority_topics if item.strip()])

    event_facts: list[dict] = []
    for idx, event in enumerate(extracted_events):
        outcome = per_event_outcomes[idx] if idx < len(per_event_outcomes) else {}
        execution_disposition = str(outcome.get("execution_disposition") or "")
        relevancy_evidence = dict(outcome.get("relevancy_evidence") or {})
        routed_preference_evaluated = "preference_match" in relevancy_evidence
        routed_suppression_evaluated = "suppressed_match" in outcome

        applies_to = _applies_to_for_event(event, child_context)
        matched_system_defaults = _matched_system_defaults(event, applies_to, child_context, enabled_system_defaults)
        routed_positive_matches = _dedupe_strings(
            [str(item).strip() for item in list(outcome.get("matched_positive_topics") or []) if str(item).strip()]
        )
        routed_suppressed_matches = _dedupe_strings(
            [str(item).strip() for item in list(outcome.get("matched_suppressed_topics") or []) if str(item).strip()]
        )
        matched_user_priorities = (
            routed_positive_matches
            if routed_preference_evaluated
            else _matched_user_priorities(event, normalized_user_topics)
        )
        matched_suppressed_priorities = (
            routed_suppressed_matches
            if routed_suppression_evaluated
            else _matched_user_priorities(event, normalized_suppressed_topics)
        )
        consolidated_priority = _determine_consolidated_priority(
            event=event,
            execution_disposition=execution_disposition,
            matched_system_defaults=matched_system_defaults,
            matched_user_priorities=matched_user_priorities,
            matched_suppressed_priorities=matched_suppressed_priorities,
        )
        if consolidated_priority == "ignore":
            continue

        event_facts.append(
            {
                "title": event.title,
                "start_at": _serialize_dt(event.start_at),
                "end_at": _serialize_dt(event.end_at),
                "has_date": event.start_at is not None,
                "applies_to": applies_to,
                "source_refs": [f"event:{event.title}"],
                "reason": event.model_reason or "event_fact",
                "target_scope": event.target_scope,
                "category": event.category,
                "execution_disposition": execution_disposition,
                "matched_system_defaults": matched_system_defaults,
                "matched_user_priorities": matched_user_priorities,
                "matched_suppressed_priorities": matched_suppressed_priorities,
                "preference_matching_mode": "llm" if routed_preference_evaluated or routed_suppression_evaluated else "deterministic",
                "consolidated_priority": consolidated_priority,
            }
        )

    kept_sections: list[dict] = []
    dropped_sections: list[dict] = []
    preference_terms = [_normalize_text(item) for item in normalized_user_topics]
    for section in sections:
        lowered = _normalize_text(section.text)
        keep = (
            any(term in lowered for term in preference_terms)
            or any(alias in lowered for alias in child_context["grade_aliases"])
            or any(keyword in lowered for keyword in SUMMARY_MENTION_TERMS)
        )
        payload = {
            "section_index": section.index,
            "label": section.label,
            "section_kind": section.section_kind,
            "priority_score": section.priority_score,
            "source_kind": section.source_kind,
            "char_count": len(section.text),
            "text": section.text[:SUMMARY_KEPT_SECTION_TEXT_LIMIT],
        }
        if keep and not any(term in lowered for term in GENERIC_SECTION_TERMS):
            kept_sections.append(payload)
        else:
            dropped_sections.append({k: v for k, v in payload.items() if k != "text"})

    rescued_sections: list[dict] = []
    if not event_facts and not kept_sections:
        rescued_sections = _rescue_high_signal_sections(sections)
        for rescued in rescued_sections:
            kept_sections.append(rescued)
            dropped_sections = [
                item
                for item in dropped_sections
                if item.get("section_index") != rescued.get("section_index")
            ]

    kept_sections = kept_sections[:SUMMARY_KEPT_SECTION_LIMIT]
    notes = [note for note in chunk_notes if note and note != "empty_model_events"]
    if rescued_sections:
        notes.append("summary rescue used high-signal event sections")
    notes.extend(_document_understanding_notes(document_understanding))
    notes = _dedupe_strings(notes)[:SUMMARY_NOTES_LIMIT]
    title_hint = _summary_title_hint(subject, child_context["schools"])
    fallback_candidates = _deterministic_candidates(
        timezone_name=timezone_name,
        event_facts=event_facts,
        kept_sections=kept_sections,
        analysis_text=analysis_text,
        user_priority_topics=normalized_user_topics,
        suppressed_priority_topics=normalized_suppressed_topics,
        rescued_sections=rescued_sections,
        title_hint=title_hint,
        reference_datetime_hint=reference_datetime_hint,
    )
    fallback_candidates = _merge_summary_candidates(
        fallback_candidates,
        _document_understanding_candidates(document_understanding),
    )
    missing_requested_topics = _detect_missing_requested_topics(normalized_user_topics, analysis_text, fallback_candidates)
    return {
        "title_hint": title_hint,
        "assistant_intro": _document_assistant_intro(document_understanding),
        "household_context": {
            "children": child_context["children"],
            "grades": child_context["grades"],
            "schools": child_context["schools"],
            "timezone": timezone_name,
        },
        "enabled_system_defaults": enabled_system_defaults,
        "user_priority_topics": normalized_user_topics,
        "suppressed_priority_topics": normalized_suppressed_topics,
        "event_facts": event_facts,
        "kept_sections": kept_sections,
        "dropped_sections": dropped_sections,
        "notes": notes,
        "fallback_candidates": fallback_candidates,
        "missing_requested_topics": missing_requested_topics,
    }


def _document_assistant_intro(document_understanding: Optional[dict]) -> str:
    if not isinstance(document_understanding, dict):
        return ""
    return str(document_understanding.get("assistant_intro") or "").strip()


def _document_understanding_notes(document_understanding: Optional[dict]) -> list[str]:
    if not isinstance(document_understanding, dict):
        return []
    notes = [str(note).strip() for note in list(document_understanding.get("notes") or []) if str(note).strip()]
    assistant_summary = str(document_understanding.get("assistant_summary") or "").strip()
    if assistant_summary:
        notes.insert(0, assistant_summary)
    return _dedupe_strings(notes)[:SUMMARY_NOTES_LIMIT]


def _document_understanding_candidates(document_understanding: Optional[dict]) -> list[dict]:
    if not isinstance(document_understanding, dict):
        return []
    candidates: list[dict] = []
    for bucket_name, priority in (("actionable_topics", "important"), ("informational_topics", "mentioned")):
        for index, topic in enumerate(list(document_understanding.get(bucket_name) or []), start=1):
            if not isinstance(topic, dict):
                continue
            title = str(topic.get("title") or "").strip()
            why_it_matters = str(topic.get("why_it_matters") or "").strip()
            action_hint = str(topic.get("action_hint") or "").strip()
            timing_hint = str(topic.get("timing_hint") or "").strip()
            if not title:
                continue
            text = title
            if timing_hint:
                text = f"{text}: {timing_hint}"
            elif why_it_matters:
                text = f"{text}: {why_it_matters}"
            candidates.append(
                {
                    "text": text,
                    "consolidated_priority": priority,
                    "matched_system_defaults": [],
                    "matched_user_priorities": [],
                    "source_refs": [f"document_understanding:{bucket_name}:{index}"],
                    "applies_to": [],
                    "date_sort_key": None,
                    "has_date": False,
                    "reason": why_it_matters or action_hint or "document_understanding",
                }
            )
    return candidates


def _trim_summary_prompt_examples(retrieved_examples: list[dict]) -> list[dict]:
    trimmed: list[dict] = []
    for example in list(retrieved_examples or [])[:SUMMARY_PROMPT_EXAMPLE_LIMIT]:
        if not isinstance(example, dict):
            continue
        trimmed.append(
            {
                "entry_id": str(example.get("entry_id") or "").strip(),
                "doc_type": str(example.get("doc_type") or "").strip(),
                "scope": str(example.get("scope") or "").strip(),
                "topics": list(example.get("topics") or []),
                "event_types": list(example.get("event_types") or []),
                "commonness": str(example.get("commonness") or "").strip(),
                "action_required": str(example.get("action_required") or "").strip(),
                "audience": str(example.get("audience") or "").strip(),
                "snippet": str(example.get("snippet") or "").strip()[:SUMMARY_PROMPT_SNIPPET_LIMIT],
            }
        )
    return trimmed


def _matched_system_defaults(
    event: ExtractedEvent,
    applies_to: list[str],
    child_context: dict,
    enabled_system_defaults: list[str],
) -> list[str]:
    matches: list[str] = []
    text_values = [
        event.title,
        event.category,
        event.model_reason,
        " ".join(event.target_grades or []),
        " ".join(event.mentioned_names or []),
    ]
    if "school_closures" in enabled_system_defaults and school_closure_matches(*text_values):
        matches.append("school_closures")
    if "grade_relevant" in enabled_system_defaults and _grade_relevant_match(event, applies_to, child_context):
        matches.append("grade_relevant")
    return matches


def _matched_user_priorities(event: ExtractedEvent, user_priority_topics: list[str]) -> list[str]:
    matches: list[str] = []
    for topic in user_priority_topics:
        if topic_matches_text(
            topic,
            event.title,
            event.category,
            event.model_reason,
            " ".join(event.target_grades or []),
        ):
            matches.append(topic)
    return matches


def _grade_relevant_match(event: ExtractedEvent, applies_to: list[str], child_context: dict) -> bool:
    child_names = set(child_context["child_names"])
    if any(_normalize_text(name) in child_names for name in list(event.mentioned_names or [])):
        return True
    event_grades = {str(value).strip() for value in list(event.target_grades or []) if str(value).strip()}
    if event_grades & set(child_context["grades"]):
        return True
    if event.target_scope == "child_specific":
        return True
    return False


def _determine_consolidated_priority(
    *,
    event: ExtractedEvent,
    execution_disposition: str,
    matched_system_defaults: list[str],
    matched_user_priorities: list[str],
    matched_suppressed_priorities: list[str],
) -> str:
    normalized = _normalize_text(f"{event.title} {event.model_reason}")
    if any(term in normalized for term in SUMMARY_IGNORE_TERMS):
        return "ignore"
    if matched_suppressed_priorities and not matched_system_defaults:
        return "ignore"
    if matched_system_defaults or matched_user_priorities:
        return "important"
    if execution_disposition in {"create_event", "followup_available", "informational_item"} and _is_mention_worthy(event):
        return "mentioned"
    return "ignore"


def _is_mention_worthy(event: ExtractedEvent) -> bool:
    normalized = _normalize_text(f"{event.title} {event.category} {event.model_reason}")
    if any(term in normalized for term in SUMMARY_IGNORE_TERMS):
        return False
    return any(term in normalized for term in SUMMARY_MENTION_TERMS)


def _deterministic_candidates(
    *,
    timezone_name: str,
    event_facts: list[dict],
    kept_sections: list[dict],
    analysis_text: str,
    user_priority_topics: list[str],
    suppressed_priority_topics: list[str],
    rescued_sections: list[dict],
    title_hint: str,
    reference_datetime_hint: str = "",
) -> list[dict]:
    candidates: list[dict] = []
    for event in event_facts:
        text = _summary_text_for_event(event, timezone_name)
        if not text:
            continue
        candidates.append(
            {
                "text": text,
                "consolidated_priority": event["consolidated_priority"],
                "matched_system_defaults": list(event["matched_system_defaults"]),
                "matched_user_priorities": list(event["matched_user_priorities"]),
                "source_refs": list(event["source_refs"]),
                "applies_to": list(event["applies_to"]),
                "date_sort_key": event["start_at"],
                "has_date": bool(event["has_date"]),
                "reason": event["reason"],
            }
        )

    candidates.extend(_dated_candidates_from_sections(kept_sections, timezone_name, reference_datetime_hint))

    normalized_text = _normalize_text(analysis_text)
    topic_patterns = [
        ("Safe arrival/absence procedures", ("safe arrival", "absence", "attendance")),
        ("Weather-appropriate clothing", ("weather", "clothing", "cold weather")),
        ("Volunteering or event support", ("volunteer", "wristband")),
        ("School council updates", ("school council", "executive results")),
        ("Fundraising or donation updates", ("donation", "fundraising", "fundraiser", "food drive", "holiday hamper")),
        ("Heritage months mentioned", ("heritage month", "heritage months")),
        ("Awareness months mentioned", ("awareness month", "awareness months")),
    ]
    for label, tokens in topic_patterns:
        if all(token not in normalized_text for token in tokens):
            continue
        candidates.append(
            {
                "text": label,
                "consolidated_priority": "mentioned",
                "matched_system_defaults": [],
                "matched_user_priorities": [],
                "source_refs": [f"topic:{_normalize_text(label).replace(' ', '_')}"],
                "applies_to": [],
                "date_sort_key": None,
                "has_date": False,
                "reason": "deterministic_topic_match",
            }
        )

    candidates.extend(_rescue_candidates_from_sections(rescued_sections, title_hint, timezone_name))

    for topic in user_priority_topics:
        normalized_topic = _normalize_text(topic)
        if not normalized_topic or normalized_topic not in normalized_text:
            continue
        if any(normalized_topic in _normalize_text(item.get("text", "")) for item in candidates):
            continue
        if normalized_topic and normalized_topic in normalized_text:
            candidates.append(
                {
                    "text": topic,
                    "consolidated_priority": "mentioned",
                    "matched_system_defaults": [],
                    "matched_user_priorities": [topic],
                    "source_refs": [f"preference:{normalized_topic.replace(' ', '_')}"],
                    "applies_to": [],
                    "date_sort_key": None,
                    "has_date": False,
                    "reason": "requested_topic_found",
                }
            )
    merged = _prune_redundant_dated_candidates(_merge_summary_candidates([], candidates))
    return _filter_suppressed_candidates(merged, suppressed_priority_topics)


def _compress_summary_deterministically(
    title: str,
    candidates: list[dict],
    missing_requested_topics: list[str],
    notes: list[str],
    timezone_name: str,
) -> dict:
    important_info = [candidate for candidate in candidates if candidate.get("consolidated_priority") == "important"]
    mentioned = [candidate for candidate in candidates if candidate.get("consolidated_priority") == "mentioned"]
    other_dates = [candidate for candidate in mentioned if candidate.get("has_date")]
    other_topics = [candidate for candidate in mentioned if not candidate.get("has_date")]

    return {
        "title": title,
        "important_info": _merge_repeated_priority_lines(important_info, timezone_name),
        "other_dates": _dedupe_lines(other_dates),
        "other_topics": _dedupe_lines(other_topics)[:4],
        "missing_requested_topics": _dedupe_strings(missing_requested_topics),
        "notes": _dedupe_strings(notes),
    }


def _summary_result_from_dict(
    payload: dict,
    fallback_title: str,
    *,
    informational_only: bool = False,
    assistant_intro: str = "",
) -> SummaryResult:
    title = (payload.get("title") or fallback_title or "School Update").strip()
    important_info = [_line_from_dict(item) for item in list(payload.get("important_info") or []) if item.get("text")]
    other_dates = [_line_from_dict(item) for item in list(payload.get("other_dates") or []) if item.get("text")]
    other_topics = [_line_from_dict(item) for item in list(payload.get("other_topics") or []) if item.get("text")]
    missing_requested_topics = _dedupe_strings(list(payload.get("missing_requested_topics") or []))
    notes = _dedupe_strings(list(payload.get("notes") or []))
    rendered = _render_summary(
        title=title,
        important_info=important_info,
        other_dates=other_dates,
        other_topics=other_topics,
        missing_requested_topics=missing_requested_topics,
        informational_only=informational_only,
        assistant_intro=assistant_intro,
    )
    return SummaryResult(
        title=title,
        important_info=important_info,
        other_dates=other_dates,
        other_topics=other_topics,
        missing_requested_topics=missing_requested_topics,
        notes=notes,
        assistant_intro=assistant_intro,
        rendered_message=rendered,
    )


def _line_from_dict(item: dict) -> SummaryLine:
    return SummaryLine(
        text=str(item.get("text") or "").strip(),
        source_refs=[str(ref) for ref in list(item.get("source_refs") or []) if str(ref).strip()],
        applies_to=[str(value) for value in list(item.get("applies_to") or []) if str(value).strip()],
        date_sort_key=str(item.get("date_sort_key")) if item.get("date_sort_key") else None,
    )


def _render_summary(
    *,
    title: str,
    important_info: list[SummaryLine],
    other_dates: list[SummaryLine],
    other_topics: list[SummaryLine],
    missing_requested_topics: list[str],
    informational_only: bool = False,
    assistant_intro: str = "",
) -> str:
    if not important_info and not other_dates and not other_topics:
        message = (
            "I saved this as an informational school update, but I couldn't produce a cleaner recap yet."
            if informational_only
            else "I found a school update but couldn't extract a clean summary. Reply if you want me to summarize this one manually."
        )
        lines = [title]
        if assistant_intro:
            lines.extend(["", f"Brief: {assistant_intro}"])
        lines.extend(["", message])
        return "\n".join(lines)
    lines = [title]
    if assistant_intro:
        lines.extend(["", f"Brief: {assistant_intro}"])
    if important_info:
        lines.extend(["", "Important Info"])
        lines.extend(f"- {item.text}" for item in important_info)
    if other_dates:
        lines.extend(["", "Other Dates Mentioned"])
        lines.extend(f"- {item.text}" for item in other_dates)
    if other_topics:
        lines.extend(["", "Other Logistics / Topics Mentioned"])
        lines.extend(f"- {item.text}" for item in other_topics)
    lines.extend(
        [
            "",
            (
                "This looks informational only, so I saved the key details without queuing any follow-up."
                if informational_only
                else "Let me know if you want me to add any of these to the calendar or want more info on any topic mentioned."
            ),
        ]
    )
    return "\n".join(lines)


def _summary_text_for_event(event: dict, timezone_name: str) -> str:
    title = (event.get("title") or "").strip()
    if not title:
        return ""
    start_at = _coerce_datetime(event.get("start_at"))
    end_at = _coerce_datetime(event.get("end_at"))
    applies_to = list(event.get("applies_to") or [])
    qualifier = f"{', '.join(applies_to)} " if applies_to else ""
    label = title
    if qualifier and not title.lower().startswith(qualifier.lower()):
        label = f"{qualifier}{title}"
    if start_at is None:
        return label

    start_local = _to_user_timezone(start_at, timezone_name)
    end_local = _to_user_timezone(end_at, timezone_name) if end_at else None
    date_label = f"{start_local.strftime('%b')} {start_local.day}"
    if _is_single_day_all_day_window(start_local, end_local):
        return f"{date_label}: {label}"
    if end_local and start_local.date() != end_local.date():
        end_label = f"{end_local.strftime('%b')} {end_local.day}"
        return f"{date_label}-{end_label}: {label}"
    if start_local.hour == 0 and start_local.minute == 0:
        return f"{date_label}: {label}"
    time_label = _time_range_label(start_local, end_local)
    if time_label:
        return f"{date_label}: {label} ({time_label})"
    return f"{date_label}: {label}"


def _dated_candidates_from_sections(
    kept_sections: list[dict],
    timezone_name: str,
    reference_datetime_hint: str = "",
) -> list[dict]:
    candidates: list[dict] = []
    for section in kept_sections:
        label = str(section.get("label") or "")
        source_ref = f"section:{section['section_index']}" if section.get("section_index") is not None else "section:unknown"
        for raw_line in str(section.get("text") or "").splitlines():
            candidate = _candidate_from_dated_section_line(
                raw_line,
                label,
                source_ref,
                timezone_name,
                reference_datetime_hint,
            )
            if candidate is not None:
                candidates.append(candidate)
    return candidates


def _candidate_from_dated_section_line(
    line: str,
    section_label: str,
    source_ref: str,
    timezone_name: str,
    reference_datetime_hint: str = "",
) -> Optional[dict]:
    cleaned_line = re.sub(r"\s+", " ", line or "").strip(" -*\t")
    if not cleaned_line:
        return None
    normalized_line = _normalize_text(cleaned_line)
    if normalized_line.startswith("page ") or normalized_line.startswith("original email date"):
        return None

    title = _dated_line_title(cleaned_line, section_label)
    if not title:
        return None
    if _looks_like_low_quality_dated_title(title):
        return None

    zone = _safe_zoneinfo(timezone_name)
    start_local, end_local = _section_line_window(
        cleaned_line,
        zone,
        _reference_datetime_for_summary(reference_datetime_hint, timezone_name),
    )
    if start_local is None:
        return None
    start_at = start_local.astimezone(timezone.utc)
    end_at = end_local.astimezone(timezone.utc) if end_local else None
    text = _summary_text_for_event(
        {
            "title": title,
            "start_at": _serialize_dt(start_at),
            "end_at": _serialize_dt(end_at),
            "applies_to": [],
        },
        timezone_name,
    )
    return {
        "text": text,
        "consolidated_priority": "mentioned",
        "matched_system_defaults": [],
        "matched_user_priorities": [],
        "source_refs": [source_ref],
        "applies_to": [],
        "date_sort_key": _serialize_dt(start_at),
        "has_date": True,
        "reason": "dated_section_match",
    }


def _dated_line_title(line: str, section_label: str) -> str:
    sections = [segment.strip(" -:\t") for segment in re.split(r"\s+-\s+", line) if segment.strip(" -:\t")]
    if len(sections) >= 2:
        left, right = sections[0], sections[-1]
        left_has_date = bool(DATE_PATTERN.search(left))
        right_has_date = bool(DATE_PATTERN.search(right))
        if left_has_date and not right_has_date:
            title = _clean_dated_title_fragment(right)
            if title:
                return title
        if right_has_date and not left_has_date:
            title = _clean_dated_title_fragment(left)
            if title:
                return title

    match = DATE_PATTERN.search(line)
    if match:
        before = _clean_dated_title_fragment(line[: match.start()])
        after = _clean_dated_title_fragment(line[match.end() :])
        if before and not _looks_like_generic_summary_fragment(before):
            return before
        if after and not _looks_like_generic_summary_fragment(after):
            return after

    fallback = _clean_section_title(section_label)
    if fallback and not _looks_like_generic_summary_fragment(fallback):
        return fallback
    return ""


def _clean_dated_title_fragment(value: str) -> str:
    cleaned = re.sub(r"https?://\S+", "", value or "", flags=re.I)
    cleaned = re.sub(r"\b\S+\.(?:ca|com|org|net|edu)(?:/\S*)?\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^\s*(?:to|-)\s*\d{1,2}(?:st|nd|rd|th)?\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^\s*\d{1,2}(?:st|nd|rd|th)?\s+is\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b,?", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\b(?:when|date|today|register today)\b\s*:?", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^(?:this is a friendly reminder that|friendly reminder that|please note that)\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(
        r"^from\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)?(?:\s*(?:-|to)\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)?)?",
        "",
        cleaned,
        flags=re.I,
    )
    if re.search(r"\bregistration deadline\b", cleaned, re.I):
        cleaned = re.sub(r"^.*?\b(registration deadline\b.*)$", r"\1", cleaned, flags=re.I)
    if re.search(r"\bbegins\b", cleaned, re.I):
        cleaned = re.sub(r"\bbegins\b.*$", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\blearn more\b.*$", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\bwebsite\b$", "", cleaned, flags=re.I)
    cleaned = re.sub(r"[^\w\s&'/()+.-]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -:,.")
    return _humanize_event_title(cleaned) if cleaned else ""


def _clean_section_title(label: str) -> str:
    cleaned = re.sub(r"\s+", " ", label or "").strip(" -:\t")
    cleaned = re.sub(r"\bPage\s+\d+:\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\sis\b.*$", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s+-\s+.*$", "", cleaned)
    cleaned = cleaned.strip(" -:,.")
    return _humanize_event_title(cleaned) if cleaned else ""


def _looks_like_generic_summary_fragment(value: str) -> bool:
    normalized = _normalize_text(value)
    if not normalized:
        return True
    if normalized in {"kind regards", "regards", "for full program details please see the attached flyer"}:
        return True
    if normalized.startswith("page ") or normalized in {"when", "date", "today", "registration deadline"}:
        return True
    return bool(re.match(r"^from \d", normalized))


def _looks_like_low_quality_dated_title(value: str) -> bool:
    normalized = _normalize_text(value)
    if not normalized:
        return True
    if normalized.endswith(" is on"):
        return True
    if normalized.startswith("over the next "):
        return True
    if normalized.startswith("students in "):
        return True
    if normalized.startswith("the frankland ") and normalized.endswith(" run is on"):
        return True
    if "students in" in normalized and len(normalized.split()) > 8:
        return True
    return False


def _ensure_dated_candidate_coverage(payload: dict, candidates: list[dict], timezone_name: str) -> dict:
    important_info = list(payload.get("important_info") or [])
    other_dates = list(payload.get("other_dates") or [])
    other_topics = list(payload.get("other_topics") or [])
    rendered = important_info + other_dates + other_topics
    missing_important: list[dict] = []
    missing_other: list[dict] = []

    for candidate in candidates:
        if not candidate.get("has_date"):
            continue
        if candidate.get("consolidated_priority") not in {"important", "mentioned"}:
            continue
        if _dated_candidate_is_rendered(candidate, rendered):
            continue
        restored = {
            "text": str(candidate.get("text") or "").strip(),
            "source_refs": [str(ref) for ref in list(candidate.get("source_refs") or []) if str(ref).strip()],
            "applies_to": [str(value) for value in list(candidate.get("applies_to") or []) if str(value).strip()],
            "date_sort_key": str(candidate.get("date_sort_key")) if candidate.get("date_sort_key") else None,
        }
        if candidate.get("consolidated_priority") == "important":
            missing_important.append(restored)
        else:
            missing_other.append(restored)

    notes = list(payload.get("notes") or [])
    if missing_important or missing_other:
        notes.append("summary coverage restored missing dated item(s)")

    return {
        "title": payload.get("title") or "",
        "important_info": _merge_repeated_priority_lines(important_info + missing_important, timezone_name),
        "other_dates": _dedupe_lines(other_dates + missing_other),
        "other_topics": _dedupe_lines(other_topics),
        "missing_requested_topics": _dedupe_strings(list(payload.get("missing_requested_topics") or [])),
        "notes": _dedupe_strings(notes),
    }


def _item_content_subsumed_by_any(item: dict, others: list[dict]) -> bool:
    """Return True if this item's content-without-date is a non-empty substring of any other item's content."""
    content = _normalize_text(_summary_content_without_date(item.get("text", "")))
    if not content:
        return False
    for other in others:
        if other is item:
            continue
        other_content = _normalize_text(_summary_content_without_date(other.get("text", "")))
        if other_content and content != other_content and content in other_content:
            return True
    return False


def _normalize_compressed_summary(payload: dict, timezone_name: str) -> dict:
    important_info = _merge_repeated_priority_lines(list(payload.get("important_info") or []), timezone_name)
    # Drop items within important_info whose content is fully subsumed by another item in the same section
    important_info = [item for item in important_info if not _item_content_subsumed_by_any(item, important_info)]
    other_dates = _dedupe_lines(list(payload.get("other_dates") or []))
    other_topics = _dedupe_lines(list(payload.get("other_topics") or []))
    other_dates = _filter_lines_covered_by_higher_priority_sections(other_dates, important_info)
    other_topics = _filter_lines_covered_by_higher_priority_sections(other_topics, important_info)
    return {
        "title": str(payload.get("title") or "").strip(),
        "important_info": important_info,
        "other_dates": other_dates,
        "other_topics": other_topics,
        "missing_requested_topics": _dedupe_strings(list(payload.get("missing_requested_topics") or [])),
        "notes": _dedupe_strings(list(payload.get("notes") or [])),
    }


def _upgrade_rendered_summary_with_candidates(payload: dict, candidates: list[dict], timezone_name: str) -> dict:
    candidate_lines = _candidate_summary_lines_for_upgrade(candidates, timezone_name)
    if not candidate_lines:
        return payload
    return {
        "title": str(payload.get("title") or "").strip(),
        "important_info": _upgrade_rendered_lines_with_candidates(list(payload.get("important_info") or []), candidate_lines),
        "other_dates": _upgrade_rendered_lines_with_candidates(list(payload.get("other_dates") or []), candidate_lines),
        "other_topics": _upgrade_rendered_lines_with_candidates(list(payload.get("other_topics") or []), candidate_lines),
        "missing_requested_topics": _dedupe_strings(list(payload.get("missing_requested_topics") or [])),
        "notes": _dedupe_strings(list(payload.get("notes") or [])),
    }


def _candidate_summary_lines_for_upgrade(candidates: list[dict], timezone_name: str) -> list[dict]:
    lines: list[dict] = []
    for candidate in candidates:
        text = str(candidate.get("text") or "").strip()
        if not text or not candidate.get("has_date"):
            continue
        normalized = {
            "text": text,
            "source_refs": [str(ref) for ref in list(candidate.get("source_refs") or []) if str(ref).strip()],
            "applies_to": [str(value) for value in list(candidate.get("applies_to") or []) if str(value).strip()],
            "date_sort_key": str(candidate.get("date_sort_key")) if candidate.get("date_sort_key") else None,
        }
        if _should_drop_rendered_summary_line(normalized):
            continue
        lines.append(normalized)
    return _merge_repeated_priority_lines(lines, timezone_name)


def _upgrade_rendered_lines_with_candidates(lines: list[dict], candidate_lines: list[dict]) -> list[dict]:
    upgraded: list[dict] = []
    for line in lines:
        current = {
            "text": str(line.get("text") or "").strip(),
            "source_refs": [str(ref) for ref in list(line.get("source_refs") or []) if str(ref).strip()],
            "applies_to": [str(value) for value in list(line.get("applies_to") or []) if str(value).strip()],
            "date_sort_key": str(line.get("date_sort_key")) if line.get("date_sort_key") else None,
        }
        if _should_drop_rendered_summary_line(current):
            continue
        for candidate in candidate_lines:
            if not _lines_semantically_overlap(current, candidate):
                continue
            if _summary_line_quality(candidate) <= _summary_line_quality(current):
                continue
            current = _merge_overlapping_lines(current, candidate)
        upgraded.append(current)
    return upgraded


def _filter_lines_covered_by_higher_priority_sections(lines: list[dict], covered_lines: list[dict]) -> list[dict]:
    filtered: list[dict] = []
    for line in lines:
        if any(_lines_semantically_overlap(line, covered) for covered in covered_lines):
            continue
        filtered.append(line)
    return filtered


def _dated_candidate_is_rendered(candidate: dict, rendered_lines: list[dict]) -> bool:
    candidate_text = _normalize_text(candidate.get("text", ""))
    candidate_refs = {str(ref) for ref in list(candidate.get("source_refs") or []) if str(ref).strip()}
    candidate_date = str(candidate.get("date_sort_key") or "")
    for rendered in rendered_lines:
        rendered_text = _normalize_text(rendered.get("text", ""))
        if candidate_text and rendered_text == candidate_text:
            return True
        rendered_refs = {str(ref) for ref in list(rendered.get("source_refs") or []) if str(ref).strip()}
        rendered_date = str(rendered.get("date_sort_key") or "")
        if candidate_refs and candidate_refs & rendered_refs and (candidate_date == rendered_date or (candidate_text and candidate_text in rendered_text)):
            return True
        if candidate_date and candidate_date == rendered_date:
            if candidate_text and candidate_text in rendered_text:
                return True
    return False


def _section_line_window(line: str, zone: ZoneInfo, reference_dt: Optional[datetime] = None) -> tuple[Optional[datetime], Optional[datetime]]:
    start_local = _absolute_date_from_text(line, zone, reference_dt)
    if start_local is None:
        return None, None
    range_match = re.search(
        r"\b(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        r"\.?\s+(?P<start>\d{1,2})(?:st|nd|rd|th)?\s*(?:-|to)\s*(?P<end>\d{1,2})(?:st|nd|rd|th)?\b",
        line,
        re.I,
    )
    if range_match:
        try:
            return start_local, start_local.replace(day=int(range_match.group("end")))
        except ValueError:
            return start_local, None

    start_time, end_time = _event_times_from_text(line)
    if start_time is not None:
        start_local = start_local.replace(hour=start_time.hour, minute=start_time.minute)
    end_local = None
    if end_time is not None:
        end_local = start_local.replace(hour=end_time.hour, minute=end_time.minute)
        if end_local <= start_local:
            end_local += timedelta(days=1)
    return start_local, end_local


def _prune_redundant_dated_candidates(candidates: list[dict]) -> list[dict]:
    pruned: list[dict] = []
    for candidate in candidates:
        if _has_stronger_related_candidate(candidate, candidates):
            continue
        pruned.append(candidate)
    return pruned


def _has_stronger_related_candidate(candidate: dict, candidates: list[dict]) -> bool:
    candidate_refs = {str(ref) for ref in list(candidate.get("source_refs") or []) if str(ref).strip()}
    candidate_tokens = _candidate_core_tokens(candidate)
    candidate_quality = _candidate_quality_score(candidate)
    for other in candidates:
        if other is candidate:
            continue
        other_refs = {str(ref) for ref in list(other.get("source_refs") or []) if str(ref).strip()}
        if candidate_refs and other_refs and not (candidate_refs & other_refs):
            continue
        if not candidate.get("has_date") and other.get("has_date"):
            other_tokens = _candidate_core_tokens(other)
            if candidate_tokens and other_tokens and candidate_tokens <= other_tokens:
                return True
            continue
        if not candidate.get("has_date") or not other.get("has_date"):
            continue
        if str(other.get("date_sort_key") or "") != str(candidate.get("date_sort_key") or ""):
            continue
        if (
            str(candidate.get("reason") or "") == "dated_section_match"
            and str(other.get("reason") or "") != "dated_section_match"
            and (candidate_refs & other_refs)
        ):
            return True
        other_tokens = _candidate_core_tokens(other)
        if not candidate_tokens or not other_tokens:
            continue
        if not (candidate_tokens <= other_tokens or other_tokens <= candidate_tokens):
            continue
        if _candidate_quality_score(other) > candidate_quality:
            return True
    return False


def _candidate_core_tokens(candidate: dict) -> set[str]:
    return {
        token
        for token in _normalize_text(candidate.get("text", "")).split()
        if token
        not in {
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "sept",
            "oct",
            "nov",
            "dec",
            "am",
            "pm",
        }
        and not token.isdigit()
    }


def _candidate_quality_score(candidate: dict) -> int:
    normalized = _normalize_text(candidate.get("text", ""))
    score = len(normalized.split())
    if str(candidate.get("reason") or "") != "dated_section_match":
        score += 4
    if "website" in normalized or "learn more" in normalized:
        score -= 4
    if normalized.startswith("from "):
        score -= 5
    if normalized in {"registration deadline", "session begins"}:
        score -= 3
    return score


def _merge_summary_candidates(existing: list[dict], new_items: list[dict]) -> list[dict]:
    merged: dict[tuple[str, str], dict] = {}
    for item in list(existing) + list(new_items):
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        key = (_normalize_text(text), str(item.get("date_sort_key") or ""))
        prior = merged.get(key)
        if prior is None:
            merged[key] = {
                "text": text,
                "consolidated_priority": str(item.get("consolidated_priority") or "mentioned"),
                "matched_system_defaults": [str(ref) for ref in list(item.get("matched_system_defaults") or []) if str(ref).strip()],
                "matched_user_priorities": [str(ref) for ref in list(item.get("matched_user_priorities") or []) if str(ref).strip()],
                "source_refs": [str(ref) for ref in list(item.get("source_refs") or []) if str(ref).strip()],
                "applies_to": [str(value) for value in list(item.get("applies_to") or []) if str(value).strip()],
                "date_sort_key": str(item.get("date_sort_key")) if item.get("date_sort_key") else None,
                "has_date": bool(item.get("has_date")),
                "reason": str(item.get("reason") or ""),
            }
            continue
        prior["consolidated_priority"] = _higher_priority(
            prior["consolidated_priority"], str(item.get("consolidated_priority") or "mentioned")
        )
        prior["matched_system_defaults"] = _dedupe_strings(
            prior["matched_system_defaults"] + list(item.get("matched_system_defaults") or [])
        )
        prior["matched_user_priorities"] = _dedupe_strings(
            prior["matched_user_priorities"] + list(item.get("matched_user_priorities") or [])
        )
        prior["source_refs"] = _dedupe_strings(prior["source_refs"] + list(item.get("source_refs") or []))
        prior["applies_to"] = _dedupe_strings(prior["applies_to"] + list(item.get("applies_to") or []))
        prior["has_date"] = prior["has_date"] or bool(item.get("has_date"))
    values = list(merged.values())
    values.sort(key=lambda item: (_priority_sort_key(item["consolidated_priority"]), item.get("date_sort_key") or "9999", item["text"]))
    return values


def _merge_repeated_priority_lines(candidates: list[dict], timezone_name: str) -> list[dict]:
    pizza_events = [item for item in candidates if "pizza lunch" in _normalize_text(item.get("text", ""))]
    other_events = [item for item in candidates if item not in pizza_events]
    merged: list[dict] = []
    if len(pizza_events) > 1:
        pizza_dates = []
        for item in pizza_events:
            value = _coerce_datetime(item.get("date_sort_key"))
            if value is not None:
                pizza_dates.append(_to_user_timezone(value, timezone_name))
        if pizza_dates:
            pizza_dates.sort()
            month_label = pizza_dates[0].strftime("%b")
            days = " & ".join(str(dt.day) for dt in pizza_dates)
            merged.append(
                {
                    "text": f"{month_label} {days}: Pizza Lunches",
                    "source_refs": _dedupe_strings([ref for item in pizza_events for ref in item.get("source_refs", [])]),
                    "applies_to": _dedupe_strings([value for item in pizza_events for value in item.get("applies_to", [])]),
                    "date_sort_key": pizza_dates[0].astimezone(timezone.utc).isoformat(),
                }
            )
    elif pizza_events:
        merged.extend(_dedupe_lines(pizza_events))
    merged.extend(_dedupe_lines(other_events))
    merged.sort(key=lambda item: (item.get("date_sort_key") or "9999", item["text"]))
    return merged


def _dedupe_lines(items: list[dict]) -> list[dict]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for item in items:
        normalized_item = {
            "text": str(item.get("text") or "").strip(),
            "source_refs": [str(ref) for ref in list(item.get("source_refs") or []) if str(ref).strip()],
            "applies_to": [str(value) for value in list(item.get("applies_to") or []) if str(value).strip()],
            "date_sort_key": str(item.get("date_sort_key")) if item.get("date_sort_key") else None,
        }
        if _should_drop_rendered_summary_line(normalized_item):
            continue
        key = (_normalize_text(normalized_item.get("text", "")), str(normalized_item.get("date_sort_key") or ""))
        if key in seen:
            continue
        overlap_index = _find_overlapping_line_index(normalized_item, deduped)
        if overlap_index is not None:
            deduped[overlap_index] = _merge_overlapping_lines(deduped[overlap_index], normalized_item)
            continue
        seen.add(key)
        deduped.append(normalized_item)
    deduped.sort(key=lambda item: (item.get("date_sort_key") or "9999", item["text"]))
    return deduped


def _find_overlapping_line_index(item: dict, existing: list[dict]) -> Optional[int]:
    for index, prior in enumerate(existing):
        if _lines_semantically_overlap(prior, item):
            return index
    return None


def _merge_overlapping_lines(left: dict, right: dict) -> dict:
    preferred = left if _summary_line_quality(left) >= _summary_line_quality(right) else right
    other = right if preferred is left else left
    return {
        "text": preferred["text"],
        "source_refs": _dedupe_strings(list(preferred.get("source_refs") or []) + list(other.get("source_refs") or [])),
        "applies_to": _dedupe_strings(list(preferred.get("applies_to") or []) + list(other.get("applies_to") or [])),
        "date_sort_key": preferred.get("date_sort_key") or other.get("date_sort_key"),
    }


def _lines_semantically_overlap(left: dict, right: dict) -> bool:
    left_day = _date_sort_day_key(left.get("date_sort_key"))
    right_day = _date_sort_day_key(right.get("date_sort_key"))
    if not left_day or left_day != right_day:
        return False
    left_content = _normalize_text(_summary_content_without_date(left.get("text", "")))
    right_content = _normalize_text(_summary_content_without_date(right.get("text", "")))
    if left_content and right_content:
        left_tokens = left_content.split()
        right_tokens = right_content.split()
        if min(len(left_tokens), len(right_tokens)) >= 1 and (
            left_content == right_content or left_content in right_content or right_content in left_content
        ):
            return True
    left_tokens = _summary_overlap_tokens(left.get("text", ""))
    right_tokens = _summary_overlap_tokens(right.get("text", ""))
    if len(left_tokens) < 3 or len(right_tokens) < 3:
        return False
    shared = left_tokens & right_tokens
    if len(shared) < 3:
        return False
    if left_tokens <= right_tokens or right_tokens <= left_tokens:
        return True
    overlap_ratio = len(shared) / min(len(left_tokens), len(right_tokens))
    return overlap_ratio >= 0.75


def _summary_overlap_tokens(text: str) -> set[str]:
    normalized = re.sub(r"^[A-Za-z]{3,9}\s+\d{1,2}(?:-\d{1,2})?:\s*", "", text or "", flags=re.I)
    normalized = re.sub(r"\([^)]*\)", " ", normalized)
    tokens = _normalize_text(normalized).split()
    return {
        token
        for token in tokens
        if token
        and len(token) > 2
        and token not in SUMMARY_OVERLAP_STOPWORDS
        and token not in MONTH_NAME_MAP
        and not token.isdigit()
    }


def _summary_line_quality(item: dict) -> int:
    text = str(item.get("text") or "").strip()
    score = len(text.split())
    if ";" in text:
        score += 2
    if "(" in text and ")" in text:
        score += 1
    score += len(list(item.get("source_refs") or []))
    if any(str(ref).startswith("event:") for ref in list(item.get("source_refs") or [])):
        score += 4
    normalized = _normalize_text(text)
    if re.search(r"\b\d{1,2}\s+\d{2}\b", text):
        score -= 6
    if normalized.endswith(" is on"):
        score -= 6
    if normalized.startswith("over the next "):
        score -= 6
    if "students in" in normalized and len(normalized.split()) > 8:
        score -= 5
    return score


def _should_drop_rendered_summary_line(item: dict) -> bool:
    text = str(item.get("text") or "").strip()
    if not text:
        return True
    content = _summary_content_without_date(text)
    if _looks_like_low_quality_dated_title(content):
        return True
    normalized_content = _normalize_text(content)
    if not normalized_content:
        return True
    if normalized_content.startswith("over the next "):
        return True
    if normalized_content.endswith(" students in"):
        return True
    # Drop forwarded email header artifacts
    if "original email subject" in normalized_content:
        return True
    if normalized_content.startswith("forwarded message"):
        return True
    if "email subject" in normalized_content and len(normalized_content.split()) < 8:
        return True
    # Drop ASD/welcome contextual narrative that sneak through as dated items
    if normalized_content.startswith("welcome for the new"):
        return True
    if normalized_content.startswith("welcoming our new"):
        return True
    if normalized_content.startswith("welcome to the new"):
        return True
    if normalized_content.startswith("welcome our new"):
        return True
    if "welcoming" in normalized_content and "new" in normalized_content and "class" in normalized_content:
        return True
    if "welcome" in normalized_content and "new" in normalized_content and "class" in normalized_content:
        return True
    # Drop ASD class-start announcements when they leak through as dated items
    # (they're contextual school announcements, not calendar events; retained as undated in Other Logistics)
    if "asd" in normalized_content and "class" in normalized_content:
        return True
    # Drop grade-only fragments that have no event context (e.g. "Grades 4 5 6 at Harbord CI")
    if re.match(r"^grades?\s+\d", normalized_content, re.I) and len(normalized_content.split()) < 8:
        return True
    return False


def _summary_content_without_date(text: str) -> str:
    return re.sub(r"^[A-Za-z]{3,9}\s+\d{1,2}(?:-\d{1,2})?:\s*", "", text or "", flags=re.I).strip()


def _date_sort_day_key(value: Optional[str]) -> str:
    dt = _coerce_datetime(value)
    if dt is None:
        return ""
    return dt.date().isoformat()


def _detect_missing_requested_topics(requested_topics: list[str], analysis_text: str, candidates: list[dict]) -> list[str]:
    normalized_text = _normalize_text(analysis_text)
    candidate_text = " ".join(_normalize_text(item.get("text", "")) for item in candidates)
    missing = []
    for topic in requested_topics:
        normalized_topic = _normalize_text(topic)
        if not normalized_topic:
            continue
        if normalized_topic in normalized_text or normalized_topic in candidate_text:
            continue
        missing.append(topic)
    return _dedupe_strings(missing)


def _filter_suppressed_candidates(candidates: list[dict], suppressed_priority_topics: list[str]) -> list[dict]:
    if not suppressed_priority_topics:
        return candidates
    filtered: list[dict] = []
    for candidate in candidates:
        if list(candidate.get("matched_system_defaults") or []):
            filtered.append(candidate)
            continue
        if candidate.get("preference_matching_mode") == "llm":
            if list(candidate.get("matched_suppressed_priorities") or []):
                continue
            filtered.append(candidate)
            continue
        if list(candidate.get("matched_suppressed_priorities") or []):
            continue
        if any(
            topic_matches_text(topic, candidate.get("text", ""), candidate.get("reason", ""))
            for topic in suppressed_priority_topics
        ):
            continue
        filtered.append(candidate)
    return filtered


_GRADE_NUMBER_RE = re.compile(r"\bgrades?\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?", re.I)


def _extract_mentioned_grade_nums(text: str) -> set[int]:
    """Extract all explicitly mentioned grade numbers from text, expanding ranges like 'Grades 4-6'."""
    nums: set[int] = set()
    for m in _GRADE_NUMBER_RE.finditer(text):
        start = int(m.group(1))
        nums.add(start)
        if m.group(2):
            end = int(m.group(2))
            # Expand range (e.g. Grades 4-6 → {4, 5, 6})
            nums.update(range(min(start, end), max(start, end) + 1))
    return nums


def _filter_mismatched_grade_candidates(candidates: list[dict], household_grades: list[str]) -> list[dict]:
    """Drop candidates that explicitly name a grade not in the household's enrolled grades."""
    if not household_grades:
        return candidates
    normalized_grades = {g.strip().lower() for g in household_grades if g.strip()}
    # Build set of numeric grade integers the household has
    household_grade_nums: set[int] = set()
    for g in normalized_grades:
        try:
            household_grade_nums.add(int(g))
        except ValueError:
            pass
    if not household_grade_nums:
        return candidates
    filtered: list[dict] = []
    for candidate in candidates:
        text = str(candidate.get("text") or "")
        reason = str(candidate.get("reason") or "")
        combined = f"{text} {reason}"
        mentioned_grades = _extract_mentioned_grade_nums(combined)
        if not mentioned_grades:
            filtered.append(candidate)
            continue
        if mentioned_grades.isdisjoint(household_grade_nums):
            continue  # all mentioned grades are non-matching — drop
        filtered.append(candidate)
    return filtered


def _build_child_context(children: list) -> dict:
    child_names = [_normalize_text(getattr(child, "name", "")) for child in children if getattr(child, "name", "")]
    schools = [_normalize_text(getattr(child, "school_name", "")) for child in children if getattr(child, "school_name", "")]
    grades_raw = [str(getattr(child, "grade", "")).strip() for child in children if str(getattr(child, "grade", "")).strip()]
    grade_aliases: list[str] = []
    for grade in grades_raw:
        normalized_grade = _normalize_text(grade)
        grade_aliases.extend(GRADE_ALIAS_MAP.get(normalized_grade, [f"grade {normalized_grade}", f"gr {normalized_grade}"]))
    return {
        "children": [getattr(child, "name", "").strip() for child in children if getattr(child, "name", "").strip()],
        "child_names": child_names,
        "schools": schools,
        "grades": grades_raw,
        "grade_aliases": _dedupe_strings(grade_aliases),
    }


def _applies_to_for_event(event: ExtractedEvent, child_context: dict) -> list[str]:
    applies_to: list[str] = []
    for name in list(event.mentioned_names or []):
        if _normalize_text(name) in child_context["child_names"]:
            applies_to.append(name.strip())
    for grade in list(event.target_grades or []):
        if str(grade).strip() in child_context["grades"]:
            applies_to.append(f"Gr {str(grade).strip()}")
    if not applies_to and event.target_scope == "grade_specific":
        for grade in list(event.target_grades or [])[:2]:
            applies_to.append(f"Gr {str(grade).strip()}")
    return _dedupe_strings(applies_to)


def _higher_priority(left: str, right: str) -> str:
    return left if _priority_sort_key(left) <= _priority_sort_key(right) else right


def _priority_sort_key(value: str) -> int:
    mapping = {"important": 0, "mentioned": 1, "ignore": 2}
    return mapping.get(value, 3)


def _summary_title_hint(subject: str, school_names: list[str]) -> str:
    display_subject = (subject or "").replace("Fwd:", "").strip()
    match = re.search(
        (
            r"(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|"
            r"september|sept|sep|october|oct|november|nov|december|dec)\.?\s+\d{1,2}(?:[\/-]\d{2,4})?"
        ),
        display_subject,
        re.I,
    )
    date_part = ""
    if match:
        date_part = re.sub(r"[\/-]\d{2,4}$", "", match.group(0)).replace(".", "").strip().title()
    school = school_names[0].title() if school_names else ""
    school = school.replace(" Community School Junior", " CS").replace(" Junior", "")
    if school and date_part:
        return f"{school} Update ({date_part})"
    if school:
        return f"{school} Update"
    return display_subject or "School Update"


def _coerce_datetime(value: Optional[object]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _to_user_timezone(value: datetime, timezone_name: str) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    try:
        return value.astimezone(ZoneInfo(timezone_name))
    except Exception:
        return value.astimezone(timezone.utc)


def _time_range_label(start_local: datetime, end_local: Optional[datetime]) -> str:
    if end_local is None:
        return start_local.strftime("%I:%M %p").lstrip("0")
    if start_local.date() == end_local.date():
        return f"{start_local.strftime('%I:%M %p').lstrip('0')} to {end_local.strftime('%I:%M %p').lstrip('0')}"
    return f"{start_local.strftime('%I:%M %p').lstrip('0')} to {end_local.strftime('%b')} {end_local.day}"


def _is_single_day_all_day_window(start_local: datetime, end_local: Optional[datetime]) -> bool:
    if end_local is None:
        return False
    return (
        start_local.hour == 0
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
    )


def _serialize_dt(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _normalize_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", (value or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = str(value).strip()
        key = cleaned.lower()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        ordered.append(cleaned)
    return ordered


def _rescue_high_signal_sections(sections: list[AnalysisSection]) -> list[dict]:
    rescued_sections: dict[int, AnalysisSection] = {}
    for section in sections:
        lowered = _normalize_text(f"{section.label} {section.text}")
        if any(term in lowered for term in GENERIC_SECTION_TERMS):
            continue
        if "unsubscribe" in lowered or "schoolmessenger is a notification service" in lowered:
            continue
        if not _section_looks_like_rescuable_event(section, lowered):
            continue
        rescued_sections[section.index] = section
        for neighbor in sections:
            if abs(neighbor.index - section.index) != 1:
                continue
            neighbor_lowered = _normalize_text(f"{neighbor.label} {neighbor.text}")
            if any(term in neighbor_lowered for term in GENERIC_SECTION_TERMS):
                continue
            if any(term in neighbor_lowered for term in RESCUE_EVENT_TERMS) or DATE_PATTERN.search(neighbor.text) or TIME_PATTERN.search(neighbor.text):
                rescued_sections[neighbor.index] = neighbor
    rescued = [
        {
            "section_index": section.index,
            "label": section.label,
            "section_kind": section.section_kind,
            "priority_score": section.priority_score,
            "source_kind": section.source_kind,
            "char_count": len(section.text),
            "text": section.text[:1400],
        }
        for section in rescued_sections.values()
    ]
    rescued.sort(key=lambda item: (-int(item["priority_score"]), int(item["section_index"])))
    return rescued[:3]


def _section_looks_like_rescuable_event(section: AnalysisSection, lowered: str) -> bool:
    has_date = bool(DATE_PATTERN.search(section.text))
    has_time = bool(TIME_PATTERN.search(section.text))
    has_relative_day = "tonight" in lowered or "tomorrow" in lowered
    has_event_term = any(term in lowered for term in RESCUE_EVENT_TERMS)
    if not (has_date or has_time or has_relative_day):
        return False
    if section.priority_score >= 50 and has_event_term:
        return True
    return has_date and has_event_term


def _rescue_candidates_from_sections(rescued_sections: list[dict], title_hint: str, timezone_name: str) -> list[dict]:
    if not rescued_sections:
        return []
    combined = "\n".join(section.get("text", "") for section in rescued_sections if section.get("text"))
    event_title = _rescue_event_title(combined, title_hint)
    if not event_title:
        return []

    start_local, end_local = _rescue_event_window(combined, timezone_name)
    source_refs = [f"section:{section['section_index']}" for section in rescued_sections if section.get("section_index")]
    if start_local is not None:
        start_utc = start_local.astimezone(timezone.utc)
        end_utc = end_local.astimezone(timezone.utc) if end_local else None
        line = _summary_text_for_event(
            {
                "title": event_title,
                "start_at": _serialize_dt(start_utc),
                "end_at": _serialize_dt(end_utc),
                "applies_to": [],
            },
            timezone_name,
        )
        return [
            {
                "text": line,
                "consolidated_priority": "important",
                "matched_system_defaults": [],
                "matched_user_priorities": [],
                "source_refs": source_refs,
                "applies_to": [],
                "date_sort_key": _serialize_dt(start_utc),
                "has_date": True,
                "reason": "rescued_single_event_section",
            }
        ]

    return [
        {
            "text": event_title,
            "consolidated_priority": "important",
            "matched_system_defaults": [],
            "matched_user_priorities": [],
            "source_refs": source_refs,
            "applies_to": [],
            "date_sort_key": None,
            "has_date": False,
            "reason": "rescued_single_event_section",
        }
    ]


def _rescue_event_title(text: str, title_hint: str) -> str:
    for pattern in (
        r"join us for\s+([A-Z][A-Z\s&'-]{4,}|[A-Za-z][A-Za-z0-9\s&'/-]{4,40})",
        r"save the date for\s+([A-Z][A-Za-z0-9\s&'/-]{4,40})",
        r"upcoming\s+([A-Z][A-Za-z0-9\s&'/-]{4,40})",
    ):
        match = re.search(pattern, text, re.I)
        if match:
            return _humanize_event_title(match.group(1))

    for line in text.splitlines():
        cleaned = line.strip(" *\t")
        if not cleaned:
            continue
        if "family math night" in cleaned.lower():
            return "Family Math Night"
        if cleaned.isupper() and len(cleaned.split()) <= 6:
            return _humanize_event_title(cleaned)

    subject = re.sub(r"^Fwd:\s*", "", title_hint, flags=re.I)
    subject = re.sub(r"\b(tonight|tomorrow|today)\b", "", subject, flags=re.I)
    subject = re.sub(r"\bupdate\b", "", subject, flags=re.I)
    subject = re.sub(r"\([^)]*\)", "", subject)
    subject = re.sub(r"\s+", " ", subject).strip(" -:")
    if subject:
        return _humanize_event_title(subject)
    return ""


def _humanize_event_title(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.replace("*", " ")).strip(" -:")
    if cleaned.isupper():
        return cleaned.title()
    words = []
    for word in cleaned.split():
        if word.isupper() and len(word) > 1:
            words.append(word.title())
        else:
            words.append(word)
    return " ".join(words)


def _rescue_event_window(text: str, timezone_name: str) -> tuple[Optional[datetime], Optional[datetime]]:
    zone = _safe_zoneinfo(timezone_name)
    reference_dt = _reference_datetime_from_text(text, zone)
    start_date = _absolute_date_from_text(text, zone)
    if start_date is None and reference_dt is not None:
        lowered = _normalize_text(text)
        if "tomorrow" in lowered:
            start_date = datetime(
                reference_dt.year,
                reference_dt.month,
                reference_dt.day,
                0,
                0,
                tzinfo=zone,
            ) + timedelta(days=1)
        elif "tonight" in lowered or "today" in lowered:
            start_date = datetime(reference_dt.year, reference_dt.month, reference_dt.day, 0, 0, tzinfo=zone)
    if start_date is None:
        return None, None

    start_time, end_time = _event_times_from_text(text)
    if start_time:
        start_local = start_date.replace(hour=start_time.hour, minute=start_time.minute)
    else:
        start_local = start_date
    if end_time:
        end_local = start_date.replace(hour=end_time.hour, minute=end_time.minute)
        if end_local <= start_local:
            end_local += timedelta(days=1)
    else:
        end_local = None
    return start_local, end_local


def _reference_datetime_from_text(text: str, zone: ZoneInfo) -> Optional[datetime]:
    match = re.search(
        r"original email date:\s*(?:\w{3},\s*)?(?P<month>\w+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})",
        text,
        re.I,
    )
    if not match:
        return None
    month = MONTH_NAME_MAP.get(match.group("month").lower().rstrip("."))
    if month is None:
        return None
    return datetime(int(match.group("year")), month, int(match.group("day")), 0, 0, tzinfo=zone)


def _reference_datetime_for_summary(reference_datetime_hint: str, timezone_name: str) -> Optional[datetime]:
    raw = (reference_datetime_hint or "").strip()
    if not raw:
        return None
    zoneinfo = _safe_zoneinfo(timezone_name)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d")
            parsed = parsed.replace(tzinfo=zoneinfo)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zoneinfo)
    return parsed.astimezone(zoneinfo)


def _absolute_date_from_text(text: str, zone: ZoneInfo, reference_dt: Optional[datetime] = None) -> Optional[datetime]:
    match = DATE_PATTERN.search(text)
    if match:
        month = MONTH_NAME_MAP.get(match.group("month").lower().rstrip("."))
        if month is None:
            return None
        base_year = reference_dt.year if reference_dt is not None else datetime.now(zone).year
        year = int(match.group("year") or base_year)
        day = int(match.group("day"))
        try:
            return datetime(year, month, day, 0, 0, tzinfo=zone)
        except ValueError:
            return None

    normalized = _normalize_text(text)
    if reference_dt is None:
        return None
    if "tomorrow" in normalized:
        target = reference_dt + timedelta(days=1)
        return datetime(target.year, target.month, target.day, 0, 0, tzinfo=zone)
    if "today" in normalized or "tonight" in normalized:
        return datetime(reference_dt.year, reference_dt.month, reference_dt.day, 0, 0, tzinfo=zone)

    weekday_match = re.search(
        r"\b(?P<prefix>on|for|this|next)\s+(?P<weekday>monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
        normalized,
    )
    if weekday_match:
        weekday_lookup = {
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6,
        }
        target_weekday = weekday_lookup[weekday_match.group("weekday")]
        days_ahead = (target_weekday - reference_dt.weekday()) % 7
        if weekday_match.group("prefix") == "next":
            days_ahead = days_ahead or 7
        target = reference_dt + timedelta(days=days_ahead)
        return datetime(target.year, target.month, target.day, 0, 0, tzinfo=zone)
    return None


def _event_times_from_text(text: str) -> tuple[Optional[datetime], Optional[datetime]]:
    cleaned_text = re.sub(r"^Original email date:.*$", "", text, flags=re.I | re.M)
    from_to = re.search(
        r"\bfrom\s+(?P<start>\d{1,2}:\d{2}\s*(?:am|pm))\s+to\s+(?P<end>\d{1,2}:\d{2}\s*(?:am|pm))",
        cleaned_text,
        re.I,
    )
    if from_to:
        return _parse_clock(from_to.group("start")), _parse_clock(from_to.group("end"))

    begin_match = re.search(
        r"\bbegin(?:s|ning)?(?:\s+promptly)?(?:.*?)(?:at)\s+(?P<time>\d{1,2}:\d{2}\s*(?:am|pm))",
        cleaned_text,
        re.I | re.S,
    )
    end_match = re.search(
        r"\bend(?:s|ing)?(?:.*?)(?:at|by)\s+(?P<time>\d{1,2}:\d{2}\s*(?:am|pm))",
        cleaned_text,
        re.I | re.S,
    )
    if begin_match:
        return _parse_clock(begin_match.group("time")), _parse_clock(end_match.group("time")) if end_match else None

    times = []
    for match in TIME_PATTERN.finditer(cleaned_text):
        value = match.group(1)
        context = cleaned_text[max(0, match.start() - 32) : match.end() + 32].lower()
        times.append((value, context))
    filtered = [
        value
        for value, context in times
        if "doors open" not in context and "original email date" not in context and "\ndate:" not in context
    ]
    if len(filtered) >= 2:
        return _parse_clock(filtered[0]), _parse_clock(filtered[1])
    if filtered:
        return _parse_clock(filtered[0]), None
    return None, None


def _parse_clock(value: str) -> Optional[datetime]:
    cleaned = re.sub(r"\s+", " ", value.strip()).upper()
    try:
        return datetime.strptime(cleaned, "%I:%M %p")
    except ValueError:
        return None


def _safe_zoneinfo(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except Exception:
        return ZoneInfo("UTC")
