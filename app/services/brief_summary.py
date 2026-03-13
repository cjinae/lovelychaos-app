from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import re
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from app.services.content_analysis import AnalysisSection
from app.services.llm import DecisionEngine, ExtractedEvent
from app.services.priorities import school_closure_matches, topic_matches_text


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
)
SUMMARY_IGNORE_TERMS = (
    "daylight saving time",
    "email address change window",
    "volunteer setup shift",
    "volunteer cleanup shift",
    "event-running shift",
    "donation drop-off window",
    "drop-off window",
)


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
    important_dates: list[SummaryLine]
    important_items: list[SummaryLine]
    other_topics: list[SummaryLine]
    missing_requested_topics: list[str]
    notes: list[str]
    rendered_message: str

    def as_dict(self) -> dict:
        return {
            "title": self.title,
            "important_dates": [line.as_dict() for line in self.important_dates],
            "important_items": [line.as_dict() for line in self.important_items],
            "other_topics": [line.as_dict() for line in self.other_topics],
            "missing_requested_topics": list(self.missing_requested_topics),
            "notes": list(self.notes),
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
    children: list,
    extracted_events: list[ExtractedEvent],
    per_event_outcomes: list[dict],
    sections: list[AnalysisSection],
    analysis_text: str,
    chunk_notes: list[str],
) -> tuple[SummaryResult, dict]:
    summary_context = _prefilter_summary_context(
        subject=subject,
        timezone_name=timezone_name,
        household_preferences=household_preferences,
        system_defaults=system_defaults or {"school_closures": True, "grade_relevant": True},
        user_priority_topics=user_priority_topics or [],
        children=children,
        extracted_events=extracted_events,
        per_event_outcomes=per_event_outcomes,
        sections=sections,
        analysis_text=analysis_text,
        chunk_notes=chunk_notes,
    )

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
            }
        )
    except Exception:
        extracted_summary = {
            "title": summary_context["title_hint"],
            "candidates": fallback_candidates,
            "notes": list(summary_context["notes"]),
            "missing_requested_topics": list(summary_context["missing_requested_topics"]),
        }

    merged_candidates = _merge_summary_candidates(fallback_candidates, extracted_summary.get("candidates") or [])
    extracted_notes = [str(note) for note in list(extracted_summary.get("notes") or []) if str(note).strip()]
    extracted_missing = [
        str(topic) for topic in list(extracted_summary.get("missing_requested_topics") or []) if str(topic).strip()
    ]

    try:
        compressed_summary = engine.compress_summary(
            {
                "title_hint": extracted_summary.get("title") or summary_context["title_hint"],
                "candidates": merged_candidates,
                "missing_requested_topics": _dedupe_strings(
                    list(summary_context["missing_requested_topics"]) + extracted_missing
                ),
                "notes": list(summary_context["notes"]) + extracted_notes,
            }
        )
    except Exception:
        compressed_summary = _compress_summary_deterministically(
            extracted_summary.get("title") or summary_context["title_hint"],
            merged_candidates,
            _dedupe_strings(list(summary_context["missing_requested_topics"]) + extracted_missing),
            list(summary_context["notes"]) + extracted_notes,
            timezone_name,
        )

    result = _summary_result_from_dict(compressed_summary, summary_context["title_hint"])
    audit_payload = {
        "input_context": {
            "grades": summary_context["household_context"]["grades"],
            "children": summary_context["household_context"]["children"],
            "schools": summary_context["household_context"]["schools"],
            "preferences": household_preferences,
            "user_priority_topics": summary_context["user_priority_topics"],
            "enabled_system_defaults": summary_context["enabled_system_defaults"],
            "timezone": timezone_name,
        },
        "prefilter": {
            "kept_sections": summary_context["kept_sections"],
            "dropped_sections": summary_context["dropped_sections"],
            "kept_event_titles": [fact["title"] for fact in summary_context["event_facts"]],
        },
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
    children: list,
    extracted_events: list[ExtractedEvent],
    per_event_outcomes: list[dict],
    sections: list[AnalysisSection],
    analysis_text: str,
    chunk_notes: list[str],
) -> dict:
    child_context = _build_child_context(children)
    enabled_system_defaults = sorted([key for key, enabled in system_defaults.items() if enabled])
    normalized_user_topics = _dedupe_strings([item.strip() for item in user_priority_topics if item.strip()])

    event_facts: list[dict] = []
    for idx, event in enumerate(extracted_events):
        outcome = per_event_outcomes[idx] if idx < len(per_event_outcomes) else {}
        execution_disposition = str(outcome.get("execution_disposition") or _legacy_disposition(outcome) or "")

        applies_to = _applies_to_for_event(event, child_context)
        matched_system_defaults = _matched_system_defaults(event, applies_to, child_context, enabled_system_defaults)
        matched_user_priorities = _matched_user_priorities(event, normalized_user_topics)
        consolidated_priority = _determine_consolidated_priority(
            event=event,
            execution_disposition=execution_disposition,
            matched_system_defaults=matched_system_defaults,
            matched_user_priorities=matched_user_priorities,
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
            "text": section.text[:1400],
        }
        if keep and not any(term in lowered for term in GENERIC_SECTION_TERMS):
            kept_sections.append(payload)
        else:
            dropped_sections.append({k: v for k, v in payload.items() if k != "text"})

    kept_sections = kept_sections[:8]
    notes = [note for note in chunk_notes if note and note != "empty_model_events"]
    fallback_candidates = _deterministic_candidates(
        timezone_name=timezone_name,
        event_facts=event_facts,
        analysis_text=analysis_text,
        user_priority_topics=normalized_user_topics,
    )
    missing_requested_topics = _detect_missing_requested_topics(normalized_user_topics, analysis_text, fallback_candidates)
    title_hint = _summary_title_hint(subject, child_context["schools"])
    return {
        "title_hint": title_hint,
        "household_context": {
            "children": child_context["children"],
            "grades": child_context["grades"],
            "schools": child_context["schools"],
            "timezone": timezone_name,
        },
        "enabled_system_defaults": enabled_system_defaults,
        "user_priority_topics": normalized_user_topics,
        "event_facts": event_facts,
        "kept_sections": kept_sections,
        "dropped_sections": dropped_sections,
        "notes": notes,
        "fallback_candidates": fallback_candidates,
        "missing_requested_topics": missing_requested_topics,
    }


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
    if applies_to:
        return True
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
) -> str:
    normalized = _normalize_text(f"{event.title} {event.model_reason}")
    if any(term in normalized for term in SUMMARY_IGNORE_TERMS):
        return "ignore"
    if matched_system_defaults or matched_user_priorities:
        return "important"
    if execution_disposition in {"create_event", "pending_event", "informational_item"} and _is_mention_worthy(event):
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
    analysis_text: str,
    user_priority_topics: list[str],
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

    normalized_text = _normalize_text(analysis_text)
    topic_patterns = [
        ("Safe arrival/absence procedures", ("safe arrival", "absence", "attendance")),
        ("Weather-appropriate clothing", ("weather", "clothing", "cold weather")),
        ("Mandatory forms or payments", ("form", "school cash online", "cash online", "permission")),
        ("Volunteering or event support", ("volunteer", "wristband")),
        ("School council updates", ("school council", "executive results")),
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
    return _merge_summary_candidates([], candidates)


def _compress_summary_deterministically(
    title: str,
    candidates: list[dict],
    missing_requested_topics: list[str],
    notes: list[str],
    timezone_name: str,
) -> dict:
    important_dates = [candidate for candidate in candidates if candidate.get("consolidated_priority") == "important" and candidate.get("has_date")]
    important_items = [candidate for candidate in candidates if candidate.get("consolidated_priority") == "important" and not candidate.get("has_date")]
    other_topics = [candidate for candidate in candidates if candidate.get("consolidated_priority") == "mentioned"]

    return {
        "title": title,
        "important_dates": _merge_repeated_priority_lines(important_dates, timezone_name)[:8],
        "important_items": _dedupe_lines(important_items)[:5],
        "other_topics": _dedupe_lines(other_topics)[:4],
        "missing_requested_topics": _dedupe_strings(missing_requested_topics),
        "notes": _dedupe_strings(notes),
    }


def _summary_result_from_dict(payload: dict, fallback_title: str) -> SummaryResult:
    title = (payload.get("title") or fallback_title or "School Update").strip()
    important_dates = [_line_from_dict(item) for item in list(payload.get("important_dates") or []) if item.get("text")]
    important_items = [_line_from_dict(item) for item in list(payload.get("important_items") or []) if item.get("text")]
    other_topics = [_line_from_dict(item) for item in list(payload.get("other_topics") or []) if item.get("text")]
    missing_requested_topics = _dedupe_strings(list(payload.get("missing_requested_topics") or []))
    notes = _dedupe_strings(list(payload.get("notes") or []))
    rendered = _render_summary(
        title=title,
        important_dates=important_dates,
        important_items=important_items,
        other_topics=other_topics,
        missing_requested_topics=missing_requested_topics,
    )
    return SummaryResult(
        title=title,
        important_dates=important_dates,
        important_items=important_items,
        other_topics=other_topics,
        missing_requested_topics=missing_requested_topics,
        notes=notes,
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
    important_dates: list[SummaryLine],
    important_items: list[SummaryLine],
    other_topics: list[SummaryLine],
    missing_requested_topics: list[str],
) -> str:
    lines = [title]
    if important_dates:
        lines.extend(["", "Important Dates"])
        lines.extend(f"- {item.text}" for item in important_dates)
    if important_items:
        lines.extend(["", "Important Items"])
        lines.extend(f"- {item.text}" for item in important_items)
    if other_topics:
        lines.extend(["", "Other Logistics / Topics Mentioned"])
        lines.extend(f"- {item.text}" for item in other_topics)
    lines.extend(
        [
            "",
            "Let me know if you want me to add any of these to the calendar or want more info on any topic mentioned.",
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
    if end_local and start_local.date() != end_local.date():
        end_label = f"{end_local.strftime('%b')} {end_local.day}"
        return f"{date_label}-{end_label}: {label}"
    if start_local.hour == 0 and start_local.minute == 0:
        return f"{date_label}: {label}"
    time_label = _time_range_label(start_local, end_local)
    if time_label:
        return f"{date_label}: {label} ({time_label})"
    return f"{date_label}: {label}"


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
    if pizza_events:
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
    merged.extend(_dedupe_lines(other_events))
    merged.sort(key=lambda item: (item.get("date_sort_key") or "9999", item["text"]))
    return merged


def _dedupe_lines(items: list[dict]) -> list[dict]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for item in items:
        key = (_normalize_text(item.get("text", "")), str(item.get("date_sort_key") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(
            {
                "text": str(item.get("text") or "").strip(),
                "source_refs": [str(ref) for ref in list(item.get("source_refs") or []) if str(ref).strip()],
                "applies_to": [str(value) for value in list(item.get("applies_to") or []) if str(value).strip()],
                "date_sort_key": str(item.get("date_sort_key")) if item.get("date_sort_key") else None,
            }
        )
    deduped.sort(key=lambda item: (item.get("date_sort_key") or "9999", item["text"]))
    return deduped


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


def _legacy_disposition(outcome: dict) -> str:
    final_batch = str(outcome.get("final_batch") or "")
    if final_batch == "A":
        return "create_event"
    if final_batch == "B":
        return "pending_event"
    if final_batch == "C":
        return "informational_item"
    return ""


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
