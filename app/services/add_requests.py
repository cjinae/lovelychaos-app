from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from app.services.content_analysis import (
    AnalysisChunk,
    DownloadedAttachment,
    build_analysis_text,
    build_prioritized_chunks,
    extract_candidate_links,
    resolve_and_download_links,
)
from app.services.followups import resolve_followup_candidates
from app.services.llm import ExtractedEvent


@dataclass
class ContextDocumentExtractionResult:
    content_body_text: str
    analysis_text: str
    attachments: list[DownloadedAttachment]
    prioritized_chunks: list[AnalysisChunk]
    reference_datetime_hint: str
    analysis_audit: dict[str, Any]


@dataclass
class CalendarAddCandidateResult:
    candidates: list[ExtractedEvent]
    validation_outcomes: list[dict[str, Any]]
    resolution_source: str
    resolution_audit: dict[str, Any]
    candidate_choices: list[dict[str, Any]]
    clarification_message: Optional[str] = None
    extracted_events: Optional[list[ExtractedEvent]] = None
    chunk_notes: Optional[list[str]] = None
    chunk_failures: Optional[list[dict[str, Any]]] = None
    chunk_summaries: Optional[list[dict[str, Any]]] = None
    extraction_result: Optional[ContextDocumentExtractionResult] = None


@dataclass
class CalendarAddResolutionResult:
    status: str
    message: str
    mutation_executed: bool
    created_event_ids: list[int]
    created_titles: list[str]
    candidate_choices: list[dict[str, Any]]
    resolution_source: str
    audit_payload: dict[str, Any]
    audit_validation: dict[str, Any]
    audit_policy_outcome: dict[str, Any]
    audit_committed_actions: dict[str, Any]


def extract_context_documents(
    *,
    content_body_text: str,
    reference_datetime_hint: str = "",
) -> ContextDocumentExtractionResult:
    candidate_links = extract_candidate_links(content_body_text)
    link_report = resolve_and_download_links(candidate_links)
    analysis_text = build_analysis_text(content_body_text, link_report.attachments)
    if not analysis_text:
        analysis_text = content_body_text
    sections, prioritized_chunks = build_prioritized_chunks(content_body_text, link_report.attachments)
    return ContextDocumentExtractionResult(
        content_body_text=content_body_text,
        analysis_text=analysis_text,
        attachments=list(link_report.attachments),
        prioritized_chunks=list(prioritized_chunks),
        reference_datetime_hint=reference_datetime_hint,
        analysis_audit={
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
        },
    )


def resolve_calendar_add_candidates(
    *,
    raw_body_text: str,
    subject: str,
    timezone_name: str,
    command_topic: Optional[str],
    followup_context: Any = None,
    forwarded_subject: str = "",
    forwarded_date: str = "",
    preference_text: str = "",
    extraction_result: ContextDocumentExtractionResult,
    explicit_candidate: Optional[ExtractedEvent] = None,
    fallback_command_topic_fn: Callable[[str], Optional[str]],
    extract_direct_add_candidate_fn: Callable[[str, str], Optional[ExtractedEvent]],
    candidate_from_followup_item_fn: Callable[[dict[str, Any], str], Optional[ExtractedEvent]],
    resolve_forwarded_add_candidates_fn: Callable[..., tuple[list[ExtractedEvent], dict[str, Any]]],
    collect_extraction_results_fn: Callable[..., tuple[list[ExtractedEvent], list[dict[str, Any]], list[str], list[dict[str, Any]]]],
    validate_candidate_fn: Callable[[ExtractedEvent], dict[str, Any]],
    serialize_dt_fn: Callable[[Any], str],
    build_candidate_clarification_fn: Callable[[list[ExtractedEvent], str], str],
) -> CalendarAddCandidateResult:
    resolved_command_topic = (command_topic or "").strip() or fallback_command_topic_fn(raw_body_text) or None
    if explicit_candidate is not None:
        return CalendarAddCandidateResult(
            candidates=[explicit_candidate],
            validation_outcomes=[
                {
                    "title": explicit_candidate.title,
                    "start_at": serialize_dt_fn(explicit_candidate.start_at),
                    "end_at": serialize_dt_fn(explicit_candidate.end_at),
                    "validation": validate_candidate_fn(explicit_candidate),
                }
            ],
            resolution_source="direct_command",
            resolution_audit={"source": "explicit_tool_candidate"},
            candidate_choices=[_candidate_choice(explicit_candidate, serialize_dt_fn)],
            extraction_result=extraction_result,
        )

    if followup_context is not None:
        followup_matches = resolve_followup_candidates(
            followup_context,
            query_text=resolved_command_topic or raw_body_text,
            topic=resolved_command_topic,
        )
        if len(followup_matches) > 1:
            actionable_followup_candidates = [
                candidate
                for candidate in (
                    candidate_from_followup_item_fn(match.item, timezone_name) for match in followup_matches
                )
                if candidate is not None and candidate.start_at and candidate.end_at
            ]
            return CalendarAddCandidateResult(
                candidates=[],
                validation_outcomes=[],
                resolution_source="followup_context",
                resolution_audit={"source": "followup_context", "match_count": len(followup_matches)},
                candidate_choices=[_candidate_choice(item, serialize_dt_fn) for item in actionable_followup_candidates],
                clarification_message=(
                    build_candidate_clarification_fn(actionable_followup_candidates, timezone_name)
                    if actionable_followup_candidates
                    else "I found more than one matching topic in the last update. Tell me which one you want to add."
                ),
                extraction_result=extraction_result,
            )
        followup_match = followup_matches[0] if followup_matches else None
        if followup_match is not None:
            followup_candidate = candidate_from_followup_item_fn(followup_match.item, timezone_name)
            if followup_candidate and followup_candidate.start_at and followup_candidate.end_at:
                return CalendarAddCandidateResult(
                    candidates=[followup_candidate],
                    validation_outcomes=[
                        {
                            "title": followup_candidate.title,
                            "start_at": serialize_dt_fn(followup_candidate.start_at),
                            "end_at": serialize_dt_fn(followup_candidate.end_at),
                            "validation": validate_candidate_fn(followup_candidate),
                        }
                    ],
                    resolution_source="followup_context",
                    resolution_audit={"source": "followup_context", "matched_item": followup_match.item},
                    candidate_choices=[_candidate_choice(followup_candidate, serialize_dt_fn)],
                    extraction_result=extraction_result,
                )
            if dict(followup_match.item.get("action_capabilities") or {}).get("can_explain"):
                return CalendarAddCandidateResult(
                    candidates=[],
                    validation_outcomes=[],
                    resolution_source="followup_context",
                    resolution_audit={"source": "followup_context", "matched_item": followup_match.item},
                    candidate_choices=[],
                    clarification_message="I matched that topic from the last update, but it doesn't have enough scheduling detail to add to the calendar.",
                    extraction_result=extraction_result,
                )

    direct_candidate = extract_direct_add_candidate_fn(raw_body_text, timezone_name)
    if direct_candidate is not None:
        return CalendarAddCandidateResult(
            candidates=[direct_candidate],
            validation_outcomes=[
                {
                    "title": direct_candidate.title,
                    "start_at": serialize_dt_fn(direct_candidate.start_at),
                    "end_at": serialize_dt_fn(direct_candidate.end_at),
                    "validation": validate_candidate_fn(direct_candidate),
                }
            ],
            resolution_source="direct_command",
            resolution_audit={"source": "direct_command", "candidate_title": direct_candidate.title},
            candidate_choices=[_candidate_choice(direct_candidate, serialize_dt_fn)],
            extraction_result=extraction_result,
        )

    extracted_events, chunk_summaries, chunk_notes, chunk_failures = collect_extraction_results_fn(
        extraction_result.prioritized_chunks,
        subject,
        preference_text,
        timezone_name,
        reference_datetime_hint=extraction_result.reference_datetime_hint,
    )
    resolved_candidates, resolution_audit = resolve_forwarded_add_candidates_fn(
        extracted_events=extracted_events,
        command_topic=resolved_command_topic,
        content_body_text=extraction_result.content_body_text,
        forwarded_subject=forwarded_subject or subject,
        forwarded_date=forwarded_date,
        timezone_name=timezone_name,
    )
    final_candidates = resolved_candidates or extracted_events
    final_validations = _validation_outcomes(final_candidates, validate_candidate_fn, serialize_dt_fn)
    return CalendarAddCandidateResult(
        candidates=list(final_candidates),
        validation_outcomes=final_validations,
        resolution_source="forwarded_date_resolver" if resolved_candidates else "content_extraction",
        resolution_audit=resolution_audit if resolved_candidates else {"source": "content_extraction"},
        candidate_choices=[_candidate_choice(candidate, serialize_dt_fn) for candidate in final_candidates],
        extracted_events=list(extracted_events),
        chunk_notes=list(chunk_notes),
        chunk_failures=list(chunk_failures),
        chunk_summaries=list(chunk_summaries),
        extraction_result=extraction_result,
    )


def resolve_add_request_from_context(
    *,
    raw_body_text: str,
    subject: str,
    timezone_name: str,
    response_channel: str,
    command_topic: Optional[str],
    followup_context: Any = None,
    forwarded_subject: str = "",
    forwarded_date: str = "",
    preference_text: str = "",
    explicit_candidate: Optional[ExtractedEvent] = None,
    extraction_result: ContextDocumentExtractionResult,
    fallback_command_topic_fn: Callable[[str], Optional[str]],
    extract_direct_add_candidate_fn: Callable[[str, str], Optional[ExtractedEvent]],
    candidate_from_followup_item_fn: Callable[[dict[str, Any], str], Optional[ExtractedEvent]],
    resolve_forwarded_add_candidates_fn: Callable[..., tuple[list[ExtractedEvent], dict[str, Any]]],
    collect_extraction_results_fn: Callable[..., tuple[list[ExtractedEvent], list[dict[str, Any]], list[str], list[dict[str, Any]]]],
    validate_candidate_fn: Callable[[ExtractedEvent], dict[str, Any]],
    serialize_dt_fn: Callable[[Any], str],
    build_candidate_clarification_fn: Callable[[list[ExtractedEvent], str], str],
    build_past_event_message_fn: Callable[[list[ExtractedEvent], str], str],
    past_only_candidates_fn: Callable[[list[ExtractedEvent]], list[ExtractedEvent]],
    allows_multiple_add_fn: Callable[[str], bool],
    create_candidate_event_fn: Callable[[ExtractedEvent], dict[str, Any]],
) -> CalendarAddResolutionResult:
    candidate_result = resolve_calendar_add_candidates(
        raw_body_text=raw_body_text,
        subject=subject,
        timezone_name=timezone_name,
        command_topic=command_topic,
        followup_context=followup_context,
        forwarded_subject=forwarded_subject,
        forwarded_date=forwarded_date,
        preference_text=preference_text,
        extraction_result=extraction_result,
        explicit_candidate=explicit_candidate,
        fallback_command_topic_fn=fallback_command_topic_fn,
        extract_direct_add_candidate_fn=extract_direct_add_candidate_fn,
        candidate_from_followup_item_fn=candidate_from_followup_item_fn,
        resolve_forwarded_add_candidates_fn=resolve_forwarded_add_candidates_fn,
        collect_extraction_results_fn=collect_extraction_results_fn,
        validate_candidate_fn=validate_candidate_fn,
        serialize_dt_fn=serialize_dt_fn,
        build_candidate_clarification_fn=build_candidate_clarification_fn,
    )
    resolved_command_topic = (command_topic or "").strip() or fallback_command_topic_fn(raw_body_text) or None
    audit_payload = {
        "analysis": dict(extraction_result.analysis_audit),
        "command_resolution": candidate_result.resolution_audit,
        "events": list(candidate_result.validation_outcomes),
    }
    if candidate_result.chunk_notes:
        audit_payload["email_level_notes"] = "\n".join(candidate_result.chunk_notes)
    if candidate_result.chunk_summaries:
        audit_payload["analysis"]["chunk_summaries"] = list(candidate_result.chunk_summaries)
    if candidate_result.chunk_failures:
        audit_payload["analysis"]["chunk_failures"] = list(candidate_result.chunk_failures)
    if candidate_result.extracted_events and candidate_result.resolution_source == "forwarded_date_resolver":
        audit_payload["resolved_candidates"] = list(candidate_result.validation_outcomes)
    if candidate_result.chunk_failures and not (candidate_result.extracted_events or []):
        return CalendarAddResolutionResult(
            status="command_needs_clarification",
            message="I couldn't find a clear event to add from that message. Please forward the event details again or be more specific.",
            mutation_executed=False,
            created_event_ids=[],
            created_titles=[],
            candidate_choices=[],
            resolution_source=candidate_result.resolution_source,
            audit_payload=audit_payload,
            audit_validation={"valid": False, "issues": ["llm_extraction_error"]},
            audit_policy_outcome={"status": "command_needs_clarification", "reason": "add_extraction_error"},
            audit_committed_actions={},
        )

    if candidate_result.clarification_message:
        return CalendarAddResolutionResult(
            status="command_needs_clarification",
            message=candidate_result.clarification_message,
            mutation_executed=False,
            created_event_ids=[],
            created_titles=[],
            candidate_choices=list(candidate_result.candidate_choices),
            resolution_source=candidate_result.resolution_source,
            audit_payload=audit_payload,
            audit_validation={"valid": False, "issues": ["multiple_actionable_events"] if candidate_result.candidate_choices else ["no_actionable_event"]},
            audit_policy_outcome={"status": "command_needs_clarification", "reason": "followup_needs_clarification"},
            audit_committed_actions={},
        )

    actionable_candidates = [
        candidate
        for candidate, outcome in zip(candidate_result.candidates, candidate_result.validation_outcomes)
        if outcome["validation"].get("valid")
    ]
    if not actionable_candidates:
        past_candidates = past_only_candidates_fn(candidate_result.candidates)
        if past_candidates:
            return CalendarAddResolutionResult(
                status="command_noop_past_event",
                message=build_past_event_message_fn(past_candidates, timezone_name),
                mutation_executed=False,
                created_event_ids=[],
                created_titles=[],
                candidate_choices=[_candidate_choice(candidate, serialize_dt_fn) for candidate in past_candidates],
                resolution_source=candidate_result.resolution_source,
                audit_payload=audit_payload,
                audit_validation={"valid": False, "issues": ["event_in_past"]},
                audit_policy_outcome={"status": "command_noop_past_event", "reason": "event_in_past"},
                audit_committed_actions={},
            )
        return CalendarAddResolutionResult(
            status="command_needs_clarification",
            message="I couldn't find a clear future event to add from that message. Please reply with the exact event details you want added.",
            mutation_executed=False,
            created_event_ids=[],
            created_titles=[],
            candidate_choices=list(candidate_result.candidate_choices),
            resolution_source=candidate_result.resolution_source,
            audit_payload=audit_payload,
            audit_validation={"valid": False, "issues": ["no_actionable_event"]},
            audit_policy_outcome={"status": "command_needs_clarification", "reason": "no_actionable_event"},
            audit_committed_actions={},
        )

    if len(actionable_candidates) > 1:
        if allows_multiple_add_fn(resolved_command_topic or raw_body_text):
            created_ids: list[int] = []
            created_titles: list[str] = []
            for candidate in actionable_candidates:
                created = create_candidate_event_fn(candidate)
                if created.get("status") != "command_completed":
                    return CalendarAddResolutionResult(
                        status=str(created.get("status") or "command_needs_clarification"),
                        message=str(created.get("message") or "I couldn't add that event right now."),
                        mutation_executed=bool(created.get("mutation_executed")),
                        created_event_ids=created_ids,
                        created_titles=created_titles,
                        candidate_choices=[_candidate_choice(item, serialize_dt_fn) for item in actionable_candidates],
                        resolution_source="multiple",
                        audit_payload=audit_payload,
                        audit_validation={"valid": False, "issues": ["calendar_write_failed"]},
                        audit_policy_outcome={"status": "command_needs_clarification", "reason": "calendar_write_failed"},
                        audit_committed_actions={"events_created": created_titles},
                    )
                if created.get("event_id") is not None:
                    created_ids.append(int(created["event_id"]))
                if created.get("title"):
                    created_titles.append(str(created["title"]))
            past_candidates = past_only_candidates_fn(candidate_result.candidates)
            message = f"Added {len(created_titles)} events to calendar."
            if past_candidates:
                message += f" Skipped {len(past_candidates)} past event(s)."
            return CalendarAddResolutionResult(
                status="command_completed",
                message=message,
                mutation_executed=bool(created_titles),
                created_event_ids=created_ids,
                created_titles=created_titles,
                candidate_choices=[_candidate_choice(item, serialize_dt_fn) for item in actionable_candidates],
                resolution_source="multiple",
                audit_payload=audit_payload,
                audit_validation={"valid": True},
                audit_policy_outcome={"status": "command_completed", "reason": "multiple_events_added"},
                audit_committed_actions={"events_created": created_titles, "past_events_skipped": [item.title for item in past_candidates]},
            )
        return CalendarAddResolutionResult(
            status="command_needs_clarification",
            message=build_candidate_clarification_fn(actionable_candidates, timezone_name),
            mutation_executed=False,
            created_event_ids=[],
            created_titles=[],
            candidate_choices=[_candidate_choice(item, serialize_dt_fn) for item in actionable_candidates],
            resolution_source="multiple",
            audit_payload=audit_payload,
            audit_validation={"valid": False, "issues": ["multiple_actionable_events"]},
            audit_policy_outcome={"status": "command_needs_clarification", "reason": "multiple_actionable_events"},
            audit_committed_actions={},
        )

    selected = actionable_candidates[0]
    created = create_candidate_event_fn(selected)
    audit_payload["selected_event"] = {
        "title": selected.title,
        "start_at": serialize_dt_fn(selected.start_at),
        "end_at": serialize_dt_fn(selected.end_at),
    }
    return CalendarAddResolutionResult(
        status=str(created.get("status") or "command_needs_clarification"),
        message=str(created.get("message") or "I couldn't add that event right now."),
        mutation_executed=bool(created.get("mutation_executed")),
        created_event_ids=[int(created["event_id"])] if created.get("event_id") is not None else [],
        created_titles=[str(created["title"])] if created.get("title") else [],
        candidate_choices=[_candidate_choice(selected, serialize_dt_fn)],
        resolution_source=candidate_result.resolution_source,
        audit_payload=audit_payload,
        audit_validation={"valid": created.get("status") == "command_completed"},
        audit_policy_outcome={"status": str(created.get("status") or "command_needs_clarification"), "reason": str(created.get("reason") or "event_add_attempt")},
        audit_committed_actions={"event_created": created.get("title")} if created.get("mutation_executed") else {},
    )


def _validation_outcomes(
    candidates: list[ExtractedEvent],
    validate_candidate_fn: Callable[[ExtractedEvent], dict[str, Any]],
    serialize_dt_fn: Callable[[Any], str],
) -> list[dict[str, Any]]:
    return [
        {
            "title": candidate.title,
            "start_at": serialize_dt_fn(candidate.start_at),
            "end_at": serialize_dt_fn(candidate.end_at),
            "validation": validate_candidate_fn(candidate),
        }
        for candidate in candidates
    ]


def _candidate_choice(candidate: ExtractedEvent, serialize_dt_fn: Callable[[Any], str]) -> dict[str, Any]:
    return {
        "title": candidate.title,
        "start_at": serialize_dt_fn(candidate.start_at),
        "end_at": serialize_dt_fn(candidate.end_at),
    }
