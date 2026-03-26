from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.services.add_requests import (
    CalendarAddResolutionResult,
    ContextDocumentExtractionResult,
    extract_context_documents,
    resolve_add_request_from_context,
    resolve_calendar_add_candidates,
)
from app.services.followups import FollowupMatch
from app.services.llm import ExtractedEvent


def _candidate(title: str, *, days: int = 1) -> ExtractedEvent:
    start_at = datetime.now(timezone.utc) + timedelta(days=days)
    end_at = start_at + timedelta(hours=1)
    return ExtractedEvent(
        title=title,
        start_at=start_at,
        end_at=end_at,
        category="command",
        confidence=0.95,
        model_reason="test_candidate",
    )


def _extraction_result() -> ContextDocumentExtractionResult:
    return ContextDocumentExtractionResult(
        content_body_text="Add this",
        analysis_text="Add this",
        attachments=[],
        prioritized_chunks=[],
        reference_datetime_hint="",
        analysis_audit={"links": [], "link_attempts": [], "attachment_count": 0, "analysis_char_count": 8, "section_summaries": []},
    )


def _validate(candidate: ExtractedEvent) -> dict:
    issues = []
    if not candidate.start_at or not candidate.end_at:
        issues.append("missing_schedule")
    elif candidate.end_at <= datetime.now(timezone.utc):
        issues.append("event_in_past")
    return {"valid": not issues, "issues": issues}


def _serialize_dt(value) -> str:
    return value.isoformat() if value else ""


def _clarify(candidates: list[ExtractedEvent], _timezone_name: str) -> str:
    return "Choose: " + ", ".join(candidate.title for candidate in candidates)


def _past_message(candidates: list[ExtractedEvent], _timezone_name: str) -> str:
    return "Past: " + ", ".join(candidate.title for candidate in candidates)


def test_extract_context_documents_returns_analysis_and_audit(monkeypatch):
    @dataclass
    class _Attempt:
        url: str
        status: str

    @dataclass
    class _Section:
        index: int
        label: str
        section_kind: str
        priority_score: int
        source_kind: str
        text: str

    @dataclass
    class _LinkReport:
        attachments: list
        attempts: list

    monkeypatch.setattr("app.services.add_requests.extract_candidate_links", lambda text: ["https://example.com/flyer.pdf"])
    monkeypatch.setattr(
        "app.services.add_requests.resolve_and_download_links",
        lambda links: _LinkReport(attachments=["attachment-1"], attempts=[_Attempt(url=links[0], status="downloaded")]),
    )
    monkeypatch.setattr("app.services.add_requests.build_analysis_text", lambda text, attachments: f"{text}\n{attachments[0]}")
    monkeypatch.setattr(
        "app.services.add_requests.build_prioritized_chunks",
        lambda text, attachments: (
            [_Section(index=0, label="body", section_kind="body", priority_score=10, source_kind="email", text=text)],
            ["chunk-1"],
        ),
    )

    result = extract_context_documents(content_body_text="Family Movie Night", reference_datetime_hint="2026-03-20T12:00:00+00:00")

    assert result.analysis_text == "Family Movie Night\nattachment-1"
    assert result.attachments == ["attachment-1"]
    assert result.prioritized_chunks == ["chunk-1"]
    assert result.analysis_audit["links"] == ["https://example.com/flyer.pdf"]
    assert result.analysis_audit["attachment_count"] == 1


def test_resolve_calendar_add_candidates_prefers_followup_clarification(monkeypatch):
    first = _candidate("Pizza Day")
    second = _candidate("PJ Day")
    monkeypatch.setattr(
        "app.services.add_requests.resolve_followup_candidates",
        lambda context, query_text="", topic=None: [
            FollowupMatch(item={"item_id": "pizza"}, from_summary=False, score=10),
            FollowupMatch(item={"item_id": "pj"}, from_summary=False, score=9),
        ],
    )

    def candidate_from_item(item: dict, _timezone_name: str) -> ExtractedEvent | None:
        if item["item_id"] == "pizza":
            return first
        if item["item_id"] == "pj":
            return second
        return None

    result = resolve_calendar_add_candidates(
        raw_body_text="add this",
        subject="Weekly update",
        timezone_name="America/Toronto",
        command_topic="pizza",
        followup_context=object(),
        extraction_result=_extraction_result(),
        fallback_command_topic_fn=lambda text: None,
        extract_direct_add_candidate_fn=lambda text, tz: None,
        candidate_from_followup_item_fn=candidate_from_item,
        resolve_forwarded_add_candidates_fn=lambda **kwargs: ([], {}),
        collect_extraction_results_fn=lambda *args, **kwargs: ([], [], [], []),
        validate_candidate_fn=_validate,
        serialize_dt_fn=_serialize_dt,
        build_candidate_clarification_fn=_clarify,
    )

    assert result.resolution_source == "followup_context"
    assert result.clarification_message == "Choose: Pizza Day, PJ Day"
    assert len(result.candidate_choices) == 2


def test_resolve_calendar_add_candidates_uses_forwarded_candidate_recovery():
    extracted = _candidate("Generic Flyer Event")
    resolved = _candidate("Family Field Trip")
    result = resolve_calendar_add_candidates(
        raw_body_text="add this",
        subject="Fwd: Family Field Trip",
        timezone_name="America/Toronto",
        command_topic=None,
        forwarded_subject="Family Field Trip",
        forwarded_date="Sun, Mar 8, 2026 at 6:07 PM",
        preference_text="",
        extraction_result=_extraction_result(),
        fallback_command_topic_fn=lambda text: None,
        extract_direct_add_candidate_fn=lambda text, tz: None,
        candidate_from_followup_item_fn=lambda item, tz: None,
        resolve_forwarded_add_candidates_fn=lambda **kwargs: ([resolved], {"source": "forwarded_date_resolver"}),
        collect_extraction_results_fn=lambda *args, **kwargs: ([extracted], [{"title": extracted.title}], [], []),
        validate_candidate_fn=_validate,
        serialize_dt_fn=_serialize_dt,
        build_candidate_clarification_fn=_clarify,
    )

    assert result.resolution_source == "forwarded_date_resolver"
    assert [candidate.title for candidate in result.candidates] == ["Family Field Trip"]
    assert result.resolution_audit["source"] == "forwarded_date_resolver"


def test_resolve_add_request_from_context_returns_past_noop():
    past = _candidate("Past Assembly", days=-2)
    result = resolve_add_request_from_context(
        raw_body_text="add past assembly",
        subject="School update",
        timezone_name="America/Toronto",
        response_channel="email",
        command_topic=None,
        extraction_result=_extraction_result(),
        fallback_command_topic_fn=lambda text: None,
        extract_direct_add_candidate_fn=lambda text, tz: None,
        candidate_from_followup_item_fn=lambda item, tz: None,
        resolve_forwarded_add_candidates_fn=lambda **kwargs: ([], {}),
        collect_extraction_results_fn=lambda *args, **kwargs: ([past], [], [], []),
        validate_candidate_fn=_validate,
        serialize_dt_fn=_serialize_dt,
        build_candidate_clarification_fn=_clarify,
        build_past_event_message_fn=_past_message,
        past_only_candidates_fn=lambda candidates: list(candidates),
        allows_multiple_add_fn=lambda text: False,
        create_candidate_event_fn=lambda candidate: {"status": "command_completed", "mutation_executed": True},
    )

    assert isinstance(result, CalendarAddResolutionResult)
    assert result.status == "command_noop_past_event"
    assert result.message == "Past: Past Assembly"
    assert result.mutation_executed is False


def test_resolve_add_request_from_context_returns_clarification_for_multiple_actionable():
    first = _candidate("Pizza Day")
    second = _candidate("PJ Day")
    result = resolve_add_request_from_context(
        raw_body_text="add this",
        subject="School update",
        timezone_name="America/Toronto",
        response_channel="sms",
        command_topic=None,
        extraction_result=_extraction_result(),
        fallback_command_topic_fn=lambda text: None,
        extract_direct_add_candidate_fn=lambda text, tz: None,
        candidate_from_followup_item_fn=lambda item, tz: None,
        resolve_forwarded_add_candidates_fn=lambda **kwargs: ([], {}),
        collect_extraction_results_fn=lambda *args, **kwargs: ([first, second], [], [], []),
        validate_candidate_fn=_validate,
        serialize_dt_fn=_serialize_dt,
        build_candidate_clarification_fn=_clarify,
        build_past_event_message_fn=_past_message,
        past_only_candidates_fn=lambda candidates: [],
        allows_multiple_add_fn=lambda text: False,
        create_candidate_event_fn=lambda candidate: {"status": "command_completed", "mutation_executed": True},
    )

    assert result.status == "command_needs_clarification"
    assert result.message == "Choose: Pizza Day, PJ Day"
    assert len(result.candidate_choices) == 2


def test_resolve_add_request_from_context_creates_single_event():
    candidate = _candidate("Family Field Trip")
    result = resolve_add_request_from_context(
        raw_body_text="please add family field trip",
        subject="Quick add",
        timezone_name="America/Toronto",
        response_channel="email",
        command_topic="Family Field Trip",
        extraction_result=_extraction_result(),
        fallback_command_topic_fn=lambda text: None,
        extract_direct_add_candidate_fn=lambda text, tz: candidate,
        candidate_from_followup_item_fn=lambda item, tz: None,
        resolve_forwarded_add_candidates_fn=lambda **kwargs: ([], {}),
        collect_extraction_results_fn=lambda *args, **kwargs: ([], [], [], []),
        validate_candidate_fn=_validate,
        serialize_dt_fn=_serialize_dt,
        build_candidate_clarification_fn=_clarify,
        build_past_event_message_fn=_past_message,
        past_only_candidates_fn=lambda candidates: [],
        allows_multiple_add_fn=lambda text: False,
        create_candidate_event_fn=lambda event: {
            "status": "command_completed",
            "message": "Added to calendar.",
            "mutation_executed": True,
            "event_id": 42,
            "title": event.title,
            "reason": "event_added",
        },
    )

    assert result.status == "command_completed"
    assert result.mutation_executed is True
    assert result.created_event_ids == [42]
    assert result.created_titles == ["Family Field Trip"]
