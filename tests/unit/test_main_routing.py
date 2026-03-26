from datetime import datetime, timezone
import time

import app.main as main_module
from app.services.content_analysis import AnalysisChunk
from app.services.llm import ExtractedEvent


def test_route_extracted_events_falls_back_when_llm_route_fails(monkeypatch):
    candidate = ExtractedEvent(
        title="Pizza Day",
        start_at=datetime(2099, 10, 1, 13, 0, tzinfo=timezone.utc),
        end_at=datetime(2099, 10, 1, 14, 0, tzinfo=timezone.utc),
        category="school",
        confidence=0.9,
        target_scope="child_specific",
        mentioned_names=["Nolan"],
        mentioned_schools=["Frankland"],
        target_grades=["1"],
        preference_match=True,
        model_reason="explicit date",
    )
    sentinel = [{"execution_disposition": "followup_available"}]

    monkeypatch.setattr(main_module.engine_llm, "route_events", lambda **kwargs: (_ for _ in ()).throw(TimeoutError("slow")))
    monkeypatch.setattr(main_module, "_legacy_route_extracted_events", lambda **kwargs: sentinel)

    decisions = main_module._route_extracted_events(
        extracted_events=[candidate],
        children=[],
        priority_preferences={
            "user_priority_topics": [],
            "effective_suppressed_priority_topics": [],
        },
        sender_email_hint="school@example.com",
        sender_name_hint="School Office",
        timezone_hint="America/Toronto",
        evaluation_datetime_utc="2026-03-20T12:00:00Z",
    )

    assert decisions == sentinel


def test_collect_extraction_results_preserves_chunk_order_with_parallel_execution(monkeypatch):
    delays = {1: 0.05, 2: 0.01, 3: 0.03}

    def fake_extract_events(body_text, subject, household_preferences="", timezone_hint="UTC", reference_datetime_hint=""):
        chunk_index = int(body_text)
        time.sleep(delays[chunk_index])
        return {
            "events": [
                ExtractedEvent(
                    title=f"Event {chunk_index}",
                    start_at=datetime(2099, 10, chunk_index, 13, 0, tzinfo=timezone.utc),
                    end_at=datetime(2099, 10, chunk_index, 14, 0, tzinfo=timezone.utc),
                    category="school",
                    confidence=0.9,
                    target_scope="school_global",
                    mentioned_names=[],
                    mentioned_schools=[],
                    target_grades=[],
                    preference_match=False,
                    model_reason="explicit",
                )
            ],
            "email_level_notes": f"note {chunk_index}",
        }

    monkeypatch.setattr(main_module.engine_llm, "extract_events", fake_extract_events)

    chunks = [
        AnalysisChunk(index=1, source_kind="email_body", label="chunk-1", priority_score=10, section_labels=["a"], text="1"),
        AnalysisChunk(index=2, source_kind="email_body", label="chunk-2", priority_score=9, section_labels=["b"], text="2"),
        AnalysisChunk(index=3, source_kind="email_body", label="chunk-3", priority_score=8, section_labels=["c"], text="3"),
    ]

    events, summaries, notes, failures = main_module._collect_extraction_results(
        chunks,
        subject="Newsletter",
        household_preferences="",
        timezone_hint="UTC",
    )

    assert [event.title for event in events] == ["Event 1", "Event 2", "Event 3"]
    assert [summary["chunk_index"] for summary in summaries] == [1, 2, 3]
    assert notes == ["note 1", "note 2", "note 3"]
    assert failures == []


def test_route_extracted_events_applies_deterministic_guardrails_to_llm_downgrade(monkeypatch):
    candidate = ExtractedEvent(
        title="Pizza Day",
        start_at=datetime(2099, 10, 1, 13, 0, tzinfo=timezone.utc),
        end_at=datetime(2099, 10, 1, 14, 0, tzinfo=timezone.utc),
        category="school",
        confidence=0.95,
        target_scope="child_specific",
        mentioned_names=["Nolan"],
        mentioned_schools=["Frankland"],
        target_grades=["1"],
        preference_match=True,
        model_reason="explicit date",
    )
    fallback = [
        {
            "index": 1,
            "validation": {"valid": True, "issues": []},
            "relevancy_evidence": {
                "name_match": True,
                "name_child_ids": [1],
                "teacher_match": False,
                "teacher_child_ids": [],
                "school_match": False,
                "school_child_ids": [],
                "grade_match": False,
                "grade_child_ids": [],
                "preference_match": False,
            },
            "suppressed_match": False,
            "auto_add_decision": {"allow": True, "reason": "household_specific_preference_event"},
            "execution_disposition": "create_event",
            "final_reason": "relevant_and_actionable_auto_add",
        }
    ]
    proposed = [
        {
            "index": 1,
            "validation": {"valid": True, "issues": []},
            "relevancy_evidence": {
                "name_match": True,
                "name_child_ids": [1],
                "teacher_match": False,
                "teacher_child_ids": [],
                "school_match": False,
                "school_child_ids": [],
                "grade_match": False,
                "grade_child_ids": [],
                "preference_match": False,
            },
            "suppressed_match": False,
            "auto_add_decision": {"allow": False, "reason": "needs_confirmation"},
            "execution_disposition": "followup_available",
            "final_reason": "relevant_for_followup",
        }
    ]

    monkeypatch.setattr(main_module, "_legacy_route_extracted_events", lambda **kwargs: fallback)
    monkeypatch.setattr(main_module.engine_llm, "route_events", lambda **kwargs: proposed)
    monkeypatch.setattr(
        main_module.engine_llm,
        "match_event_preferences",
        lambda **kwargs: [
            {
                "index": 1,
                "preference_match": True,
                "matched_positive_topics": ["Bricklabs"],
                "suppressed_match": False,
                "matched_suppressed_topics": [],
            }
        ],
    )

    decisions = main_module._route_extracted_events(
        extracted_events=[candidate],
        children=[],
        priority_preferences={
            "user_priority_topics": [],
            "effective_suppressed_priority_topics": [],
        },
        sender_email_hint="school@example.com",
        sender_name_hint="School Office",
        timezone_hint="America/Toronto",
        evaluation_datetime_utc="2026-03-20T12:00:00Z",
        document_understanding={"routing_hints": {"recap_like": True}},
    )

    assert decisions[0]["execution_disposition"] == "followup_available"
    assert decisions[0]["validation"] == fallback[0]["validation"]
    assert decisions[0]["auto_add_decision"] == fallback[0]["auto_add_decision"]


def test_route_extracted_events_uses_llm_preference_matches_over_deterministic_fallback(monkeypatch):
    candidate = ExtractedEvent(
        title="Brick Labs club registration opens",
        start_at=datetime(2099, 10, 1, 13, 0, tzinfo=timezone.utc),
        end_at=datetime(2099, 10, 1, 14, 0, tzinfo=timezone.utc),
        category="school",
        confidence=0.95,
        target_scope="school_global",
        mentioned_names=[],
        mentioned_schools=["Frankland"],
        target_grades=[],
        preference_match=False,
        model_reason="registration notice",
    )
    fallback = [
        {
            "index": 1,
            "validation": {"valid": True, "issues": []},
            "relevancy_evidence": {
                "name_match": False,
                "name_child_ids": [],
                "teacher_match": False,
                "teacher_child_ids": [],
                "school_match": False,
                "school_child_ids": [],
                "grade_match": False,
                "grade_child_ids": [],
                "preference_match": False,
            },
            "suppressed_match": False,
            "matched_positive_topics": [],
            "matched_suppressed_topics": [],
            "auto_add_decision": {"allow": False, "reason": "needs_confirmation"},
            "execution_disposition": "ignore",
            "final_reason": "not_relevant",
        }
    ]
    proposed = [
        {
            "index": 1,
            "validation": {"valid": True, "issues": []},
            "relevancy_evidence": {
                "name_match": False,
                "name_child_ids": [],
                "teacher_match": False,
                "teacher_child_ids": [],
                "school_match": False,
                "school_child_ids": [],
                "grade_match": False,
                "grade_child_ids": [],
                "preference_match": True,
                "matched_positive_topics": ["Bricklabs"],
            },
            "suppressed_match": False,
            "matched_suppressed_topics": [],
            "auto_add_decision": {"allow": False, "reason": "needs_confirmation"},
            "execution_disposition": "followup_available",
            "final_reason": "relevant_for_followup",
        }
    ]

    monkeypatch.setattr(main_module, "_legacy_route_extracted_events", lambda **kwargs: fallback)
    monkeypatch.setattr(main_module.engine_llm, "route_events", lambda **kwargs: proposed)
    monkeypatch.setattr(
        main_module.engine_llm,
        "match_event_preferences",
        lambda **kwargs: [
            {
                "index": 1,
                "preference_match": True,
                "matched_positive_topics": ["Bricklabs"],
                "suppressed_match": False,
                "matched_suppressed_topics": [],
            }
        ],
    )

    decisions = main_module._route_extracted_events(
        extracted_events=[candidate],
        children=[],
        priority_preferences={
            "user_priority_topics": ["Bricklabs"],
            "effective_suppressed_priority_topics": [],
        },
        sender_email_hint="school@example.com",
        sender_name_hint="School Office",
        timezone_hint="America/Toronto",
        evaluation_datetime_utc="2026-03-20T12:00:00Z",
        document_understanding={"routing_hints": {"recap_like": False}},
    )

    assert decisions[0]["relevancy_evidence"]["preference_match"] is True
    assert decisions[0]["matched_positive_topics"] == ["Bricklabs"]
    assert decisions[0]["execution_disposition"] == "followup_available"


def test_route_extracted_events_can_use_dedicated_preference_matcher_when_route_model_misses(monkeypatch):
    candidate = ExtractedEvent(
        title="Brick Labs club registration opens",
        start_at=datetime(2099, 10, 1, 13, 0, tzinfo=timezone.utc),
        end_at=datetime(2099, 10, 1, 14, 0, tzinfo=timezone.utc),
        category="registration",
        confidence=0.95,
        target_scope="school_global",
        mentioned_names=[],
        mentioned_schools=["Frankland"],
        target_grades=[],
        preference_match=False,
        model_reason="registration notice",
    )
    fallback = [
        {
            "index": 1,
            "validation": {"valid": True, "issues": []},
            "relevancy_evidence": {
                "name_match": False,
                "name_child_ids": [],
                "teacher_match": False,
                "teacher_child_ids": [],
                "school_match": False,
                "school_child_ids": [],
                "grade_match": False,
                "grade_child_ids": [],
                "preference_match": False,
            },
            "suppressed_match": False,
            "matched_positive_topics": [],
            "matched_suppressed_topics": [],
            "auto_add_decision": {"allow": False, "reason": "optional_or_admin_event"},
            "execution_disposition": "ignore",
            "final_reason": "not_relevant",
        }
    ]
    proposed = [
        {
            "index": 1,
            "validation": {"valid": True, "issues": []},
            "relevancy_evidence": {
                "name_match": False,
                "name_child_ids": [],
                "teacher_match": False,
                "teacher_child_ids": [],
                "school_match": False,
                "school_child_ids": [],
                "grade_match": False,
                "grade_child_ids": [],
                "preference_match": False,
            },
            "suppressed_match": False,
            "matched_positive_topics": [],
            "matched_suppressed_topics": [],
            "auto_add_decision": {"allow": False, "reason": "optional_or_admin_event"},
            "execution_disposition": "ignore",
            "final_reason": "not_relevant",
        }
    ]

    monkeypatch.setattr(main_module, "_legacy_route_extracted_events", lambda **kwargs: fallback)
    monkeypatch.setattr(main_module.engine_llm, "route_events", lambda **kwargs: proposed)
    monkeypatch.setattr(
        main_module.engine_llm,
        "match_event_preferences",
        lambda **kwargs: [
            {
                "index": 1,
                "preference_match": True,
                "matched_positive_topics": ["Bricklabs"],
                "suppressed_match": False,
                "matched_suppressed_topics": [],
            }
        ],
    )

    decisions = main_module._route_extracted_events(
        extracted_events=[candidate],
        children=[],
        priority_preferences={
            "user_priority_topics": ["Bricklabs"],
            "effective_suppressed_priority_topics": [],
        },
        sender_email_hint="school@example.com",
        sender_name_hint="School Office",
        timezone_hint="America/Toronto",
        evaluation_datetime_utc="2026-03-20T12:00:00Z",
        document_understanding={"routing_hints": {"recap_like": False}},
    )

    assert decisions[0]["relevancy_evidence"]["preference_match"] is True
    assert decisions[0]["matched_positive_topics"] == ["Bricklabs"]
    assert decisions[0]["execution_disposition"] == "followup_available"
