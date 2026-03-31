from datetime import datetime, timezone
import json
import time
from types import SimpleNamespace

import httpx
from agents.run_config import ModelInputData
from openai import APIConnectionError

from app.services.agent_threads import ThreadDocumentContext
from app.services.llm import (
    COMMAND_EXECUTION_SYSTEM_PROMPT,
    COMMAND_SYSTEM_PROMPT,
    compact_document_understanding_for_downstream,
    CommandExecutionOutput,
    CommandToolRuntime,
    CommandParseOutput,
    DOCUMENT_UNDERSTANDING_PROMPT_VERSION,
    DOCUMENT_UNDERSTANDING_SYSTEM_PROMPT,
    DocumentRoutingHintsOutput,
    DocumentTopicOutput,
    DocumentUnderstandingOutput,
    EventRoutingAutoAddOutput,
    EventRoutingDecisionOutput,
    EventRoutingOutput,
    EventRoutingRelevancyOutput,
    EventRoutingValidationOutput,
    ExtractedEvent,
    EventExtractionOutput,
    ExtractedEventOutput,
    FORWARDED_INTENT_SYSTEM_PROMPT,
    ForwardedPrefaceIntentOutput,
    MORE_INFO_SYSTEM_PROMPT,
    MockDecisionEngine,
    MoreInfoReplyOutput,
    OpenAIDecisionEngine,
    PREFERENCE_PARSE_SYSTEM_PROMPT,
    PreferenceClauseClassificationOutput,
    PreferenceTopicMatchDecisionOutput,
    PreferenceTopicMatchingOutput,
    PreferenceParseOutput,
    SummaryCandidateExtractionOutput,
    SummaryCompressionOutput,
    SUMMARY_COMPRESSION_SYSTEM_PROMPT,
    SUMMARY_COMPRESSION_PROMPT_VERSION,
    SUMMARY_EXTRACTION_SYSTEM_PROMPT,
    EXTRACTION_PROMPT_VERSION,
    EXTRACTION_SYSTEM_PROMPT,
)
from app.services.openai_tracing import current_trace_context, request_trace_context


def test_openai_engine_extract_and_command(monkeypatch):
    captured = []

    def fake_run_agent(self, **kwargs):
        captured.append(kwargs)
        if kwargs["output_type"] is EventExtractionOutput:
            return EventExtractionOutput(
                events=[
                    ExtractedEventOutput(
                        title="Closure",
                        start_at="2099-10-01T08:30:00Z",
                        end_at="2099-10-01T09:30:00Z",
                        category="school_closure",
                        confidence=0.94,
                        target_scope="school_global",
                        mentioned_names=["Nolan"],
                        mentioned_schools=["Frankland"],
                        target_grades=["1"],
                        preference_match=True,
                        model_reason="closure_urgent",
                    )
                ],
                email_level_notes=None,
            )
        if kwargs["output_type"] is CommandParseOutput:
            return CommandParseOutput(
                action="more_info",
                execution_strategy="semantic",
                event_id=None,
                topic="Pizza Lunch",
                preference_behavior=None,
                minutes_before=30,
                reminder_channel="sms",
                async_requested=False,
                confidence=0.96,
            )
        return ForwardedPrefaceIntentOutput(
            mode="command",
            action="add",
            execution_strategy="deterministic",
            event_id=None,
            topic=None,
            preference_behavior=None,
            minutes_before=None,
            reminder_channel=None,
            async_requested=False,
            confidence=0.95,
            reason="preface_supported_action",
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    result = engine.extract_events(
        body_text="School closure on 2099-10-01 08:30",
        subject="Closure",
        household_preferences="Closures are critical",
        timezone_hint="UTC",
    )
    assert result["email_level_notes"] is None
    assert len(result["events"]) == 1
    assert result["events"][0].confidence > 0.9
    assert result["events"][0].start_at.tzinfo == timezone.utc
    assert result["events"][0].target_scope == "school_global"
    assert result["events"][0].target_grades == ["1"]
    assert result["events"][0].end_at is not None

    command = engine.parse_command("more info about Pizza Lunch")
    assert command["action"] == "more_info"
    assert command["execution_strategy"] == "semantic"
    assert command["event_id"] is None
    assert command["topic"] == "Pizza Lunch"
    assert command["minutes_before"] == 30

    forwarded_intent = engine.parse_forwarded_preface_intent(
        user_preface="Add this to the calendar",
        forwarded_subject="Pizza Lunch",
        forwarded_sender="Frankland CS <donotreply@tdsb.on.ca>",
        forwarded_date="Tue, Mar 10, 2026 at 8:21 AM",
    )
    assert forwarded_intent["mode"] == "command"
    assert forwarded_intent["action"] == "add"
    assert forwarded_intent["execution_strategy"] == "deterministic"
    assert [item["use_session"] for item in captured] == [False, True, True]


def test_openai_engine_understand_document_uses_agent_sdk(monkeypatch):
    captured = {}

    def fake_run_agent(self, **kwargs):
        captured.update(kwargs)
        return DocumentUnderstandingOutput(
            document_kind="newsletter",
            overall_intent="mixed",
            assistant_summary="This newsletter mixes reminders with a few schedule-related items.",
            assistant_intro="This update is mostly informational, but there are a couple of items worth checking.",
            actionable_topics=[
                DocumentTopicOutput(
                    title="Pizza Lunch order window",
                    why_it_matters="The update suggests there is a lunch-related action to check.",
                    action_hint="Check whether an order deadline is coming up.",
                    timing_hint="Soon",
                    scope_hint="household_specific",
                )
            ],
            informational_topics=[],
            routing_hints=DocumentRoutingHintsOutput(
                recap_like=False,
                resource_share_like=False,
                contains_calendar_relevant_items=True,
            ),
            notes=["Grounded from the merged body and attachment text."],
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    result = engine.understand_document(
        analysis_text="Attachment: newsletter.pdf\nPizza lunch ordering opens this week.",
        subject="Frankland Newsletter",
        household_preferences="Pizza Lunch matters",
        timezone_hint="America/Toronto",
        reference_datetime_hint="2026-03-21T12:00:00+00:00",
        forwarded_subject="Fwd: Frankland Newsletter",
        forwarded_sender="Frankland CS <school@example.com>",
        forwarded_date="Fri, Mar 20, 2026 at 8:00 AM",
    )

    assert result["document_kind"] == "newsletter"
    assert result["routing_hints"]["contains_calendar_relevant_items"] is True
    assert captured["agent_name"] == "document_understanding"
    assert captured["prompt_version"] == DOCUMENT_UNDERSTANDING_PROMPT_VERSION
    assert captured["use_session"] is False
    assert captured["inject_conversation_context"] is True
    assert "school communication assistant" in captured["system_prompt"].lower()
    assert DOCUMENT_UNDERSTANDING_SYSTEM_PROMPT == captured["system_prompt"]
    assert "forwarded_subject" in captured["user_payload"]
    assert "newsletter.pdf" in captured["user_payload"]


def test_mock_preference_parser_segments_and_resolves_negative_conflicts():
    engine = MockDecisionEngine()

    parsed = engine.parse_preference_notes(
        raw_text="i care about pizza days, swim days. I don't care about cultural days.",
        preset_topics=["Pizza Days", "Hot Lunch Programs", "Swim Days", "Spirit Days"],
    )

    assert parsed["positive_topics"] == ["Pizza Days", "Swim Days"]
    assert parsed["negative_topics"] == ["Cultural Days"]


def test_mock_preference_parser_negative_wins_when_same_topic_is_mixed():
    engine = MockDecisionEngine()

    parsed = engine.parse_preference_notes(
        raw_text="pizza days. I don't care about pizza days.",
        preset_topics=["Pizza Days", "Hot Lunch Programs", "Swim Days", "Spirit Days"],
    )

    assert parsed["positive_topics"] == []
    assert parsed["negative_topics"] == ["Pizza Days"]


def test_mock_preference_parser_allows_negative_only_sentences():
    engine = MockDecisionEngine()

    parsed = engine.parse_preference_notes(
        raw_text="ignore school council and stop telling me about cultural days",
        preset_topics=["Pizza Days", "Hot Lunch Programs", "Swim Days", "Spirit Days"],
    )

    assert parsed["positive_topics"] == []
    assert parsed["negative_topics"] == ["School Council", "Cultural Days"]


def test_openai_engine_defaults_end_at_when_missing(monkeypatch):
    def fake_run_agent(self, **kwargs):
        return EventExtractionOutput(
            events=[
                ExtractedEventOutput(
                    title="Pizza Day",
                    start_at="2099-10-01T08:30:00Z",
                    end_at=None,
                    category="school",
                    confidence=0.94,
                    target_scope="child_specific",
                    mentioned_names=["Nolan"],
                    mentioned_schools=["Frankland"],
                    target_grades=[],
                    preference_match=True,
                    model_reason="explicit",
                )
            ],
            email_level_notes=None,
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")
    result = engine.extract_events(
        body_text="Pizza day on 2099-10-01 08:30",
        subject="Pizza Day",
        household_preferences="Pizza Day",
        timezone_hint="UTC",
    )
    event = result["events"][0]
    assert event.start_at is not None
    assert event.end_at is not None
    assert event.end_at > event.start_at


def test_openai_engine_treats_date_only_values_as_local_all_day(monkeypatch):
    def fake_run_agent(self, **kwargs):
        return EventExtractionOutput(
            events=[
                ExtractedEventOutput(
                    title="PA Day",
                    start_at="2025-09-26",
                    end_at=None,
                    category="school_closure",
                    confidence=0.98,
                    target_scope="school_global",
                    mentioned_names=[],
                    mentioned_schools=["Frankland"],
                    target_grades=[],
                    preference_match=False,
                    model_reason="Explicit no-school date for students.",
                )
            ],
            email_level_notes=None,
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")
    result = engine.extract_events(
        body_text="Sept. 26 - PA Day (no school for students)",
        subject="Newsletter",
        household_preferences="Closures",
        timezone_hint="America/Toronto",
    )

    event = result["events"][0]
    assert event.start_at == datetime.fromisoformat("2025-09-26T04:00:00+00:00")
    assert event.end_at == datetime.fromisoformat("2025-09-27T04:00:00+00:00")


def test_openai_engine_expands_same_day_date_only_ranges_to_all_day(monkeypatch):
    def fake_run_agent(self, **kwargs):
        return EventExtractionOutput(
            events=[
                ExtractedEventOutput(
                    title="Thanksgiving",
                    start_at="2025-10-13",
                    end_at="2025-10-13",
                    category="school_closure",
                    confidence=0.98,
                    target_scope="school_global",
                    mentioned_names=[],
                    mentioned_schools=["Frankland"],
                    target_grades=[],
                    preference_match=False,
                    model_reason="Explicit holiday listed.",
                )
            ],
            email_level_notes=None,
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")
    result = engine.extract_events(
        body_text="Oct. 13 - Thanksgiving (no school)",
        subject="Newsletter",
        household_preferences="Closures",
        timezone_hint="America/Toronto",
    )

    event = result["events"][0]
    assert event.start_at == datetime.fromisoformat("2025-10-13T04:00:00+00:00")
    assert event.end_at == datetime.fromisoformat("2025-10-14T04:00:00+00:00")


def test_prompt_constants_are_explicit():
    assert "Return ONLY JSON" in EXTRACTION_SYSTEM_PROMPT
    assert "Extract every distinct event mention" in EXTRACTION_SYSTEM_PROMPT
    assert "Do not use household preferences as a filter" in EXTRACTION_SYSTEM_PROMPT
    assert "infer the same year" in EXTRACTION_SYSTEM_PROMPT
    assert "Forwarded email header lines" in EXTRACTION_SYSTEM_PROMPT
    assert "reference_datetime_hint" in EXTRACTION_SYSTEM_PROMPT
    assert "<output_contract>" in EXTRACTION_SYSTEM_PROMPT
    assert "<verification_loop>" in EXTRACTION_SYSTEM_PROMPT
    assert "Allowed actions only" in COMMAND_SYSTEM_PROMPT
    assert "update" in COMMAND_SYSTEM_PROMPT
    assert "Allowed execution strategies only" in COMMAND_SYSTEM_PROMPT
    assert "Treat the current user-authored message as the primary source of intent." in COMMAND_SYSTEM_PROMPT
    assert "<decision_rules>" in COMMAND_SYSTEM_PROMPT
    assert "more_info" in COMMAND_SYSTEM_PROMPT
    assert "Allowed modes only" in FORWARDED_INTENT_SYSTEM_PROMPT
    assert "command execution orchestrator" in COMMAND_EXECUTION_SYSTEM_PROMPT
    assert "Allowed execution strategies only" in FORWARDED_INTENT_SYSTEM_PROMPT
    assert "Treat `user_preface` as the only user-authored intent text" in FORWARDED_INTENT_SYSTEM_PROMPT
    assert "forwarded content" in FORWARDED_INTENT_SYSTEM_PROMPT
    assert "preference clause classifier" in PREFERENCE_PARSE_SYSTEM_PROMPT
    assert "one object per supplied clause" in PREFERENCE_PARSE_SYSTEM_PROMPT
    assert 'Preserve negatives like "I don\'t care about cultural days" as negative.' in PREFERENCE_PARSE_SYSTEM_PROMPT
    assert "first-party source material" in COMMAND_EXECUTION_SYSTEM_PROMPT
    assert "add_calendar_event_from_context_tool" in COMMAND_EXECUTION_SYSTEM_PROMPT
    assert "Treat supplied event facts, section snippets, fallback candidates, household context, and any supplied thread document context as the only factual sources." in SUMMARY_EXTRACTION_SYSTEM_PROMPT
    assert "Prefer event-fact wording over noisier section-snippet wording" in SUMMARY_EXTRACTION_SYSTEM_PROMPT
    assert "Do not emit fragmentary copied prose" in SUMMARY_EXTRACTION_SYSTEM_PROMPT
    assert "first-party source material" in SUMMARY_COMPRESSION_SYSTEM_PROMPT
    assert "prefer the strongest supported candidate" in SUMMARY_COMPRESSION_SYSTEM_PROMPT
    assert "Do not emit unfinished sentences" in SUMMARY_COMPRESSION_SYSTEM_PROMPT
    assert "Do not repeat the same dated item in `other_dates`" in SUMMARY_COMPRESSION_SYSTEM_PROMPT
    assert "follow-up assistant" in MORE_INFO_SYSTEM_PROMPT
    assert "Start with the answer" in MORE_INFO_SYSTEM_PROMPT
    assert "Prefer paraphrase over copying" in MORE_INFO_SYSTEM_PROMPT


def test_openai_preference_parser_uses_segmented_clause_payload(monkeypatch):
    captured = {}

    def fake_run_agent(self, **kwargs):
        captured["system_prompt"] = kwargs["system_prompt"]
        captured["user_payload"] = kwargs["user_payload"]
        assert kwargs["prompt_version"] != SUMMARY_COMPRESSION_PROMPT_VERSION
        return PreferenceParseOutput(
            classifications=[
                PreferenceClauseClassificationOutput(
                    clause="i care about pizza days, swim days.",
                    polarity="positive",
                    topic="Pizza Days, Swim Days",
                    aliases=[],
                ),
                PreferenceClauseClassificationOutput(
                    clause="I don't care about cultural days.",
                    polarity="negative",
                    topic="Heritage Months",
                    aliases=["heritage month", "cultural event", "awareness month"],
                ),
            ]
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    parsed = engine.parse_preference_notes(
        raw_text="i care about pizza days, swim days. I don't care about cultural days.",
        preset_topics=["Pizza Days", "Hot Lunch Programs", "Swim Days", "Spirit Days"],
    )

    assert parsed["positive_topics"] == ["Pizza Days", "Swim Days"]
    assert parsed["negative_topics"] == ["Heritage Months"]
    assert "clause classifier" in captured["system_prompt"]
    assert '"clauses": [' in captured["user_payload"]
    assert '"i care about pizza days, swim days."' in captured["user_payload"]
    assert '"I don\'t care about cultural days."' in captured["user_payload"]


def test_openai_extract_includes_reference_datetime_hint(monkeypatch):
    captured = {}

    def fake_run_agent(self, **kwargs):
        captured["user_payload"] = kwargs["user_payload"]
        assert kwargs["prompt_version"] == EXTRACTION_PROMPT_VERSION
        return EventExtractionOutput(events=[], email_level_notes=None)

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    engine.extract_events(
        body_text="Tomorrow is swim day.",
        subject="Room 106",
        household_preferences="Swim Days",
        timezone_hint="America/Toronto",
        reference_datetime_hint="2026-03-01T18:53:00-05:00",
    )

    assert "reference_datetime_hint:" in captured["user_payload"]
    assert "2026-03-01T18:53:00-05:00" in captured["user_payload"]


def test_openai_extract_uses_compact_document_context_without_conversation_injection(monkeypatch):
    captured = {}
    document_understanding = {
        "document_kind": "newsletter",
        "overall_intent": "mixed",
        "assistant_summary": "A" * 500,
        "assistant_intro": "Intro " * 80,
        "actionable_topics": [
            {
                "title": f"Action {idx}",
                "why_it_matters": f"Why {idx}",
                "action_hint": f"Do {idx}",
                "timing_hint": f"When {idx}",
                "scope_hint": "school_global",
            }
            for idx in range(1, 7)
        ],
        "informational_topics": [
            {
                "title": f"Info {idx}",
                "why_it_matters": f"Info why {idx}",
                "action_hint": None,
                "timing_hint": None,
                "scope_hint": "school_global",
            }
            for idx in range(1, 7)
        ],
        "routing_hints": {"recap_like": True, "resource_share_like": False, "contains_calendar_relevant_items": True},
        "notes": [f"Note {idx}" for idx in range(1, 6)],
    }

    def fake_run_agent(self, **kwargs):
        captured.update(kwargs)
        return EventExtractionOutput(events=[], email_level_notes=None)

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    engine.extract_events(
        body_text="Tomorrow is swim day.",
        subject="Room 106",
        household_preferences="Swim Days",
        timezone_hint="America/Toronto",
        document_understanding=document_understanding,
    )

    compact = compact_document_understanding_for_downstream(document_understanding)
    assert captured["inject_conversation_context"] is False
    assert json.dumps(compact, ensure_ascii=True, indent=2) in captured["user_payload"]
    assert '"Action 5"' not in captured["user_payload"]
    assert '"Info 5"' not in captured["user_payload"]
    assert '"Note 4"' not in captured["user_payload"]


def test_openai_engine_does_not_fallback_to_mock_when_model_returns_no_events(monkeypatch):
    monkeypatch.setattr(
        OpenAIDecisionEngine,
        "_run_agent",
        lambda self, **kwargs: EventExtractionOutput(events=[], email_level_notes=None),
    )
    engine = OpenAIDecisionEngine(api_key="test-key")

    result = engine.extract_events(
        body_text="No event content here.",
        subject="Newsletter",
        household_preferences="Pizza Day",
        timezone_hint="UTC",
    )

    assert result["events"] == []
    assert result["email_level_notes"] == "empty_model_events"


def test_gpt5_models_use_agents_reasoning_settings():
    engine = OpenAIDecisionEngine(api_key="test-key", model="gpt-5-mini-2025-08-07")
    settings = engine._agent_model_settings()

    assert settings.temperature is None
    assert settings.reasoning is not None
    assert settings.reasoning.effort == "medium"
    assert settings.store is False


def test_openai_engine_can_opt_in_to_stored_responses():
    engine = OpenAIDecisionEngine(
        api_key="test-key",
        model="gpt-5-mini-2025-08-07",
        store_responses=True,
    )
    settings = engine._agent_model_settings()

    assert settings.store is True


def test_openai_engine_uses_configured_sdk_timeout():
    engine = OpenAIDecisionEngine(api_key="test-key", timeout_sec=17)
    client = engine._new_openai_client()

    assert client.timeout == 17
    assert client.max_retries == 0


def test_openai_metadata_reports_agents_runtime():
    engine = OpenAIDecisionEngine(api_key="test-key")
    metadata = engine.metadata()

    assert metadata["runtime"] == "openai_agents"
    assert metadata["api"] == "responses"
    assert "sdk_version" in metadata


def test_openai_engine_injects_household_context_and_thread_documents_into_model_input():
    engine = OpenAIDecisionEngine(api_key="test-key")
    thread_documents = [
        ThreadDocumentContext(
            filename="newsletter.pdf",
            content_type="application/pdf",
            extracted_text="The pizza lunch form is due Friday.",
            openai_file_id="file_123",
        )
    ]
    household_context = {
        "timezone": "America/Toronto",
        "children": [{"name": "Nolan", "grade": "1"}],
        "preferences": {"raw_text": "Closures are critical"},
    }
    data = SimpleNamespace(
        model_data=ModelInputData(
            input=[{"role": "user", "content": [{"type": "input_text", "text": "Summarize the thread."}]}],
            instructions="Test instructions",
        )
    )

    with engine.conversation_scope(
        session_id="email:1:test-thread",
        thread_documents=thread_documents,
        household_context=household_context,
    ):
        injected = engine._inject_conversation_context(data)

    assert len(injected.input) == 3
    household_text = injected.input[0]["content"][0]["text"]
    assert "Household context" in household_text
    assert '"timezone": "America/Toronto"' in household_text
    assert '"name": "Nolan"' in household_text
    assert '"raw_text": "Closures are critical"' in household_text

    document_text = injected.input[1]["content"][0]["text"]
    assert "Thread document context" in document_text
    assert "newsletter.pdf" in document_text
    assert "pizza lunch form is due friday" in document_text.lower()
    assert injected.input[2]["content"][0]["text"] == "Summarize the thread."


def test_openai_engine_filters_non_conversational_session_history():
    engine = OpenAIDecisionEngine(api_key="test-key")
    history = [
        {"role": "user", "content": [{"type": "input_text", "text": "Original user turn"}]},
        {"role": "assistant", "content": [{"type": "output_text", "text": '{"action":"none"}'}]},
        {"role": "assistant", "content": [{"type": "output_text", "text": "Helpful follow-up"}]},
        {"content": [{"type": "output_text", "text": "Missing role"}]},
    ]
    new_items = [{"role": "user", "content": [{"type": "input_text", "text": "Newest turn"}]}]

    merged = engine._merge_session_history(history, new_items)

    assert [item["content"][0]["text"] for item in merged] == [
        "Original user turn",
        "Helpful follow-up",
        "Newest turn",
    ]


def test_openai_engine_routes_events_via_agent_sdk(monkeypatch):
    captured = {}

    def fake_run_agent(self, **kwargs):
        captured["agent_name"] = kwargs["agent_name"]
        captured["system_prompt"] = kwargs["system_prompt"]
        captured["user_payload"] = kwargs["user_payload"]
        captured["use_session"] = kwargs["use_session"]
        return EventRoutingOutput(
            decisions=[
                EventRoutingDecisionOutput(
                    index=1,
                    validation=EventRoutingValidationOutput(valid=True, issues=[]),
                    relevancy_evidence=EventRoutingRelevancyOutput(
                        name_match=True,
                        name_child_ids=[7],
                        teacher_match=False,
                        teacher_child_ids=[],
                        school_match=True,
                        school_child_ids=[7],
                        grade_match=False,
                        grade_child_ids=[],
                        preference_match=False,
                    ),
                    suppressed_match=False,
                    auto_add_decision=EventRoutingAutoAddOutput(
                        allow=True,
                        reason="closure_or_break",
                    ),
                    execution_disposition="create_event",
                    final_reason="relevant_and_actionable_auto_add",
                )
            ]
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    decisions = engine.route_events(
        extracted_events=[
            ExtractedEvent(
                title="Closure",
                start_at=datetime.fromisoformat("2099-10-01T08:30:00+00:00"),
                end_at=datetime.fromisoformat("2099-10-01T09:30:00+00:00"),
                category="school_closure",
                confidence=0.98,
                target_scope="school_global",
                mentioned_names=["Nolan"],
                mentioned_schools=["Frankland"],
                target_grades=["1"],
                preference_match=True,
                model_reason="closure_urgent",
            )
        ],
        children=[SimpleNamespace(id=7, name="Nolan", school_name="Frankland", grade="1", teacher_contacts=[])],
        positive_preference_topics=["Closures"],
        suppressed_priority_topics=[],
        sender_email="teacher@example.com",
        sender_display_name="Teacher",
        timezone_hint="America/Toronto",
        evaluation_datetime_utc="2099-09-01T12:00:00+00:00",
        document_understanding={
            "document_kind": "newsletter",
            "overall_intent": "mixed",
            "routing_hints": {"recap_like": False, "resource_share_like": False, "contains_calendar_relevant_items": True},
        },
    )

    assert decisions[0]["execution_disposition"] == "create_event"
    assert decisions[0]["relevancy_evidence"]["name_child_ids"] == [7]
    assert captured["agent_name"] == "event_routing"
    assert captured["use_session"] is False
    assert "event routing arbiter" in captured["system_prompt"].lower()
    assert '"evaluation_datetime_utc": "2099-09-01T12:00:00+00:00"' in captured["user_payload"]
    assert '"document_understanding"' in captured["user_payload"]


def test_openai_engine_matches_event_preferences_via_agent_sdk(monkeypatch):
    captured = {}

    def fake_run_agent(self, **kwargs):
        captured["agent_name"] = kwargs["agent_name"]
        captured["system_prompt"] = kwargs["system_prompt"]
        captured["user_payload"] = kwargs["user_payload"]
        return PreferenceTopicMatchingOutput(
            decisions=[
                PreferenceTopicMatchDecisionOutput(
                    index=1,
                    preference_match=True,
                    matched_positive_topics=["Bricklabs"],
                    suppressed_match=False,
                    matched_suppressed_topics=[],
                )
            ]
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    decisions = engine.match_event_preferences(
        extracted_events=[
            ExtractedEvent(
                title="Brick Labs club registration opens",
                start_at=datetime.fromisoformat("2099-10-01T08:30:00+00:00"),
                end_at=datetime.fromisoformat("2099-10-01T09:30:00+00:00"),
                category="registration",
                confidence=0.94,
                target_scope="school_specific",
                mentioned_names=[],
                mentioned_schools=["Frankland"],
                target_grades=[],
                preference_match=False,
                model_reason="registration opens",
            )
        ],
        positive_preference_topics=["Bricklabs"],
        suppressed_priority_topics=[],
        document_understanding={"routing_hints": {"contains_calendar_relevant_items": True}},
    )

    assert decisions == [
        {
            "index": 1,
            "preference_match": True,
            "matched_positive_topics": ["Bricklabs"],
            "suppressed_match": False,
            "matched_suppressed_topics": [],
        }
    ]
    assert captured["agent_name"] == "preference_match"
    assert "household preference matcher" in captured["system_prompt"].lower()
    assert '"positive_preference_topics": [' in captured["user_payload"]
    assert '"Bricklabs"' in captured["user_payload"]


def test_request_trace_context_sets_and_resets():
    assert current_trace_context() is None

    with request_trace_context(
        workflow_name="lovelychaos.resend_inbound",
        group_id="req-123",
        metadata={"path": "/webhooks/resend/inbound"},
    ):
        context = current_trace_context()
        assert context is not None
        assert context.workflow_name == "lovelychaos.resend_inbound"
        assert context.group_id == "req-123"
        assert context.metadata == {"path": "/webhooks/resend/inbound"}

    assert current_trace_context() is None


def test_openai_engine_builds_one_trace_name_per_agent_call(monkeypatch):
    captured = {}

    class _FakeResult:
        def final_output_as(self, output_type):
            assert output_type is CommandParseOutput
            return CommandParseOutput(
                action="add",
                execution_strategy="deterministic",
                event_id=None,
                topic=None,
                preference_behavior=None,
                minutes_before=0,
                reminder_channel=None,
                async_requested=False,
                confidence=0.9,
            )

    def fake_run_sync(agent, input, context, run_config, session):
        captured["agent_name"] = agent.name
        captured["workflow_name"] = run_config.workflow_name
        captured["group_id"] = run_config.group_id
        captured["trace_metadata"] = run_config.trace_metadata
        return _FakeResult()

    monkeypatch.setattr("app.services.llm.Runner.run_sync", fake_run_sync)
    engine = OpenAIDecisionEngine(api_key="test-key")

    with engine.conversation_scope(
        session_id="email:1:thread-1",
        workflow_name="lovelychaos.resend_inbound",
        group_id="thread-1",
        trace_metadata={"path": "/webhooks/resend/inbound"},
    ):
        command = engine.parse_command("add this to the calendar")

    assert command["action"] == "add"
    assert captured["agent_name"] == "command_parse"
    assert captured["workflow_name"] == "lovelychaos.resend_inbound.command_parse"
    assert captured["group_id"] == "thread-1"
    assert captured["trace_metadata"]["path"] == "/webhooks/resend/inbound"
    assert captured["trace_metadata"]["agent_name"] == "command_parse"
    assert captured["trace_metadata"]["session_id"] == "email:1:thread-1"


def test_openai_engine_execute_command_with_tools_passes_context_and_tools(monkeypatch):
    captured = {}

    class _FakeResult:
        def final_output_as(self, output_type):
            assert output_type is CommandExecutionOutput
            return CommandExecutionOutput(
                status="command_completed",
                message="Saved preference for Pizza Days.",
                mutation_executed=True,
                action="set_preference",
                tool_name="update_preferences_tool",
            )

    def fake_run_sync(agent, input, context, run_config, session):
        captured["agent_name"] = agent.name
        captured["tool_names"] = [tool.name for tool in agent.tools]
        captured["context"] = context
        captured["workflow_name"] = run_config.workflow_name
        return _FakeResult()

    monkeypatch.setattr("app.services.llm.Runner.run_sync", fake_run_sync)
    engine = OpenAIDecisionEngine(api_key="test-key")
    runtime = CommandToolRuntime(
        household_id=1,
        response_channel="sms",
        timezone_name="America/Toronto",
        current_message="always add pizza days",
        read_preferences=lambda: {"ok": True},
        update_preference=lambda topic, behavior, reason: {"ok": True, "topic": topic, "behavior": behavior},
        search_calendar=lambda query, from_iso, to_iso, limit: {"ok": True, "items": []},
        add_calendar_event_from_context=lambda query, title, start_at_iso, end_at_iso, all_day: {"ok": True},
        update_calendar_event=lambda event_id, query, title, location, start_at_iso, end_at_iso, all_day: {"ok": True},
        delete_calendar_event=lambda event_id, query: {"ok": True},
        set_calendar_reminder=lambda event_id, query, minutes_before, reminder_channel: {"ok": True},
        notes={},
    )

    with engine.conversation_scope(session_id="sms:1", workflow_name="lovelychaos.sms", group_id="sms:1"):
        result = engine.execute_command_with_tools(
            {
                "message_text": "always add pizza days",
                "parsed_command": {
                    "action": "set_preference",
                    "execution_strategy": "deterministic",
                    "topic": "Pizza Days",
                    "preference_behavior": "auto_add",
                },
            },
            runtime,
        )

    assert result["status"] == "command_completed"
    assert captured["agent_name"] == "command_execute"
    assert "update_preferences_tool" in captured["tool_names"]
    assert "add_calendar_event_from_context_tool" in captured["tool_names"]
    assert captured["context"] is runtime
    assert captured["workflow_name"] == "lovelychaos.sms.command_execute"


def test_openai_engine_compose_more_info_reply_uses_agent_sdk(monkeypatch):
    captured = {}

    def fake_run_agent(self, **kwargs):
        captured["agent_name"] = kwargs["agent_name"]
        captured["system_prompt"] = kwargs["system_prompt"]
        captured["user_payload"] = kwargs["user_payload"]
        captured["use_session"] = kwargs["use_session"]
        captured["inject_conversation_context"] = kwargs["inject_conversation_context"]
        return MoreInfoReplyOutput(
            message="Family Math Night looks like a recap rather than a new event. The school shared slideshow and activity links plus a feedback form."
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    result = engine.compose_more_info_reply(
        {
            "user_query": "tell me more about family math night",
            "summary_title": "Frankland CS Update",
            "summary_line": "Family Math Night 2026 recap/feedback",
            "matched_item": {
                "title": "Family Math Night",
                "kind": "topic",
                "reason": "Newsletter references a recap/slideshow and feedback form for Family Math Night; no date or time is stated in the email body.",
                "start_at": None,
                "end_at": None,
            },
            "source_snippets": [
                "Please take a moment to view the Frankland Family Math Night 2026 Slideshow and the Post Family Night Activity Links that you can explore and use at home."
            ],
        }
    )

    assert "recap rather than a new event" in result["message"]
    assert captured["agent_name"] == "more_info_reply"
    assert captured["use_session"] is False
    assert captured["inject_conversation_context"] is False
    assert "follow-up assistant" in captured["system_prompt"]
    assert '"summary_line": "Family Math Night 2026 recap/feedback"' in captured["user_payload"]


def test_openai_engine_summary_calls_do_not_inject_full_conversation_context(monkeypatch):
    captured = []

    def fake_run_agent(self, **kwargs):
        captured.append(kwargs)
        if kwargs["output_type"] is SummaryCandidateExtractionOutput:
            return SummaryCandidateExtractionOutput(
                title="Frankland Update",
                candidates=[],
                notes=[],
                missing_requested_topics=[],
            )
        return SummaryCompressionOutput(
            title="Frankland Update",
            important_info=[],
            other_dates=[],
            other_topics=[],
            missing_requested_topics=[],
            notes=[],
        )

    monkeypatch.setattr(OpenAIDecisionEngine, "_run_agent", fake_run_agent)
    engine = OpenAIDecisionEngine(api_key="test-key")

    engine.extract_summary_candidates({"title_hint": "Frankland Update", "candidates": []})
    engine.compress_summary({"title_hint": "Frankland Update", "candidates": []})

    assert [item["inject_conversation_context"] for item in captured] == [False, False]


def test_openai_engine_run_agent_enforces_wall_clock_timeout(monkeypatch):
    def fake_run_sync(agent, input, context, run_config, session):
        time.sleep(0.05)
        raise AssertionError("runner should have timed out before returning")

    monkeypatch.setattr("app.services.llm.Runner.run_sync", fake_run_sync)
    engine = OpenAIDecisionEngine(api_key="test-key", timeout_sec=1)
    monkeypatch.setattr(engine, "_runner_wall_clock_timeout_seconds", lambda: 0.01)

    try:
        engine.parse_command("add this to the calendar")
    except TimeoutError as exc:
        assert "wall-clock timeout" in str(exc)
    else:
        raise AssertionError("expected TimeoutError")


def test_openai_engine_run_agent_retries_transient_connection_errors(monkeypatch):
    calls = {"count": 0}

    class _FakeResult:
        def final_output_as(self, output_type):
            assert output_type is CommandParseOutput
            return CommandParseOutput(
                action="add",
                execution_strategy="deterministic",
                event_id=None,
                topic=None,
                preference_behavior=None,
                minutes_before=None,
                reminder_channel=None,
                async_requested=False,
                confidence=0.91,
            )

    def fake_run_runner_sync_with_timeout(**kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise APIConnectionError(request=httpx.Request("POST", "https://api.openai.com/v1/responses"))
        return _FakeResult()

    monkeypatch.setattr(
        OpenAIDecisionEngine,
        "_run_runner_sync_with_timeout",
        staticmethod(fake_run_runner_sync_with_timeout),
    )
    monkeypatch.setattr("app.services.llm.time.sleep", lambda _seconds: None)
    engine = OpenAIDecisionEngine(api_key="test-key")

    result = engine.parse_command("add this to the calendar")

    assert result["action"] == "add"
    assert calls["count"] == 2
