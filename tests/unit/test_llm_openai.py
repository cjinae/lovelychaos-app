from datetime import timezone

from app.services.llm import (
    COMMAND_SYSTEM_PROMPT,
    EXTRACTION_SYSTEM_PROMPT,
    OpenAIDecisionEngine,
)


class _MockResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _MockClient:
    def __init__(self, timeout: int):
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url, headers=None, json=None):  # noqa: A002
        assert url.endswith("/chat/completions")
        if "event extraction engine" in json["messages"][0]["content"].lower():
            return _MockResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": (
                                    '{"events":[{"title":"Closure","start_at":"2099-10-01T08:30:00Z",'
                                    '"end_at":"2099-10-01T09:30:00Z","category":"school_closure","confidence":0.94,'
                                    '"target_scope":"school_global","mentioned_names":["Nolan"],'
                                    '"mentioned_schools":["Frankland"],"target_grades":["1"],'
                                    '"preference_match":true,"model_batch":"A","model_reason":"closure_urgent"}],'
                                    '"email_level_notes":null}'
                                )
                            }
                        }
                    ]
                }
            )
        return _MockResponse(
            {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"action":"more_info","pending_id":null,"topic":"Pizza Lunch","minutes_before":30,'
                                '"reminder_channel":"sms","async_requested":false,"confidence":0.96}'
                            )
                        }
                    }
                ]
            }
        )


def test_openai_engine_extract_and_command(monkeypatch):
    import app.services.llm as llm_module

    monkeypatch.setattr(llm_module.httpx, "Client", _MockClient)
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
    assert command["pending_id"] is None
    assert command["topic"] == "Pizza Lunch"
    assert command["minutes_before"] == 30


def test_openai_engine_defaults_end_at_when_missing(monkeypatch):
    import app.services.llm as llm_module

    class _MissingEndClient:
        def __init__(self, timeout: int):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, headers=None, json=None):  # noqa: A002
            return _MockResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": (
                                    '{"events":[{"title":"Pizza Day","start_at":"2099-10-01T08:30:00Z",'
                                    '"end_at":null,"category":"school","confidence":0.94,'
                                    '"target_scope":"child_specific","mentioned_names":["Nolan"],'
                                    '"mentioned_schools":["Frankland"],"target_grades":[],"preference_match":true,'
                                    '"model_batch":"A","model_reason":"explicit"}],"email_level_notes":null}'
                                )
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr(llm_module.httpx, "Client", _MissingEndClient)
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


def test_prompt_constants_are_explicit():
    assert "Return ONLY JSON" in EXTRACTION_SYSTEM_PROMPT
    assert "Extract every distinct event mention" in EXTRACTION_SYSTEM_PROMPT
    assert "Do not use household preferences as a filter" in EXTRACTION_SYSTEM_PROMPT
    assert "infer the same year" in EXTRACTION_SYSTEM_PROMPT
    assert "forwarded original-email metadata" in EXTRACTION_SYSTEM_PROMPT
    assert "<output_contract>" in EXTRACTION_SYSTEM_PROMPT
    assert "<verification_loop>" in EXTRACTION_SYSTEM_PROMPT
    assert "Allowed actions only" in COMMAND_SYSTEM_PROMPT
    assert "<decision_rules>" in COMMAND_SYSTEM_PROMPT
    assert "more_info" in COMMAND_SYSTEM_PROMPT


def test_openai_engine_does_not_fallback_to_mock_when_model_returns_no_events(monkeypatch):
    import app.services.llm as llm_module

    class _EmptyEventsClient:
        def __init__(self, timeout: int):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, headers=None, json=None):  # noqa: A002
            return _MockResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{"events":[],"email_level_notes":null}'
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr(llm_module.httpx, "Client", _EmptyEventsClient)
    engine = OpenAIDecisionEngine(api_key="test-key")

    result = engine.extract_events(
        body_text="No event content here.",
        subject="Newsletter",
        household_preferences="Pizza Day",
        timezone_hint="UTC",
    )

    assert result["events"] == []
    assert result["email_level_notes"] == "empty_model_events"


def test_gpt5_models_omit_explicit_temperature(monkeypatch):
    import app.services.llm as llm_module

    captured = {}

    class _CaptureClient:
        def __init__(self, timeout: int):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, headers=None, json=None):  # noqa: A002
            captured["payload"] = json
            return _MockResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{"events":[],"email_level_notes":null}'
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr(llm_module.httpx, "Client", _CaptureClient)
    engine = OpenAIDecisionEngine(api_key="test-key", model="gpt-5-mini-2025-08-07")
    engine.extract_events(
        body_text="UPCOMING DATES\nOctober 10 - PA Day",
        subject="Newsletter",
        household_preferences="PA Day",
        timezone_hint="UTC",
    )

    assert "temperature" not in captured["payload"]
