import httpx

from sqlalchemy import select

from app.models import Child, DecisionAudit, Event, InformationalItem, NotificationDelivery, PendingEvent, SourceMessage, User, WebhookReceipt
from app.services.llm import ExtractedEvent
from tests.fixtures import PAYLOAD_CLEAN, PAYLOAD_LOW_CONFIDENCE, PAYLOAD_UNKNOWN_SENDER


def test_inbound_happy_path_creates_mutation(client, db_session):
    response = client.post("/webhooks/email/inbound", json=PAYLOAD_CLEAN, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ingestion_accepted"
    assert body["processing_state"] == "completed"


def test_inbound_unverified_sender_fail_closed(client, db_session):
    response = client.post("/webhooks/email/inbound", json=PAYLOAD_UNKNOWN_SENDER, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "rejected_unverified_sender"


def test_inbound_ambiguous_sender_fail_closed(client, db_session):
    db_session.add(User(household_id=1, email="admin@example.com", is_admin=True, verified=True))
    db_session.commit()
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-ambiguous"
    payload["provider_message_id"] = "msg-ambiguous"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "rejected_ambiguous_sender"


def test_inbound_second_verified_admin_sender_accepted(client, db_session):
    db_session.add(User(household_id=1, email="christine.jinae@gmail.com", is_admin=True, verified=True))
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-second-admin"
    payload["provider_message_id"] = "msg-second-admin"
    payload["sender"] = "christine.jinae@gmail.com"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"


def test_inbound_validation_reject(client):
    response = client.post("/webhooks/email/inbound", json=PAYLOAD_LOW_CONFIDENCE, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"


def test_duplicate_webhook_is_deduped(client):
    headers = {"x-signature": "local-dev-secret"}
    first = client.post("/webhooks/email/inbound", json=PAYLOAD_CLEAN, headers=headers)
    second = client.post("/webhooks/email/inbound", json=PAYLOAD_CLEAN, headers=headers)
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["message"].startswith("Duplicate webhook")


def test_batch_b_pending_created(client, db_session):
    db_session.add(Child(household_id=1, name="Nolan", school_name="Frankland", grade="1", status="active"))
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-b"
    payload["provider_message_id"] = "msg-b"
    payload["subject"] = "Nolan Permission Slip"
    payload["body_text"] = "unclear details please decide soon"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200

    pending = db_session.scalars(select(PendingEvent).where(PendingEvent.household_id == 1)).all()
    assert len(pending) >= 1


def test_event_links_source_message(client, db_session):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-link"
    payload["provider_message_id"] = "msg-link"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"
    message = db_session.scalar(select(SourceMessage).where(SourceMessage.provider_message_id == "msg-link"))
    assert message is not None
    event = db_session.scalar(select(Event).where(Event.source_message_id == message.id))
    info = db_session.scalar(select(InformationalItem).where(InformationalItem.source_message_id == message.id))
    pending = db_session.scalars(select(PendingEvent).where(PendingEvent.household_id == 1)).all()
    assert event is not None or info is not None or pending


def test_multi_event_independent_routing(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    db_session.add(Child(household_id=1, name="Nolan", school_name="Frankland", grade="1", status="active"))
    db_session.commit()

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            start = datetime.now(timezone.utc) + timedelta(days=2)
            return {
                "events": [
                    ExtractedEvent(
                        title="PA Day Grade 1",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="grade_specific",
                        target_grades=["1"],
                        model_batch="A",
                        model_reason="grade1",
                    ),
                    ExtractedEvent(
                        title="School spirit day",
                        start_at=None,
                        end_at=None,
                        category="school",
                        confidence=0.93,
                        target_scope="school_global",
                        model_batch="C",
                        model_reason="global",
                    ),
                    ExtractedEvent(
                        title="Pizza Day for Nolan",
                        start_at=None,
                        end_at=None,
                        category="school",
                        confidence=0.95,
                        target_scope="child_specific",
                        mentioned_names=["Nolan"],
                        model_batch="B",
                        model_reason="missing time",
                    ),
                ],
                "email_level_notes": None,
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-multi-1"
    payload["provider_message_id"] = "msg-multi-1"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    events = db_session.scalars(select(Event).where(Event.household_id == 1)).all()
    pending = db_session.scalars(select(PendingEvent).where(PendingEvent.household_id == 1)).all()
    info = db_session.scalars(select(InformationalItem).where(InformationalItem.household_id == 1)).all()
    assert len(events) >= 1
    assert len(pending) >= 1
    assert len(info) >= 1


def test_inbound_sends_formatted_analysis_recap(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timezone

    db_session.add(Child(household_id=1, name="Nolan", school_name="Frankland", grade="1", status="active"))
    db_session.commit()
    calls = {"extract_summary": 0, "compress_summary": 0}

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {
                "events": [
                            ExtractedEvent(
                                title="First day back from winter break",
                                start_at=datetime(2026, 1, 5, 8, 30, tzinfo=timezone.utc),
                                end_at=datetime(2026, 1, 5, 9, 30, tzinfo=timezone.utc),
                                category="school",
                                confidence=0.98,
                                target_scope="child_specific",
                                mentioned_names=["Nolan"],
                                model_batch="A",
                            model_reason="clear date",
                        ),
                        ExtractedEvent(
                            title="Pizza Lunch",
                            start_at=None,
                            end_at=None,
                            category="school",
                            confidence=0.95,
                            target_scope="child_specific",
                            mentioned_names=["Nolan"],
                        model_batch="B",
                        model_reason="needs confirmation",
                    ),
                    ExtractedEvent(
                        title="Tamil Heritage Month",
                        start_at=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                        end_at=datetime(2026, 1, 31, 23, 59, tzinfo=timezone.utc),
                        category="school",
                        confidence=0.9,
                        target_scope="school_global",
                        model_batch="C",
                        model_reason="informational",
                    ),
                ],
                "email_level_notes": None,
            }

        def extract_summary_candidates(self, summary_context):
            calls["extract_summary"] += 1
            assert summary_context["household_context"]["grades"] == ["1"]
            return {
                "title": "Frankland Update (Jan 5)",
                "candidates": [
                    {
                        "text": "Jan 5: First day back from winter break",
                        "consolidated_priority": "important",
                        "matched_system_defaults": ["grade_relevant"],
                        "matched_user_priorities": [],
                        "source_refs": ["event:first_day"],
                        "applies_to": ["Nolan"],
                        "date_sort_key": "2026-01-05T08:30:00+00:00",
                        "has_date": True,
                        "reason": "schedule-impacting",
                    },
                    {
                        "text": "Pizza Lunch order window",
                        "consolidated_priority": "important",
                        "matched_system_defaults": [],
                        "matched_user_priorities": ["Pizza Lunch"],
                        "source_refs": ["event:pizza"],
                        "applies_to": ["Nolan"],
                        "date_sort_key": None,
                        "has_date": False,
                        "reason": "money-deadline",
                    },
                    {
                        "text": "Tamil Heritage Month mentioned",
                        "consolidated_priority": "mentioned",
                        "matched_system_defaults": [],
                        "matched_user_priorities": [],
                        "source_refs": ["topic:tamil_heritage_month"],
                        "applies_to": [],
                        "date_sort_key": None,
                        "has_date": False,
                        "reason": "awareness",
                    },
                ],
                "notes": [],
                "missing_requested_topics": [],
            }

        def compress_summary(self, summary_context):
            calls["compress_summary"] += 1
            return {
                "title": "Frankland Update (Jan 5)",
                "important_dates": [
                    {
                        "text": "Jan 5: First day back from winter break",
                        "source_refs": ["event:first_day"],
                        "applies_to": ["Nolan"],
                        "date_sort_key": "2026-01-05T08:30:00+00:00",
                    },
                ],
                "important_items": [
                    {
                        "text": "Pizza Lunch order window",
                        "source_refs": ["event:pizza"],
                        "applies_to": ["Nolan"],
                        "date_sort_key": None,
                    }
                ],
                "other_topics": [
                    {
                        "text": "Tamil Heritage Month mentioned",
                        "source_refs": ["topic:tamil_heritage_month"],
                        "applies_to": [],
                        "date_sort_key": None,
                    }
                ],
                "missing_requested_topics": [],
                "notes": [],
            }

        def metadata(self):
            return {
                "provider": "mock",
                "model": "test",
                "prompt_versions": {
                    "summary_extract": "test-summary-extract",
                    "summary_compress": "test-summary-compress",
                },
            }

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-recap-1"
    payload["provider_message_id"] = "msg-recap-1"
    payload["subject"] = "Replay 04: Welcome Back January 2026"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200

    delivery = db_session.scalar(
        select(NotificationDelivery).where(NotificationDelivery.template == "email_analysis_recap")
    )
    audit = db_session.scalar(select(DecisionAudit).where(DecisionAudit.request_id == response.json()["request_id"]))
    assert delivery is not None
    assert calls == {"extract_summary": 1, "compress_summary": 1}
    assert delivery.message.startswith("Frankland Update (Jan 5)")
    assert "\n\nImportant Dates\n" in delivery.message
    assert "- Jan 5: First day back from winter break" in delivery.message
    assert "\n\nImportant Items\n" in delivery.message
    assert "- Pizza Lunch order window" in delivery.message
    assert "\n\nOther Logistics / Topics Mentioned\n" in delivery.message
    assert "- Tamil Heritage Month mentioned" in delivery.message
    assert "Let me know if you want me to add any of these to the calendar" in delivery.message
    assert "Email analyzed:" not in delivery.message
    assert "Needs your input:" not in delivery.message
    assert "Batch A" not in delivery.message
    assert audit is not None
    assert audit.model_output["summary"]["final_summary"]["title"] == "Frankland Update (Jan 5)"
    assert audit.model_output["summary"]["final_summary"]["important_dates"][0]["text"] == "Jan 5: First day back from winter break"


def test_auto_add_gate_demotes_optional_and_duplicate_schoolwide_events(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    db_session.add(Child(household_id=1, name="Nolan", school_name="Frankland", grade="1", status="active"))
    db_session.add(Child(household_id=1, name="Jayden", school_name="Frankland", grade="JK", status="active"))
    db_session.commit()

    start = datetime.now(timezone.utc) + timedelta(days=14)

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {
                "events": [
                    ExtractedEvent(
                        title="March Break",
                        start_at=start,
                        end_at=start + timedelta(days=5),
                        category="school",
                        confidence=0.98,
                        target_scope="school_global",
                        preference_match=True,
                        model_batch="A",
                        model_reason="school break",
                    ),
                    ExtractedEvent(
                        title="Good Friday",
                        start_at=start + timedelta(days=20),
                        end_at=start + timedelta(days=20, hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="school_global",
                        preference_match=True,
                        model_batch="C",
                        model_reason="holiday",
                    ),
                    ExtractedEvent(
                        title="World Down Syndrome Day",
                        start_at=start + timedelta(days=7),
                        end_at=start + timedelta(days=7, hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="school_global",
                        model_batch="C",
                        model_reason="awareness day",
                    ),
                    ExtractedEvent(
                        title="School Council meeting",
                        start_at=start + timedelta(days=10),
                        end_at=start + timedelta(days=10, hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        model_batch="B",
                        model_reason="meeting",
                    ),
                    ExtractedEvent(
                        title="Frankland’s Spring Swap",
                        start_at=start + timedelta(days=12),
                        end_at=start + timedelta(days=12, hours=2),
                        category="school",
                        confidence=0.95,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        model_batch="A",
                        model_reason="school event",
                    ),
                    ExtractedEvent(
                        title="Frankland Spring Swap",
                        start_at=start + timedelta(days=12),
                        end_at=start + timedelta(days=12, hours=2),
                        category="fundraiser",
                        confidence=0.96,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        model_batch="A",
                        model_reason="school fundraiser",
                    ),
                    ExtractedEvent(
                        title="Grade 5 girls volleyball tournament",
                        start_at=start + timedelta(days=5),
                        end_at=start + timedelta(days=5, hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="grade_specific",
                        target_grades=["5"],
                        mentioned_schools=["Frankland"],
                        model_batch="A",
                        model_reason="grade 5 event",
                    ),
                ],
                "email_level_notes": None,
            }

        def extract_summary_candidates(self, summary_context):
            return {
                "title": "Frankland Update",
                "candidates": summary_context["fallback_candidates"],
                "notes": [],
                "missing_requested_topics": [],
            }

        def compress_summary(self, summary_context):
            candidates = summary_context["candidates"]
            return {
                "title": "Frankland Update",
                "important_dates": [
                    item
                    for item in candidates
                    if item.get("consolidated_priority") == "important" and item.get("has_date")
                ][:8],
                "important_items": [
                    item
                    for item in candidates
                    if item.get("consolidated_priority") == "important" and not item.get("has_date")
                ][:4],
                "other_topics": [item for item in candidates if item.get("consolidated_priority") == "mentioned"][:6],
                "missing_requested_topics": [],
                "notes": [],
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-auto-add-policy"
    payload["provider_message_id"] = "msg-auto-add-policy"
    payload["subject"] = "March 8 Newsletter"
    payload["body_text"] = "March 8 newsletter"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    events = db_session.scalars(select(Event).where(Event.household_id == 1)).all()
    pending = db_session.scalars(select(PendingEvent).where(PendingEvent.household_id == 1)).all()
    info = db_session.scalars(select(InformationalItem).where(InformationalItem.household_id == 1)).all()
    audit = db_session.scalar(select(DecisionAudit).where(DecisionAudit.request_id == response.json()["request_id"]))

    assert sorted(event.title for event in events) == ["Good Friday", "March Break"]
    assert len([item for item in pending if "Spring Swap" in item.title]) == 1
    assert any(item.title == "School Council meeting" for item in pending)
    assert any(item.title == "Grade 5 girls volleyball tournament" for item in pending)
    assert any(item.title == "World Down Syndrome Day" for item in info)
    outcomes = (audit.committed_actions or {}).get("event_outcomes") or []
    march_break = next(item for item in outcomes if item["title"] == "March Break")
    spring_swap = next(item for item in outcomes if "Spring Swap" in item["title"])
    assert march_break["auto_add_decision"]["allow"] is True
    assert spring_swap["auto_add_decision"]["allow"] is False


def test_forwarded_add_preface_uses_command_mode_and_preface_only(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    seen_parse_inputs: list[str] = []
    seen_extract_inputs: list[str] = []
    start = datetime.now(timezone.utc) + timedelta(days=3)

    class _Engine:
        def extract_events(self, body_text, *_args, **_kwargs):
            seen_extract_inputs.append(body_text)
            return {
                "events": [
                    ExtractedEvent(
                        title="Family Math Night",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.98,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        model_batch="A",
                        model_reason="clear forwarded event",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_command(self, body_text):
            seen_parse_inputs.append(body_text)
            return {
                "action": "add",
                "pending_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-add-1"
    payload["provider_message_id"] = "msg-forwarded-add-1"
    payload["subject"] = "Fwd: Family Math Night Tomorrow!"
    payload["body_text"] = (
        "Add this to the calendar\n\n"
        "--\n"
        "Christine Jinae Lee\n\n"
        "---------- Forwarded message ---------\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:21 AM\n"
        "Subject: Family Math Night Tomorrow!\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Family Math Night is on 2099-10-01 17:20.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"

    assert seen_parse_inputs == ["Add this to the calendar"]
    assert seen_extract_inputs
    assert "Forwarded message" not in seen_extract_inputs[0]
    assert "Add this to the calendar" not in seen_extract_inputs[0]

    event = db_session.scalar(select(Event).where(Event.household_id == 1, Event.title == "Family Math Night"))
    assert event is not None
    receipt = db_session.scalar(select(WebhookReceipt).where(WebhookReceipt.provider_event_id == "evt-forwarded-add-1"))
    assert receipt is not None
    assert receipt.status == "processed"
    assert receipt.processed_at is not None


def test_forwarded_ambiguous_preface_needs_clarification(client, db_session, monkeypatch):
    import app.main as main_module

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            raise AssertionError("extract_events should not be called for ambiguous forwarded prefacing")

        def parse_command(self, *_args, **_kwargs):
            raise AssertionError("parse_command should not be called for ambiguous forwarded prefacing")

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-ambiguous-1"
    payload["provider_message_id"] = "msg-forwarded-ambiguous-1"
    payload["subject"] = "Fwd: Family Math Night Tomorrow!"
    payload["body_text"] = (
        "Can you handle this?\n\n"
        "---------- Forwarded message ---------\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:21 AM\n"
        "Subject: Family Math Night Tomorrow!\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Family Math Night is on 2099-10-01 17:20.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"

    event = db_session.scalar(select(Event).where(Event.household_id == 1))
    pending = db_session.scalar(select(PendingEvent).where(PendingEvent.household_id == 1))
    assert event is None
    assert pending is None


def test_forwarded_fyi_preface_stays_ingestion(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            start = datetime.now(timezone.utc) + timedelta(days=2)
            return {
                "events": [
                    ExtractedEvent(
                        title="Pizza Day",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        preference_match=True,
                        model_batch="A",
                        model_reason="pizza",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_command(self, *_args, **_kwargs):
            raise AssertionError("parse_command should not be called for FYI forwarded email")

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-fyi-1"
    payload["provider_message_id"] = "msg-forwarded-fyi-1"
    payload["subject"] = "Fwd: Pizza Lunch"
    payload["body_text"] = (
        "FYI\n\n"
        "---------- Forwarded message ---------\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:21 AM\n"
        "Subject: Pizza Lunch\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Frankland Pizza Day is on 2099-10-01 08:30.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    event = db_session.scalar(select(Event).where(Event.household_id == 1))
    assert event is not None
    assert event.title == "Pizza Day"


def test_forwarded_add_with_multiple_actionable_events_needs_clarification(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    start = datetime.now(timezone.utc) + timedelta(days=3)

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {
                "events": [
                    ExtractedEvent(
                        title="Family Math Night",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.98,
                        target_scope="school_specific",
                        model_batch="A",
                        model_reason="event 1",
                    ),
                    ExtractedEvent(
                        title="STEM Showcase",
                        start_at=start + timedelta(days=1),
                        end_at=start + timedelta(days=1, hours=1),
                        category="school",
                        confidence=0.96,
                        target_scope="school_specific",
                        model_batch="A",
                        model_reason="event 2",
                    ),
                ],
                "email_level_notes": None,
            }

        def parse_command(self, *_args, **_kwargs):
            return {
                "action": "add",
                "pending_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-add-multi-1"
    payload["provider_message_id"] = "msg-forwarded-add-multi-1"
    payload["subject"] = "Fwd: School Events"
    payload["body_text"] = (
        "Please add this\n\n"
        "---------- Forwarded message ---------\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:21 AM\n"
        "Subject: School Events\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Family Math Night is on 2099-10-01 17:20.\n"
        "STEM Showcase is on 2099-10-02 18:00.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"
    assert "multiple possible events" in response.json()["message"].lower()

    event = db_session.scalar(select(Event).where(Event.household_id == 1))
    assert event is None


def test_forwarded_add_with_no_actionable_event_needs_clarification(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timezone

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {
                "events": [
                    ExtractedEvent(
                        title="Past Event",
                        start_at=datetime(2020, 1, 1, 8, 30, tzinfo=timezone.utc),
                        end_at=datetime(2020, 1, 1, 9, 30, tzinfo=timezone.utc),
                        category="school",
                        confidence=0.95,
                        target_scope="school_specific",
                        model_batch="A",
                        model_reason="past event",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_command(self, *_args, **_kwargs):
            return {
                "action": "add",
                "pending_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-add-none-1"
    payload["provider_message_id"] = "msg-forwarded-add-none-1"
    payload["subject"] = "Fwd: School Events"
    payload["body_text"] = (
        "Add this to the calendar\n\n"
        "---------- Forwarded message ---------\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:21 AM\n"
        "Subject: School Events\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Past event details.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"
    assert "clear future event" in response.json()["message"].lower()

    event = db_session.scalar(select(Event).where(Event.household_id == 1))
    assert event is None


def test_begin_forwarded_message_format_detected_for_command_preface(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    seen_parse_inputs: list[str] = []
    start = datetime.now(timezone.utc) + timedelta(days=4)

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {
                "events": [
                    ExtractedEvent(
                        title="Open House",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.98,
                        target_scope="school_specific",
                        model_batch="A",
                        model_reason="clear event",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_command(self, body_text):
            seen_parse_inputs.append(body_text)
            return {
                "action": "add",
                "pending_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-begin-forwarded-1"
    payload["provider_message_id"] = "msg-begin-forwarded-1"
    payload["subject"] = "Fwd: Open House"
    payload["body_text"] = (
        "Please add this\n\n"
        "Begin forwarded message:\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:21 AM\n"
        "Subject: Open House\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Open House is on 2099-10-04 18:00.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert seen_parse_inputs == ["Please add this"]


def test_forwarded_footer_does_not_trigger_command_mode(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            start = datetime.now(timezone.utc) + timedelta(days=2)
            return {
                "events": [
                    ExtractedEvent(
                        title="Pizza Day",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        preference_match=True,
                        model_batch="A",
                        model_reason="pizza",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_command(self, *_args, **_kwargs):
            raise AssertionError("parse_command should not be called for newsletter-style email")

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-footer-1"
    payload["provider_message_id"] = "msg-footer-1"
    payload["body_text"] = (
        "Frankland Pizza Day is on 2099-10-01 08:30.\n\n"
        "Toronto District School Board would like to continue connecting with you.\n"
        "follow this link and confirm: unsubscribe"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    event = db_session.scalar(select(Event).where(Event.household_id == 1))
    assert event is not None
    assert event.title == "Pizza Day"


def test_chunk_failure_still_processes_other_chunks(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    start = datetime(2099, 10, 2, 8, 30, tzinfo=timezone.utc)

    class _Engine:
        def extract_events(self, body_text, *_args, **_kwargs):
            if "Chunk 1" in body_text:
                raise httpx.ReadTimeout("timed out")
            return {
                "events": [
                    ExtractedEvent(
                        title="Swim",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="activity",
                        confidence=0.95,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        preference_match=True,
                        model_batch="A",
                        model_reason="swim",
                    )
                ],
                "email_level_notes": "chunk ok",
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-chunk-1"
    payload["provider_message_id"] = "msg-chunk-1"
    payload["subject"] = "Chunked school newsletter"
    payload["body_text"] = (
        "UPCOMING DATES\n"
        "Chunk 1\n"
        + ("intro " * 1500)
        + "\n\nSwim schedule for Nolan on 2099-10-02 08:30.\n"
        "Chunk 2\n"
        + ("details " * 1200)
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    audit = db_session.scalar(select(DecisionAudit).where(DecisionAudit.household_id == 1).order_by(DecisionAudit.id.desc()))
    assert audit is not None
    assert audit.policy_outcome["counts"]["create_event"] == 1
    assert audit.model_output["analysis"]["chunk_failures"][0]["detail"] == "ReadTimeout"


def test_upcoming_dates_section_is_attempted_first(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    seen_bodies: list[str] = []

    class _Engine:
        def extract_events(self, body_text, *_args, **_kwargs):
            seen_bodies.append(body_text)
            start = datetime.now(timezone.utc) + timedelta(days=5)
            return {
                "events": [
                    ExtractedEvent(
                        title="PA Day",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        preference_match=True,
                        model_batch="A",
                        model_reason="pa day",
                    )
                ],
                "email_level_notes": None,
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-priority-1"
    payload["provider_message_id"] = "msg-priority-1"
    payload["subject"] = "Newsletter"
    payload["body_text"] = (
        "Hello Frankland Families. We hope you had a lovely weekend.\n\n"
        "UPCOMING DATES\n"
        "PIZZA LUNCH - October 1, 15, 29\n"
        "October 10 - PA Day (no school for students)\n"
        "October 13 - Thanksgiving (Holiday -- school closed)\n\n"
        + ("Long narrative text. " * 800)
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"
    assert seen_bodies
    assert "UPCOMING DATES" in seen_bodies[0]

    audit = db_session.scalar(select(DecisionAudit).where(DecisionAudit.household_id == 1).order_by(DecisionAudit.id.desc()))
    assert audit is not None
    assert audit.model_output["analysis"]["section_summaries"][0]["label"] == "UPCOMING DATES"


def test_body_only_newsletter_uses_section_prioritization(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    class _Engine:
        def extract_events(self, body_text, *_args, **_kwargs):
            start = datetime.now(timezone.utc) + timedelta(days=7)
            title = "Open House" if "Open House" in body_text else "General"
            return {
                "events": [
                    ExtractedEvent(
                        title=title,
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="school_specific",
                        mentioned_schools=["Frankland"],
                        preference_match=True,
                        model_batch="A",
                        model_reason="body-only",
                    )
                ],
                "email_level_notes": None,
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-body-only-1"
    payload["provider_message_id"] = "msg-body-only-1"
    payload["subject"] = "Direct school email"
    payload["body_text"] = (
        "Good afternoon families.\n\n"
        "Open House is on 2099-10-07 at 18:00 in the gym.\n\n"
        "School Council Meeting is on 2099-10-08 at 18:00 in the library."
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    audit = db_session.scalar(select(DecisionAudit).where(DecisionAudit.household_id == 1).order_by(DecisionAudit.id.desc()))
    assert audit is not None
    assert audit.model_output["analysis"]["section_summaries"]
    assert audit.model_output["analysis"]["chunk_summaries"][0]["priority_score"] >= 50
