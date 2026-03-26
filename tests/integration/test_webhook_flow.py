import httpx

from sqlalchemy import select

from app.models import (
    AgentSessionItem,
    Child,
    DecisionAudit,
    Event,
    FollowupContext,
    InformationalItem,
    NotificationDelivery,
    SourceMessage,
    TeacherContact,
    User,
    WebhookReceipt,
)
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


def test_inbound_second_verified_admin_sender_rejected(client, db_session):
    db_session.add(User(household_id=1, email="christine.jinae@gmail.com", is_admin=True, verified=True))
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-second-admin"
    payload["provider_message_id"] = "msg-second-admin"
    payload["sender"] = "christine.jinae@gmail.com"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "rejected_unverified_sender"


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


def test_inbound_persists_source_before_thread_document_upload(client, session_factory, monkeypatch):
    import app.main as main_module
    from app.services.content_analysis import DownloadedAttachment, LinkResolutionReport

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-doc-commit"
    payload["provider_message_id"] = "msg-doc-commit"
    payload["subject"] = "Frankland attachment update"
    payload["body_text"] = "Please review the attached newsletter."

    observed = {"checked": False}

    def fake_resolve_and_download_links(_links):
        return LinkResolutionReport(
            attachments=[
                DownloadedAttachment(
                    filename="newsletter.pdf",
                    content_type="application/pdf",
                    content=b"%PDF-1.4",
                    source_url="https://example.com/newsletter.pdf",
                    status_reason="fixture",
                    extracted_text="Family Math Night recap and spring registration details.",
                )
            ],
            attempts=[],
        )

    def fake_persist_thread_documents(
        db,
        *,
        household_id,
        source_message_id,
        thread_key,
        attachments,
        **_kwargs,
    ):
        with session_factory() as verify_db:
            source = verify_db.scalar(
                select(SourceMessage).where(SourceMessage.provider_message_id == payload["provider_message_id"])
            )
            receipt = verify_db.scalar(
                select(WebhookReceipt).where(WebhookReceipt.provider_event_id == payload["provider_event_id"])
            )
            assert source is not None
            assert source.id == source_message_id
            assert source.thread_key == thread_key
            assert receipt is not None
            assert receipt.status == "received"
        observed["checked"] = True
        return []

    monkeypatch.setattr(main_module, "resolve_and_download_links", fake_resolve_and_download_links)
    monkeypatch.setattr(main_module, "persist_thread_documents", fake_persist_thread_documents)

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert observed["checked"] is True


def test_inbound_commits_user_turn_before_llm_extraction(client, session_factory, monkeypatch):
    import app.main as main_module
    from app.services.agent_threads import email_session_id

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-session-commit"
    payload["provider_message_id"] = "msg-session-commit"
    payload["subject"] = "Frankland user-turn commit"

    session_id = email_session_id(household_id=1, thread_key=payload["provider_message_id"])
    observed = {"checked": False}
    original_extract = main_module.engine_llm.extract_events

    def fake_extract_events(*args, **kwargs):
        with session_factory() as verify_db:
            items = list(
                verify_db.scalars(
                    select(AgentSessionItem).where(AgentSessionItem.session_id == session_id)
                )
            )
            assert items
            assert any("Email subject" in str(item.payload or {}) for item in items)
        observed["checked"] = True
        return original_extract(*args, **kwargs)

    monkeypatch.setattr(main_module.engine_llm, "extract_events", fake_extract_events)

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert observed["checked"] is True


def test_inbound_commits_processed_state_before_recap_send(client, session_factory, monkeypatch):
    import app.main as main_module

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-recap-commit"
    payload["provider_message_id"] = "msg-recap-commit"
    payload["subject"] = "Frankland recap commit"

    observed = {"checked": False}

    def fake_send_channel_notification(
        db,
        provider,
        *,
        household_id,
        recipient_type,
        channel,
        target,
        template,
        subject,
        message,
        email_headers=None,
    ):
        del db, provider, household_id, recipient_type, channel, target, template, subject, message, email_headers
        with session_factory() as verify_db:
            receipt = verify_db.scalar(
                select(WebhookReceipt).where(WebhookReceipt.provider_event_id == payload["provider_event_id"])
            )
            source = verify_db.scalar(
                select(SourceMessage).where(SourceMessage.provider_message_id == payload["provider_message_id"])
            )
            assert receipt is not None
            assert receipt.status == "processed"
            assert receipt.processed_at is not None
            assert source is not None
            assert verify_db.scalar(
                select(FollowupContext).where(FollowupContext.source_message_id == source.id)
            ) is not None
            assert verify_db.scalar(
                select(DecisionAudit).where(DecisionAudit.household_id == source.household_id)
            ) is not None
        observed["checked"] = True
        return {"sent": 1, "failed": 0}

    monkeypatch.setattr(main_module, "send_channel_notification", fake_send_channel_notification)

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert observed["checked"] is True


def test_relevant_non_actionable_event_is_preserved_for_followup(client, db_session):
    db_session.add(Child(household_id=1, name="Nolan", school_name="Frankland", grade="1", status="active"))
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-b"
    payload["provider_message_id"] = "msg-b"
    payload["subject"] = "Nolan Permission Slip"
    payload["body_text"] = "unclear details please decide soon"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200

    followup = db_session.scalar(select(FollowupContext).where(FollowupContext.household_id == 1))
    assert followup is not None
    assert any(item.get("action_capabilities", {}).get("can_explain") for item in list(followup.all_extracted_items or []))


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
    followup = db_session.scalar(select(FollowupContext).where(FollowupContext.source_message_id == message.id))
    assert event is not None or info is not None or followup is not None


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
                        model_reason="grade1",
                    ),
                    ExtractedEvent(
                        title="School spirit day",
                        start_at=None,
                        end_at=None,
                        category="school",
                        confidence=0.93,
                        target_scope="school_global",
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
    info = db_session.scalars(select(InformationalItem).where(InformationalItem.household_id == 1)).all()
    followup = db_session.scalar(select(FollowupContext).where(FollowupContext.household_id == 1))
    assert len(events) >= 1
    assert len(info) >= 1
    assert followup is not None


def test_forwarded_teacher_email_uses_original_sender_and_reference_date_for_auto_add(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    admin = db_session.scalar(select(User).where(User.email == "admin@example.com"))
    assert admin is not None
    admin.timezone = "America/Toronto"
    child = Child(
        household_id=1,
        name="Nolan",
        school_name="Frankland Community School Junior",
        grade="1",
        status="active",
    )
    child.teacher_contacts = [
        TeacherContact(teacher_name="Helen Poulos", teacher_email="helen.poulos@tdsb.on.ca", status="active")
    ]
    db_session.add(child)
    db_session.commit()

    captured = {}

    class _Engine:
        def extract_events(self, *_args, **kwargs):
            captured["reference_datetime_hint"] = kwargs.get("reference_datetime_hint")
            start = datetime.now(timezone.utc) + timedelta(days=7)
            return {
                "events": [
                    ExtractedEvent(
                        title="Swim Day",
                        start_at=start,
                        end_at=start + timedelta(hours=1),
                        category="school",
                        confidence=0.97,
                        target_scope="child_specific",
                        mentioned_names=[],
                        mentioned_schools=[],
                        target_grades=[],
                        preference_match=False,
                        model_reason="Class swim tomorrow from Ms. Poulos",
                    )
                ],
                "email_level_notes": None,
            }

        def extract_summary_candidates(self, summary_context):
            return {
                "title": summary_context["title_hint"],
                "candidates": list(summary_context["fallback_candidates"]),
                "notes": [],
                "missing_requested_topics": [],
            }

        def compress_summary(self, summary_context):
            return {
                "title": summary_context["title_hint"],
                "important_info": [item for item in list(summary_context["candidates"]) if item.get("has_date")],
                "other_dates": [],
                "other_topics": [],
                "missing_requested_topics": [],
                "notes": [],
            }

        def metadata(self):
            return {"provider": "mock", "model": "teacher-test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-teacher-linked-1"
    payload["provider_message_id"] = "msg-teacher-linked-1"
    payload["subject"] = "Fwd: Room 106 - Swim tomorrow!"
    payload["body_text"] = (
        "---------- Forwarded message ---------\n"
        "From: Helen Poulos <helen.poulos@tdsb.on.ca>\n"
        "Date: Sun, Mar 1, 2026 at 6:53 PM\n"
        "Subject: Room 106 - Swim tomorrow!\n"
        "To: Parent <parent@example.com>\n\n"
        "Dear Families,\nTomorrow is swim day.\n"
    )

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"
    assert captured["reference_datetime_hint"] == "2026-03-01T23:53:00+00:00"

    event = db_session.scalar(select(Event).where(Event.household_id == 1, Event.title == "Swim Day"))
    assert event is not None


def test_fyi_only_teacher_email_uses_informational_footer(client, db_session, monkeypatch):
    import app.main as main_module

    child = Child(
        household_id=1,
        name="Nolan",
        school_name="Frankland Community School Junior",
        grade="1",
        status="active",
    )
    child.teacher_contacts = [
        TeacherContact(teacher_name="Helen Poulos", teacher_email="helen.poulos@tdsb.on.ca", status="active")
    ]
    db_session.add(child)
    db_session.commit()

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {"events": [], "email_level_notes": None}

        def extract_summary_candidates(self, _summary_context):
            raise RuntimeError("force deterministic summary fallback")

        def compress_summary(self, _summary_context):
            raise RuntimeError("force deterministic summary fallback")

        def metadata(self):
            return {"provider": "mock", "model": "teacher-info-test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-teacher-fyi-1"
    payload["provider_message_id"] = "msg-teacher-fyi-1"
    payload["subject"] = "Fwd: Room 106 Update"
    payload["body_text"] = (
        "---------- Forwarded message ---------\n"
        "From: Helen Poulos <helen.poulos@tdsb.on.ca>\n"
        "Date: Fri, Jan 30, 2026 at 2:43 PM\n"
        "Subject: Room 106 Update\n"
        "To: Parent <parent@example.com>\n\n"
        "Dear Families,\nSafe arrival still applies if your child is absent.\n"
    )

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    delivery = db_session.scalar(
        select(NotificationDelivery)
        .where(NotificationDelivery.household_id == 1, NotificationDelivery.template == "email_analysis_recap")
        .order_by(NotificationDelivery.id.desc())
    )
    assert delivery is not None
    assert "This looks informational only" in delivery.message
    assert "Let me know if you want me to add any of these to the calendar" not in delivery.message


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
                        model_reason="needs confirmation",
                    ),
                    ExtractedEvent(
                        title="Tamil Heritage Month",
                        start_at=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                        end_at=datetime(2026, 1, 31, 23, 59, tzinfo=timezone.utc),
                        category="school",
                        confidence=0.9,
                        target_scope="school_global",
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
                "important_info": [
                    {
                        "text": "Jan 5: First day back from winter break",
                        "source_refs": ["event:first_day"],
                        "applies_to": ["Nolan"],
                        "date_sort_key": "2026-01-05T08:30:00+00:00",
                    },
                    {
                        "text": "Pizza Lunch order window",
                        "source_refs": ["event:pizza"],
                        "applies_to": ["Nolan"],
                        "date_sort_key": None,
                    },
                ],
                "other_dates": [],
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
    assert "\n\nImportant Info\n" in delivery.message
    assert "First day back from winter break" in delivery.message
    assert "- Pizza Lunch order window" in delivery.message
    assert "\n\nOther Logistics / Topics Mentioned\n" in delivery.message
    assert "- Tamil Heritage Month mentioned" in delivery.message
    assert "Let me know if you want me to add any of these to the calendar" in delivery.message
    assert "Email analyzed:" not in delivery.message
    assert "Needs your input:" not in delivery.message
    assert audit is not None
    assert audit.model_output["summary"]["final_summary"]["title"] == "Frankland Update (Jan 5)"
    assert "First day back from winter break" in audit.model_output["summary"]["final_summary"]["important_info"][0]["text"]


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
                        model_reason="holiday",
                    ),
                    ExtractedEvent(
                        title="World Down Syndrome Day",
                        start_at=start + timedelta(days=7),
                        end_at=start + timedelta(days=7, hours=1),
                        category="school",
                        confidence=0.95,
                        target_scope="school_global",
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
            mentioned = [item for item in candidates if item.get("consolidated_priority") == "mentioned"]
            return {
                "title": "Frankland Update",
                "important_info": [item for item in candidates if item.get("consolidated_priority") == "important"][:8],
                "other_dates": [item for item in mentioned if item.get("has_date")][:6],
                "other_topics": [item for item in mentioned if not item.get("has_date")][:6],
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
    info = db_session.scalars(select(InformationalItem).where(InformationalItem.household_id == 1)).all()
    followup = db_session.scalar(select(FollowupContext).where(FollowupContext.household_id == 1))
    audit = db_session.scalar(select(DecisionAudit).where(DecisionAudit.request_id == response.json()["request_id"]))

    assert sorted(event.title for event in events) == ["Good Friday", "March Break"]
    assert followup is not None
    followup_titles = {str(item.get("title") or "") for item in list(followup.all_extracted_items or [])}
    assert "School Council meeting" in followup_titles
    assert "Grade 5 girls volleyball tournament" in followup_titles
    assert any("Spring Swap" in title for title in followup_titles)
    assert any(item.title == "World Down Syndrome Day" for item in info)
    outcomes = (audit.committed_actions or {}).get("event_outcomes") or []
    march_break = next(item for item in outcomes if item["title"] == "March Break")
    spring_swap = next(item for item in outcomes if "Spring Swap" in item["title"])
    assert march_break["auto_add_decision"]["allow"] is True
    assert spring_swap["auto_add_decision"]["allow"] is False
    assert next(item for item in outcomes if item["title"] == "School Council meeting")["execution_disposition"] == "followup_available"


def test_forwarded_add_preface_uses_command_mode_and_preface_only(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    seen_parse_inputs: list[dict] = []
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
                        model_reason="clear forwarded event",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_forwarded_preface_intent(self, *, user_preface, forwarded_subject="", forwarded_sender="", forwarded_date=""):
            seen_parse_inputs.append(
                {
                    "user_preface": user_preface,
                    "forwarded_subject": forwarded_subject,
                    "forwarded_sender": forwarded_sender,
                    "forwarded_date": forwarded_date,
                }
            )
            return {
                "mode": "command",
                "action": "add",
                "execution_strategy": "deterministic",
                "topic": None,
                "preference_behavior": None,
                "event_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
                "reason": "forwarded_preface_add",
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

    assert seen_parse_inputs == [
        {
            "user_preface": "Add this to the calendar",
            "forwarded_subject": "Family Math Night Tomorrow!",
            "forwarded_sender": "Frankland CS <donotreply@tdsb.on.ca>",
            "forwarded_date": "Tue, Mar 10, 2026 at 8:21 AM",
        }
    ]
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

        def parse_forwarded_preface_intent(self, **_kwargs):
            return {
                "mode": "clarification",
                "action": "none",
                "execution_strategy": "none",
                "topic": None,
                "preference_behavior": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.92,
                "reason": "vague_help_request",
            }

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
    assert event is None


def test_forwarded_preface_parse_error_falls_back_to_direct_preface_command(client, db_session, monkeypatch):
    import app.main as main_module

    class _Engine:
        def parse_command(self, body_text):
            assert "tell me more about African Heritage Month trait" in body_text
            return {
                "action": "more_info",
                "execution_strategy": "semantic",
                "event_id": None,
                "topic": "African Heritage Month trait — Fairness",
                "preference_behavior": None,
                "minutes_before": 0,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.98,
            }

        def parse_forwarded_preface_intent(self, **_kwargs):
            raise RuntimeError("bad request")

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-parse-fallback-1"
    payload["provider_message_id"] = "msg-forwarded-parse-fallback-1"
    payload["subject"] = "Re: LovelyChaos: Fwd: Frankland Newsletter - January 28, 2025"
    payload["body_text"] = (
        "tell me more about African Heritage Month trait — Fairness\n\n"
        "On Fri, Mar 20, 2026 at 1:55 PM <schedule@lovelychaos.ca> wrote:\n\n"
        "> Frankland CS Update (Jan 28)\n"
        "> - Feb 12: African Heritage Month trait — Fairness\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"

    delivery = db_session.scalar(
        select(NotificationDelivery)
        .where(NotificationDelivery.household_id == 1, NotificationDelivery.template == "more_info")
        .order_by(NotificationDelivery.id.desc())
    )
    assert delivery is not None
    assert "African Heritage Month trait" in delivery.message

    receipt = db_session.scalar(
        select(WebhookReceipt).where(WebhookReceipt.provider_event_id == "evt-forwarded-parse-fallback-1")
    )
    assert receipt is not None
    assert receipt.status == "processed"


def test_forwarded_preface_parse_error_sends_clarification_reply(client, db_session, monkeypatch):
    import app.main as main_module

    class _Engine:
        def parse_command(self, _body_text):
            return {
                "action": "none",
                "execution_strategy": "none",
                "event_id": None,
                "topic": None,
                "preference_behavior": None,
                "minutes_before": 0,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.0,
            }

        def parse_forwarded_preface_intent(self, **_kwargs):
            raise RuntimeError("bad request")

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-parse-clarify-1"
    payload["provider_message_id"] = "msg-forwarded-parse-clarify-1"
    payload["subject"] = "Fwd: School update"
    payload["body_text"] = (
        "can you handle this?\n\n"
        "---------- Forwarded message ---------\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:21 AM\n"
        "Subject: School update\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Some forwarded content.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"

    delivery = db_session.scalar(
        select(NotificationDelivery)
        .where(NotificationDelivery.household_id == 1, NotificationDelivery.template == "command_clarification")
        .order_by(NotificationDelivery.id.desc())
    )
    assert delivery is not None
    assert "couldn't interpret the request" in delivery.message

    receipt = db_session.scalar(
        select(WebhookReceipt).where(WebhookReceipt.provider_event_id == "evt-forwarded-parse-clarify-1")
    )
    assert receipt is not None
    assert receipt.status == "processed"


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
                        model_reason="pizza",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_forwarded_preface_intent(self, **_kwargs):
            return {
                "mode": "ingestion",
                "action": "none",
                "execution_strategy": "none",
                "topic": None,
                "preference_behavior": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.98,
                "reason": "informational_preface",
            }

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
                        model_reason="event 1",
                    ),
                    ExtractedEvent(
                        title="STEM Showcase",
                        start_at=start + timedelta(days=1),
                        end_at=start + timedelta(days=1, hours=1),
                        category="school",
                        confidence=0.96,
                        target_scope="school_specific",
                        model_reason="event 2",
                    ),
                ],
                "email_level_notes": None,
            }

        def parse_forwarded_preface_intent(self, **_kwargs):
            return {
                "mode": "command",
                "action": "add",
                "execution_strategy": "deterministic",
                "topic": None,
                "preference_behavior": None,
                "event_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
                "reason": "forwarded_preface_add",
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


def test_forwarded_add_with_past_event_returns_noop_past_event(client, db_session, monkeypatch):
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
                        model_reason="past event",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_forwarded_preface_intent(self, **_kwargs):
            return {
                "mode": "command",
                "action": "add",
                "execution_strategy": "deterministic",
                "topic": None,
                "preference_behavior": None,
                "event_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
                "reason": "forwarded_preface_add",
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
    assert response.json()["status"] == "command_noop_past_event"
    assert "already passed" in response.json()["message"].lower()

    event = db_session.scalar(select(Event).where(Event.household_id == 1))
    assert event is None


def test_forwarded_add_with_topic_scoped_dates_returns_clean_past_events(client, db_session, monkeypatch):
    import app.main as main_module

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {
                "events": [
                    ExtractedEvent(
                        title="Swim dates",
                        start_at=None,
                        end_at=None,
                        category="school",
                        confidence=0.72,
                        target_scope="school_specific",
                        model_reason="swim",
                    ),
                    ExtractedEvent(
                        title="Swim date",
                        start_at=None,
                        end_at=None,
                        category="school",
                        confidence=0.7,
                        target_scope="school_specific",
                        model_reason="swim",
                    ),
                ],
                "email_level_notes": None,
            }

        def parse_forwarded_preface_intent(self, **_kwargs):
            return {
                "mode": "command",
                "action": "add",
                "execution_strategy": "deterministic",
                "topic": "swim dates",
                "preference_behavior": None,
                "event_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
                "reason": "forwarded_preface_add",
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-swim-past-1"
    payload["provider_message_id"] = "msg-forwarded-swim-past-1"
    payload["subject"] = "Fwd: Room 106 - Welcome Back!"
    payload["body_text"] = (
        "Add these swim dates to the calendar\n\n"
        "---------- Forwarded message ---------\n"
        "From: Poulos, Helen <helen.poulos@tdsb.on.ca>\n"
        "Date: Sun, Jan 4, 2026 at 4:16 PM\n"
        "Subject: Room 106 - Welcome Back!\n"
        "To: Helen Poulos <helen.poulos@tdsb.on.ca>\n\n"
        "Dear Families,\n\n"
        "Please note that our swim dates for this month are Thursday, January 8th, and Tuesday, January 20th.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_noop_past_event"
    assert "- Jan 8: Swim" in response.json()["message"]
    assert "- Jan 20: Swim" in response.json()["message"]
    assert "Jan 4" not in response.json()["message"]
    assert "Jan 8 to Jan 9" not in response.json()["message"]

    event = db_session.scalar(select(Event).where(Event.household_id == 1))
    assert event is None


def test_forwarded_add_with_plural_command_allows_multiple_resolved_events(client, db_session, monkeypatch):
    import app.main as main_module

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {"events": [], "email_level_notes": None}

        def parse_forwarded_preface_intent(self, **_kwargs):
            return {
                "mode": "command",
                "action": "add",
                "execution_strategy": "deterministic",
                "topic": None,
                "preference_behavior": None,
                "event_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
                "reason": "forwarded_preface_add",
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-add-multi-resolved-1"
    payload["provider_message_id"] = "msg-forwarded-add-multi-resolved-1"
    payload["subject"] = "Fwd: Pizza Days"
    payload["body_text"] = (
        "Add these to the calendar\n\n"
        "---------- Forwarded message ---------\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:21 AM\n"
        "Subject: Pizza Days\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Our pizza days are October 1, 2099 and October 15, 2099.\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"

    events = db_session.scalars(select(Event).where(Event.household_id == 1).order_by(Event.start_at.asc())).all()
    assert len(events) == 2
    assert [event.start_at.date().isoformat() for event in events] == ["2099-10-01", "2099-10-15"]


def test_empty_extraction_with_informational_sections_returns_summary(client, db_session, monkeypatch):
    import app.main as main_module

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {"events": [], "email_level_notes": None}

        def extract_summary_candidates(self, *_args, **_kwargs):
            return {
                "title": "Frankland Update",
                "candidates": [
                    {
                        "text": "Dec 3: Direct donation deadline",
                        "consolidated_priority": "important",
                        "matched_system_defaults": [],
                        "matched_user_priorities": [],
                        "source_refs": ["section:donation"],
                        "applies_to": [],
                        "date_sort_key": "2025-12-03T05:00:00+00:00",
                        "has_date": True,
                        "reason": "deadline",
                    },
                    {
                        "text": "Fundraising or donation updates",
                        "consolidated_priority": "mentioned",
                        "matched_system_defaults": [],
                        "matched_user_priorities": [],
                        "source_refs": ["topic:donation"],
                        "applies_to": [],
                        "date_sort_key": None,
                        "has_date": False,
                        "reason": "donation update",
                    },
                ],
                "notes": [],
                "missing_requested_topics": [],
            }

        def compress_summary(self, summary_context):
            return {
                "title": "Frankland Update",
                "important_info": [
                    {
                        "text": "Dec 3: Direct donation deadline",
                        "source_refs": ["section:donation"],
                        "applies_to": [],
                        "date_sort_key": "2025-12-03T05:00:00+00:00",
                    }
                ],
                "other_dates": [],
                "other_topics": [
                    {
                        "text": "Fundraising or donation updates",
                        "source_refs": ["topic:donation"],
                        "applies_to": [],
                        "date_sort_key": None,
                    }
                ],
                "missing_requested_topics": [],
                "notes": [],
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-empty-summary-1"
    payload["provider_message_id"] = "msg-empty-summary-1"
    payload["subject"] = "Fwd: Direct Donation Reminder"
    payload["body_text"] = (
        "---------- Forwarded message ---------\n"
        "From: Frankland School Council <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Nov 25, 2025 at 1:47 PM\n"
        "Subject: Direct Donation Reminder\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Please consider making a donation by next Wednesday, Dec 3rd.\n"
    )

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    info = db_session.scalar(select(InformationalItem).where(InformationalItem.household_id == 1))
    assert info is not None
    assert "Frankland Update" in info.title


def test_empty_extraction_single_event_email_rescues_non_empty_summary(client, db_session, monkeypatch):
    import app.main as main_module

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            return {"events": [], "email_level_notes": None}

        def extract_summary_candidates(self, *_args, **_kwargs):
            raise RuntimeError("force deterministic rescue summary")

        def compress_summary(self, summary_context):
            raise RuntimeError("force deterministic rescue summary")

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-empty-family-math-1"
    payload["provider_message_id"] = "msg-empty-family-math-1"
    payload["subject"] = "Fwd: Family Math Night Tomorrow!"
    payload["body_text"] = (
        "---------- Forwarded message ---------\n"
        "From: Frankland CS <donotreply@tdsb.on.ca>\n"
        "Date: Tue, Mar 10, 2026 at 8:17 AM\n"
        "Subject: Family Math Night Tomorrow!\n"
        "To: <christine.jinae@gmail.com>\n\n"
        "Dear Grade 1, 2 & 3 Families,\n\n"
        "Please Join Us for FAMILY MATH NIGHT\n\n"
        "Please save the date: Wednesday, March 11th, 2026\n\n"
        "Doors will open at 5:20 pm, and we will begin promptly with a welcome and a brief presentation at 5:30 pm. "
        "The evening will end at 6:30 pm.\n"
    )

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    delivery = db_session.scalar(
        select(NotificationDelivery).where(NotificationDelivery.household_id == 1, NotificationDelivery.template == "email_analysis_recap")
    )
    assert delivery is not None
    assert "Family Math Night" in delivery.message
    assert "Mar 11" in delivery.message
    assert "5:30 PM to 6:30 PM" in delivery.message
    assert "couldn't extract a clean summary" not in delivery.message


def test_begin_forwarded_message_format_detected_for_command_preface(client, db_session, monkeypatch):
    import app.main as main_module
    from datetime import datetime, timedelta, timezone

    seen_parse_inputs: list[dict] = []
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
                        model_reason="clear event",
                    )
                ],
                "email_level_notes": None,
            }

        def parse_forwarded_preface_intent(self, *, user_preface, forwarded_subject="", forwarded_sender="", forwarded_date=""):
            seen_parse_inputs.append(
                {
                    "user_preface": user_preface,
                    "forwarded_subject": forwarded_subject,
                    "forwarded_sender": forwarded_sender,
                    "forwarded_date": forwarded_date,
                }
            )
            return {
                "mode": "command",
                "action": "add",
                "execution_strategy": "deterministic",
                "topic": None,
                "preference_behavior": None,
                "event_id": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
                "reason": "forwarded_preface_add",
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
    assert seen_parse_inputs == [
        {
            "user_preface": "Please add this",
            "forwarded_subject": "Open House",
            "forwarded_sender": "Frankland CS <donotreply@tdsb.on.ca>",
            "forwarded_date": "Tue, Mar 10, 2026 at 8:21 AM",
        }
    ]


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
    assert audit.policy_outcome["counts"]["event_created"] == 1
    assert audit.model_output["analysis"]["chunk_failures"][0]["detail"] == "ReadTimeout"


def test_all_chunk_failures_fall_back_to_summary_recap(client, db_session, monkeypatch):
    import app.main as main_module

    class _Engine:
        def extract_events(self, *_args, **_kwargs):
            raise httpx.ReadTimeout("timed out")

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-chunk-fallback-1"
    payload["provider_message_id"] = "msg-chunk-fallback-1"
    payload["subject"] = "Frankland Update"
    payload["body_text"] = (
        "Important Dates\n"
        "Mar 16-20: March Break\n\n"
        "Other Logistics / Topics Mentioned\n"
        "Mar 31: School Council meeting\n"
        "Mandatory forms or payments\n"
    )
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    delivery = db_session.scalar(
        select(NotificationDelivery).where(NotificationDelivery.template == "email_analysis_recap")
    )
    audit = db_session.scalar(select(DecisionAudit).where(DecisionAudit.household_id == 1).order_by(DecisionAudit.id.desc()))
    info = db_session.scalar(select(InformationalItem).where(InformationalItem.household_id == 1))
    assert delivery is not None
    assert info is not None
    assert "School Council meeting" in delivery.message
    assert "This looks informational only" in delivery.message
    assert audit is not None
    assert audit.policy_outcome["status"] == "processed"
    assert "llm_extraction_error" in audit.validator_result["issues"]
    assert "empty_extraction_informational_fallback" in audit.validator_result["issues"]
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
