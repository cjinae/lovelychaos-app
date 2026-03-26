from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import select

from app.models import Event, FollowupContext, NotificationDelivery, PreferenceProfile, SmsConversationState, SourceMessage, ThreadDocument, User
from tests.fixtures import PAYLOAD_CLEAN, PAYLOAD_SMS_CONFIRM


def _seed_followup_context(
    db_session,
    *,
    response_channel: str,
    summary_items_shown: list[dict],
    actionable_items: list[dict],
    body_text: str = "Frankland update details.",
    thread_key: str = "msg-followup-source",
) -> FollowupContext:
    source = SourceMessage(
        provider="mock-email",
        provider_message_id=thread_key,
        source_channel="email",
        sender="admin@example.com",
        household_id=1,
        subject="Frankland Update",
        body_text=body_text,
        internet_message_id=thread_key,
        thread_key=thread_key,
    )
    db_session.add(source)
    db_session.flush()
    context = FollowupContext(
        household_id=1,
        source_message_id=source.id,
        origin_channel="email",
        response_channel=response_channel,
        thread_or_conversation_key=thread_key,
        summary_title="Frankland Update",
        summary_items_shown=summary_items_shown,
        all_extracted_items=actionable_items,
        section_snippets=[{"label": "newsletter", "text": body_text}],
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
    )
    db_session.add(context)
    db_session.commit()
    return context


def test_email_ingestion_replies_via_email_for_recap(client, db_session, monkeypatch):
    import app.services.followups as followups_module

    monkeypatch.setattr(
        followups_module,
        "settings",
        SimpleNamespace(local_test_response_channel_override=""),
    )
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-recap-sms"
    payload["provider_message_id"] = "msg-recap-sms"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    delivery = db_session.scalar(
        select(NotificationDelivery)
        .where(NotificationDelivery.household_id == 1, NotificationDelivery.template == "email_analysis_recap")
        .order_by(NotificationDelivery.id.desc())
    )
    assert delivery is not None
    assert delivery.channel == "email"


def test_email_ingestion_stays_on_email_without_phone(client, db_session, monkeypatch):
    import app.services.followups as followups_module

    monkeypatch.setattr(
        followups_module,
        "settings",
        SimpleNamespace(local_test_response_channel_override=""),
    )
    admin = db_session.scalar(select(User).where(User.household_id == 1, User.is_admin.is_(True)))
    assert admin is not None
    admin.phone = None
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-recap-email"
    payload["provider_message_id"] = "msg-recap-email"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    delivery = db_session.scalar(
        select(NotificationDelivery)
        .where(NotificationDelivery.household_id == 1, NotificationDelivery.template == "email_analysis_recap")
        .order_by(NotificationDelivery.id.desc())
    )
    assert delivery is not None
    assert delivery.channel == "email"


def test_local_override_forces_email_recap(client, db_session, monkeypatch):
    import app.services.followups as followups_module

    monkeypatch.setattr(
        followups_module,
        "settings",
        SimpleNamespace(local_test_response_channel_override="email"),
    )

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-recap-email-override"
    payload["provider_message_id"] = "msg-recap-email-override"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    delivery = db_session.scalar(
        select(NotificationDelivery)
        .where(NotificationDelivery.household_id == 1, NotificationDelivery.template == "email_analysis_recap")
        .order_by(NotificationDelivery.id.desc())
    )
    assert delivery is not None
    assert delivery.channel == "email"


def test_sms_set_preference_persists_behavior_rule(client, db_session):
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-pref-evt"
    payload["provider_message_id"] = "sms-pref-msg"
    payload["body_text"] = "Always add pizza days"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    profile = db_session.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == 1))
    assert profile is not None
    command_rules = list((profile.structured_json or {}).get("command_written_preferences") or [])
    assert any(item.get("key") == "pizza_days" and item.get("behavior") == "auto_add" for item in command_rules)


def test_sms_more_info_uses_saved_followup_context(client, db_session):
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {
                "text": "Space Pirates musical",
                "item_id": "space-pirates",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "space-pirates",
                "title": "Space Pirates musical",
                "kind": "topic",
                "reason": "school musical",
                "start_at": None,
                "end_at": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        body_text="Space Pirates musical rehearsal details are in the gym after lunch.",
    )

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-more-info-ctx"
    payload["provider_message_id"] = "sms-more-info-ctx"
    payload["body_text"] = "tell me more about Space Pirates musical"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "Space Pirates musical" in response.json()["message"]
    assert "rehearsal details" in response.json()["message"]


def test_sms_more_info_uses_stored_document_understanding_topic(client, db_session):
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {
                "text": "Family Math Night 2026 recap/feedback",
                "item_id": "doc-topic-family-math-night",
                "source_refs": ["document_understanding:informational_topics:1"],
                "applies_to": [],
                "date_sort_key": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "doc-topic-family-math-night",
                "title": "Family Math Night 2026 recap/feedback",
                "display_text": "Family Math Night 2026 recap/feedback",
                "text": "Family Math Night 2026 recap/feedback: The school shared recap resources and a feedback follow-up.",
                "kind": "document_topic",
                "reason": "The school shared recap resources and a feedback follow-up.",
                "assistant_detail": "The school shared recap resources and a feedback follow-up.",
                "timing_hint": None,
                "action_hint": None,
                "start_at": None,
                "end_at": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        body_text="Please take a moment to view the slideshow and the post-event activity links.",
    )
    context = db_session.scalar(select(FollowupContext).order_by(FollowupContext.id.desc()))
    assert context is not None
    context.section_snippets = list(context.section_snippets or []) + [
        {
            "label": "assistant_summary",
            "text": "This looks like a recap with resources to review later, not a new event notice.",
            "meta": "document_understanding",
        }
    ]
    db_session.commit()

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-more-info-doc-understanding"
    payload["provider_message_id"] = "sms-more-info-doc-understanding"
    payload["body_text"] = "tell me more about Family Math Night"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "recap" in response.json()["message"].lower()
    assert "feedback" in response.json()["message"].lower()
    assert "date or time" in response.json()["message"].lower()


def test_sms_more_info_uses_assistant_reply_composer_when_available(client, db_session, monkeypatch):
    import app.main as main_module

    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {
                "text": "Family Math Night 2026 recap/feedback",
                "item_id": "family-math-night",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "family-math-night",
                "title": "Family Math Night",
                "kind": "topic",
                "reason": "Newsletter references a recap/slideshow and feedback form for Family Math Night; no date or time is stated in the email body.",
                "start_at": None,
                "end_at": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        body_text=(
            "Please take a moment to view the Frankland Family Math Night 2026 Slideshow and the Post Family "
            "Night Activity Links that you can explore and use at home. If you could also kindly complete our "
            "Feedback Form, we would greatly appreciate your input."
        ),
    )

    captured = {}

    class _Engine:
        def parse_command(self, text):
            assert "family math night" in text.lower()
            return {
                "action": "more_info",
                "execution_strategy": "semantic",
                "event_id": None,
                "topic": "Family Math Night",
                "preference_behavior": None,
                "minutes_before": 0,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.98,
            }

        def compose_more_info_reply(self, more_info_context):
            captured["context"] = more_info_context
            return {
                "message": (
                    "Family Math Night looks like a recap rather than a new event. The update points to the slideshow, "
                    "at-home activity links, and a feedback form, but it does not include a new date or time."
                )
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-more-info-composer"
    payload["provider_message_id"] = "sms-more-info-composer"
    payload["body_text"] = "tell me more about Family Math Night"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "recap rather than a new event" in response.json()["message"]
    assert "slideshow" in response.json()["message"]
    assert captured["context"]["matched_item"]["title"] == "Family Math Night"
    assert captured["context"]["source_snippets"]


def test_sms_more_info_retrieves_on_demand_source_snippets_when_stored_context_is_weak(client, db_session, monkeypatch):
    import app.main as main_module

    context = _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {
                "text": "BRICK LABS INC. VIB Chess Club",
                "item_id": "brick-labs",
                "source_refs": ["document_understanding:informational_topics:1"],
                "applies_to": [],
                "date_sort_key": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "brick-labs",
                "title": "BRICK LABS INC. VIB Chess Club",
                "display_text": "BRICK LABS INC. VIB Chess Club",
                "kind": "document_topic",
                "reason": "Chess club mention in the newsletter.",
                "assistant_detail": "Chess club mention in the newsletter.",
                "start_at": None,
                "end_at": None,
                "source_refs": ["document_understanding:informational_topics:1"],
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        body_text="Please see the attached newsletter for club details.",
        thread_key="msg-brick-labs-followup",
    )
    source = db_session.get(SourceMessage, context.source_message_id)
    assert source is not None
    db_session.add(
        ThreadDocument(
            household_id=1,
            source_message_id=source.id,
            thread_key=source.thread_key,
            filename="newsletter.pdf",
            content_type="application/pdf",
            extracted_text=(
                "BRICK LABS INC. VIB Chess Club runs on Wednesdays after school from 3:30 PM to 4:30 PM in Room 204. "
                "Registration closes on April 3."
            ),
        )
    )
    context.section_snippets = list(context.section_snippets or []) + [
        {
            "label": "assistant_summary",
            "text": "This looks like a club announcement, but the stored follow-up context is thin.",
            "meta": "document_understanding",
        }
    ]
    db_session.commit()

    captured = {}

    class _Engine:
        def parse_command(self, text):
            return {
                "action": "more_info",
                "execution_strategy": "semantic",
                "event_id": None,
                "topic": "BRICK LABS INC. VIB Chess Club",
                "preference_behavior": None,
                "minutes_before": 0,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
            }

        def compose_more_info_reply(self, more_info_context):
            captured["context"] = more_info_context
            return {
                "message": (
                    "BRICK LABS VIB Chess Club meets on Wednesdays after school from 3:30 PM to 4:30 PM in Room 204, "
                    "and registration closes on April 3."
                )
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-more-info-source-retrieval"
    payload["provider_message_id"] = "sms-more-info-source-retrieval"
    payload["body_text"] = "tell me more about BRICK LABS INC. VIB Chess Club"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "Wednesdays after school" in response.json()["message"]
    assert captured["context"]["source_retrieval_used"] is True
    assert captured["context"]["source_retrieval_reason"] in {
        "matched_topic_only_has_document_understanding_context",
        "matched_topic_has_thin_stored_detail",
    }
    assert any("Room 204" in snippet for snippet in captured["context"]["source_snippets"])


def test_sms_more_info_skips_on_demand_source_retrieval_when_stored_context_is_strong(client, db_session, monkeypatch):
    import app.main as main_module

    context = _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {
                "text": "Space Pirates musical",
                "item_id": "space-pirates",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "space-pirates",
                "title": "Space Pirates musical",
                "kind": "topic",
                "reason": "school musical",
                "start_at": None,
                "end_at": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        body_text="Space Pirates musical rehearsal details are in the gym after lunch.",
        thread_key="msg-space-pirates-strong",
    )
    source = db_session.get(SourceMessage, context.source_message_id)
    assert source is not None
    db_session.add(
        ThreadDocument(
            household_id=1,
            source_message_id=source.id,
            thread_key=source.thread_key,
            filename="newsletter.pdf",
            content_type="application/pdf",
            extracted_text="Space Pirates musical cast photos will be in Room 204 before rehearsal.",
        )
    )
    db_session.commit()

    captured = {}

    class _Engine:
        def parse_command(self, text):
            return {
                "action": "more_info",
                "execution_strategy": "semantic",
                "event_id": None,
                "topic": "Space Pirates musical",
                "preference_behavior": None,
                "minutes_before": 0,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
            }

        def compose_more_info_reply(self, more_info_context):
            captured["context"] = more_info_context
            return {"message": "Space Pirates musical rehearsal details are in the gym after lunch."}

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-more-info-source-skip"
    payload["provider_message_id"] = "sms-more-info-source-skip"
    payload["body_text"] = "tell me more about Space Pirates musical"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert captured["context"]["source_retrieval_used"] is False
    assert captured["context"]["source_retrieval_reason"] is None
    assert not any("Room 204" in snippet for snippet in captured["context"]["source_snippets"])


def test_sms_direct_add_creates_calendar_event(client, db_session):
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-add-direct"
    payload["provider_message_id"] = "sms-add-direct"
    payload["body_text"] = "add Family Field Trip to the cal for May 9, 2026"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "Added to calendar" in response.json()["message"]
    event = db_session.scalar(select(Event).where(Event.household_id == 1, Event.title == "Family Field Trip"))
    assert event is not None


def test_email_reply_adds_date_only_followup_item_as_all_day_event(client, db_session):
    start_at = datetime(2099, 3, 19, 0, 0, tzinfo=timezone.utc)
    end_at = datetime(2099, 3, 19, 1, 0, tzinfo=timezone.utc)
    _seed_followup_context(
        db_session,
        response_channel="email",
        summary_items_shown=[
            {
                "text": "Mar 19: Extra Ed Science Club registration deadline",
                "item_id": "science-deadline",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": start_at.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "science-deadline",
                "title": "Extra Ed Science Club registration deadline",
                "aliases": ["science club registration deadline", "Extra Ed Science Club registration deadline"],
                "kind": "deadline",
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "date_sort_key": start_at.isoformat(),
                "all_day": True,
                "reason": "registration deadline",
                "action_capabilities": {"can_add": True, "can_explain": True},
            }
        ],
        body_text="Extra Ed Science Club registration deadline is March 19.",
        thread_key="msg-email-followup-add",
    )

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-email-followup-add"
    payload["provider_message_id"] = "msg-email-followup-add-reply"
    payload["subject"] = "Re: Frankland Update"
    payload["body_text"] = "Add Extra Ed Science Club registration deadline to the calendar"
    payload["thread_key"] = "msg-email-followup-add"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    event = db_session.scalar(
        select(Event).where(Event.household_id == 1, Event.title == "Extra Ed Science Club registration deadline")
    )
    assert event is not None
    assert event.all_day is True


def test_gmail_style_reply_boundary_keeps_photo_followup_clarification_scoped(client, db_session, monkeypatch):
    import app.main as main_module

    start_photo = datetime(2099, 10, 22, 0, 0, tzinfo=timezone.utc)
    end_photo = datetime(2099, 10, 22, 1, 0, tzinfo=timezone.utc)
    start_retake = datetime(2099, 11, 19, 0, 0, tzinfo=timezone.utc)
    end_retake = datetime(2099, 11, 19, 1, 0, tzinfo=timezone.utc)
    start_council = datetime(2099, 11, 4, 0, 0, tzinfo=timezone.utc)
    end_council = datetime(2099, 11, 4, 1, 0, tzinfo=timezone.utc)
    _seed_followup_context(
        db_session,
        response_channel="email",
        summary_items_shown=[
            {
                "text": "Oct 22: School Photo Day",
                "item_id": "photo-day",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": start_photo.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
            {
                "text": "Nov 19: School photo retake day",
                "item_id": "photo-retake",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": start_retake.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
            {
                "text": "Nov 4: Frankland School Council in-person meeting",
                "item_id": "school-council",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": start_council.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
        ],
        actionable_items=[
            {
                "item_id": "photo-day",
                "title": "School Photo Day",
                "aliases": ["School Photo Day"],
                "kind": "event",
                "start_at": start_photo.isoformat(),
                "end_at": end_photo.isoformat(),
                "date_sort_key": start_photo.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
            {
                "item_id": "photo-retake",
                "title": "Retake Day",
                "aliases": ["Retake Day"],
                "kind": "event",
                "start_at": start_retake.isoformat(),
                "end_at": end_retake.isoformat(),
                "date_sort_key": start_retake.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
            {
                "item_id": "school-council",
                "title": "Frankland School Council in-person meeting",
                "aliases": ["Frankland School Council in-person meeting"],
                "kind": "event",
                "start_at": start_council.isoformat(),
                "end_at": end_council.isoformat(),
                "date_sort_key": start_council.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
        ],
        body_text="School Photo Day is Oct 22. School photo retake day is Nov 19. School Council meets Nov 4.",
        thread_key="msg-gmail-photo-reply-context",
    )

    seen_prefaces: list[dict] = []

    class _Engine:
        def parse_forwarded_preface_intent(self, *, user_preface, forwarded_subject="", forwarded_sender="", forwarded_date=""):
            seen_prefaces.append(
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
                "confidence": 0.98,
                "reason": "reply_preface_add",
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-gmail-photo-reply"
    payload["provider_message_id"] = "msg-gmail-photo-reply"
    payload["subject"] = "Re: LovelyChaos: Frankland CS update"
    payload["thread_key"] = "msg-gmail-photo-reply-context"
    payload["body_text"] = (
        "Add Photo day to the calendar\n\n"
        "On Wed, Mar 18, 2026 at 12:49 PM <schedule@lovelychaos.ca> wrote:\n"
        "> Frankland CS update: key October dates\n"
        ">\n"
        "> - Oct 22: School Photo Day\n"
        "> - Nov 4: Next Frankland School Council in-person meeting in the Library\n"
        "> - Nov 19: School photo retake day\n"
    )

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"
    assert seen_prefaces == [
        {
            "user_preface": "Add Photo day to the calendar",
            "forwarded_subject": "",
            "forwarded_sender": "",
            "forwarded_date": "",
        }
    ]
    assert "School Photo Day" in response.json()["message"]
    assert "Retake Day" in response.json()["message"]
    assert "School Council" not in response.json()["message"]


def test_gmail_style_reply_prefers_direct_more_info_command_when_preface_is_clear(client, db_session, monkeypatch):
    import app.main as main_module

    _seed_followup_context(
        db_session,
        response_channel="email",
        summary_items_shown=[
            {
                "text": "Sep 28/29: Terry Fox Run, Walk, and Roll",
                "item_id": "terry-fox-run",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "terry-fox-run",
                "title": "Terry Fox Run",
                "aliases": ["Terry Fox Run", "Terry Fox Run, Walk, and Roll"],
                "kind": "topic",
                "start_at": None,
                "end_at": None,
                "reason": "school event topic",
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        body_text="Terry Fox Run, Walk, and Roll took place around 12:55 p.m. at Withrow Park.",
        thread_key="msg-gmail-terry-fox-context",
    )

    class _Engine:
        def parse_command(self, text):
            if "terry fox run" in text.lower():
                return {
                    "action": "more_info",
                    "execution_strategy": "semantic",
                    "event_id": None,
                    "topic": "terry fox run",
                    "preference_behavior": None,
                    "minutes_before": 60,
                    "reminder_channel": "sms",
                    "async_requested": False,
                    "confidence": 0.98,
                }
            return {
                "action": "none",
                "execution_strategy": "none",
                "event_id": None,
                "topic": None,
                "preference_behavior": None,
                "minutes_before": 60,
                "reminder_channel": "sms",
                "async_requested": False,
                "confidence": 0.0,
            }

        def parse_forwarded_preface_intent(self, **_kwargs):
            return {
                "mode": "clarification",
                "action": "none",
                "execution_strategy": "none",
                "event_id": None,
                "topic": None,
                "preference_behavior": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.46,
                "reason": "ambiguous_without_grounding",
            }

        def metadata(self):
            return {"provider": "mock", "model": "test", "prompt_versions": {}}

    monkeypatch.setattr(main_module, "engine_llm", _Engine())

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-gmail-terry-fox-more-info"
    payload["provider_message_id"] = "msg-gmail-terry-fox-more-info"
    payload["subject"] = "Re: LovelyChaos: Frankland CS update"
    payload["thread_key"] = "msg-gmail-terry-fox-context"
    payload["body_text"] = (
        "tell me more about terry fox run\n\n"
        "On Wed, Mar 18, 2026 at 1:11 PM <schedule@lovelychaos.ca> wrote:\n"
        "> Frankland CS update: key October dates\n"
        ">\n"
        "> - Sep 28/29: Terry Fox Run, Walk, and Roll took place.\n"
    )

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "Terry Fox Run" in response.json()["message"]
    assert "12:55 p.m." in response.json()["message"]


def test_sms_reply_adds_date_only_followup_item_as_all_day_event(client, db_session):
    start_at = datetime(2099, 3, 19, 0, 0, tzinfo=timezone.utc)
    end_at = datetime(2099, 3, 19, 1, 0, tzinfo=timezone.utc)
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {
                "text": "Mar 19: Extra Ed Science Club registration deadline",
                "item_id": "science-deadline-sms",
                "source_refs": [],
                "applies_to": [],
                "date_sort_key": start_at.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "science-deadline-sms",
                "title": "Extra Ed Science Club registration deadline",
                "aliases": ["science club registration deadline"],
                "kind": "deadline",
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "date_sort_key": start_at.isoformat(),
                "all_day": True,
                "reason": "registration deadline",
                "action_capabilities": {"can_add": True, "can_explain": True},
            }
        ],
        body_text="Extra Ed Science Club registration deadline is March 19.",
        thread_key="msg-sms-followup-add",
    )

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-followup-add-evt"
    payload["provider_message_id"] = "sms-followup-add-msg"
    payload["body_text"] = "Add Extra Ed Science Club registration deadline to the calendar"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    event = db_session.scalar(
        select(Event).where(Event.household_id == 1, Event.title == "Extra Ed Science Club registration deadline")
    )
    assert event is not None
    assert event.all_day is True


def test_sms_reply_to_ambiguous_followup_item_returns_clarification(client, db_session):
    start_at = datetime(2099, 3, 19, 0, 0, tzinfo=timezone.utc)
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {"text": "Mar 19: Extra Ed Science Club registration deadline", "item_id": "science-a"},
            {"text": "Mar 20: Extra Ed Science Camp registration deadline", "item_id": "science-b"},
        ],
        actionable_items=[
            {
                "item_id": "science-a",
                "title": "Extra Ed Science Club registration deadline",
                "aliases": ["science registration"],
                "kind": "deadline",
                "start_at": start_at.isoformat(),
                "end_at": (start_at + timedelta(days=1)).isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
            {
                "item_id": "science-b",
                "title": "Extra Ed Science Camp registration deadline",
                "aliases": ["science registration"],
                "kind": "deadline",
                "start_at": (start_at + timedelta(days=1)).isoformat(),
                "end_at": (start_at + timedelta(days=2)).isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
        ],
        thread_key="msg-sms-ambiguous-followup",
    )

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-followup-ambiguous-evt"
    payload["provider_message_id"] = "sms-followup-ambiguous-msg"
    payload["body_text"] = "Add the science registration deadline to the calendar"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"
    assert "multiple possible events" in response.json()["message"]
    assert "Science Club registration deadline" in response.json()["message"]
    state = db_session.scalar(
        select(SmsConversationState)
        .where(SmsConversationState.household_id == 1, SmsConversationState.status == "active")
        .order_by(SmsConversationState.id.desc())
    )
    assert state is not None
    assert state.requested_action == "add"
    assert len(state.candidate_items) == 2


def test_sms_date_reference_add_uses_recent_followup_context_when_parser_returns_none(client, db_session, monkeypatch):
    start_at = datetime(2099, 3, 19, 0, 0, tzinfo=timezone.utc)
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {
                "text": "Mar 19: Extra Ed Science Club registration deadline",
                "item_id": "science-date-ref",
                "date_sort_key": start_at.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            }
        ],
        actionable_items=[
            {
                "item_id": "science-date-ref",
                "title": "Extra Ed Science Club registration deadline",
                "aliases": ["science club registration deadline", "extra ed"],
                "kind": "deadline",
                "start_at": start_at.isoformat(),
                "end_at": (start_at + timedelta(days=1)).isoformat(),
                "date_sort_key": start_at.isoformat(),
                "all_day": True,
                "reason": "registration deadline",
                "action_capabilities": {"can_add": True, "can_explain": True},
            }
        ],
        body_text="Extra Ed Science Club registration deadline is March 19.",
        thread_key="msg-sms-date-reference",
    )

    import app.main as main_module

    original_parse = main_module.engine_llm.parse_command

    def fake_parse_command(body_text: str):
        if body_text == "Add mar 19 event to cal":
            return {
                "action": "none",
                "execution_strategy": "none",
                "event_id": None,
                "topic": None,
                "preference_behavior": None,
                "minutes_before": 60,
                "reminder_channel": "sms",
                "async_requested": False,
                "confidence": 0.2,
            }
        return original_parse(body_text)

    monkeypatch.setattr(main_module.engine_llm, "parse_command", fake_parse_command)

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-date-reference-evt"
    payload["provider_message_id"] = "sms-date-reference-msg"
    payload["body_text"] = "Add mar 19 event to cal"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    event = db_session.scalar(
        select(Event).where(Event.household_id == 1, Event.title == "Extra Ed Science Club registration deadline")
    )
    assert event is not None


def test_sms_clarification_reply_uses_conversation_state_instead_of_unsupported_command(client, db_session):
    start_at = datetime(2099, 3, 19, 0, 0, tzinfo=timezone.utc)
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[
            {"text": "Mar 19: Extra Ed Science Club registration deadline", "item_id": "science-deadline-choice"},
            {"text": "Mar 25: Extra Ed Spring Session Science Club begins", "item_id": "science-session-choice"},
        ],
        actionable_items=[
            {
                "item_id": "science-deadline-choice",
                "title": "Extra Ed Science Club registration deadline",
                "aliases": ["extra ed", "registration deadline", "science club"],
                "kind": "deadline",
                "start_at": start_at.isoformat(),
                "end_at": (start_at + timedelta(days=1)).isoformat(),
                "date_sort_key": start_at.isoformat(),
                "all_day": True,
                "reason": "registration deadline",
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
            {
                "item_id": "science-session-choice",
                "title": "Extra Ed Spring Session Science Club begins",
                "aliases": ["extra ed", "science club", "spring session"],
                "kind": "event",
                "start_at": (start_at + timedelta(days=6)).isoformat(),
                "end_at": (start_at + timedelta(days=6, hours=1)).isoformat(),
                "date_sort_key": (start_at + timedelta(days=6)).isoformat(),
                "reason": "spring session start",
                "action_capabilities": {"can_add": True, "can_explain": True},
            },
        ],
        body_text="Extra Ed Science Club registration deadline is March 19 and the spring session begins March 25.",
        thread_key="msg-sms-stateful-clarification",
    )

    first_payload = dict(PAYLOAD_SMS_CONFIRM)
    first_payload["provider_event_id"] = "sms-stateful-clarify-1"
    first_payload["provider_message_id"] = "sms-stateful-clarify-1"
    first_payload["body_text"] = "Add extra ed to cal"

    first_response = client.post("/webhooks/sms/inbound", json=first_payload, headers={"x-signature": "local-dev-secret"})

    assert first_response.status_code == 200
    assert first_response.json()["status"] == "command_needs_clarification"
    assert "1." in first_response.json()["message"]

    second_payload = dict(PAYLOAD_SMS_CONFIRM)
    second_payload["provider_event_id"] = "sms-stateful-clarify-2"
    second_payload["provider_message_id"] = "sms-stateful-clarify-2"
    second_payload["body_text"] = "Registration deadline"

    second_response = client.post("/webhooks/sms/inbound", json=second_payload, headers={"x-signature": "local-dev-secret"})

    assert second_response.status_code == 200
    assert second_response.json()["status"] == "command_completed"
    assert "Unsupported SMS command" not in second_response.json()["message"]
    event = db_session.scalar(
        select(Event).where(Event.household_id == 1, Event.title == "Extra Ed Science Club registration deadline")
    )
    assert event is not None
    active_state = db_session.scalar(
        select(SmsConversationState)
        .where(SmsConversationState.household_id == 1, SmsConversationState.status == "active")
        .order_by(SmsConversationState.id.desc())
    )
    assert active_state is None


def test_sms_reply_to_info_only_followup_item_returns_clarification(client, db_session):
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[{"text": "Mandatory forms or payments", "item_id": "forms-topic"}],
        actionable_items=[
            {
                "item_id": "forms-topic",
                "title": "Mandatory forms or payments",
                "kind": "topic",
                "reason": "newsletter mention",
                "start_at": None,
                "end_at": None,
                "action_capabilities": {"can_add": False, "can_explain": True},
            }
        ],
        thread_key="msg-sms-info-only-followup",
    )

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-followup-info-only-evt"
    payload["provider_message_id"] = "sms-followup-info-only-msg"
    payload["body_text"] = "Add Mandatory forms or payments to the calendar"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"
    assert "doesn't have enough scheduling detail" in response.json()["message"]


def test_sms_reply_to_past_followup_item_returns_noop(client, db_session):
    start_at = datetime(2020, 3, 19, 0, 0, tzinfo=timezone.utc)
    end_at = datetime(2020, 3, 20, 0, 0, tzinfo=timezone.utc)
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[{"text": "Mar 19: Extra Ed Science Club registration deadline", "item_id": "science-past"}],
        actionable_items=[
            {
                "item_id": "science-past",
                "title": "Extra Ed Science Club registration deadline",
                "kind": "deadline",
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "date_sort_key": start_at.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            }
        ],
        thread_key="msg-sms-past-followup",
    )

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-followup-past-evt"
    payload["provider_message_id"] = "sms-followup-past-msg"
    payload["body_text"] = "Add Extra Ed Science Club registration deadline to the calendar"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_noop_past_event"
    assert "already passed" in response.json()["message"]


def test_sms_reply_add_calendar_failure_returns_retry_message(client, db_session):
    start_at = datetime(2099, 3, 19, 0, 0, tzinfo=timezone.utc)
    end_at = datetime(2099, 3, 20, 0, 0, tzinfo=timezone.utc)
    _seed_followup_context(
        db_session,
        response_channel="sms",
        summary_items_shown=[{"text": "Mar 19: Extra Ed Science Club registration deadline", "item_id": "science-fail"}],
        actionable_items=[
            {
                "item_id": "science-fail",
                "title": "Extra Ed Science Club registration deadline",
                "kind": "deadline",
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "date_sort_key": start_at.isoformat(),
                "action_capabilities": {"can_add": True, "can_explain": True},
            }
        ],
        thread_key="msg-sms-fail-followup",
    )

    import app.main as main_module

    if hasattr(main_module.calendar_provider, "fail_next"):
        main_module.calendar_provider.fail_next = True

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-followup-fail-evt"
    payload["provider_message_id"] = "sms-followup-fail-msg"
    payload["body_text"] = "Add Extra Ed Science Club registration deadline to the calendar"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"
    assert "couldn't add it to the calendar right now" in response.json()["message"]
    event = db_session.scalar(
        select(Event).where(Event.household_id == 1, Event.title == "Extra Ed Science Club registration deadline")
    )
    assert event is None
