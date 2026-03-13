from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import select

from app.models import Event, FollowupContext, NotificationDelivery, PreferenceProfile, SourceMessage, User
from tests.fixtures import PAYLOAD_CLEAN, PAYLOAD_SMS_CONFIRM


def test_email_ingestion_prefers_sms_for_recap(client, db_session, monkeypatch):
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
    assert delivery.channel == "sms"


def test_email_ingestion_falls_back_to_email_without_phone(client, db_session, monkeypatch):
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
    source = SourceMessage(
        provider="mock-email",
        provider_message_id="msg-followup-source",
        source_channel="email",
        sender="admin@example.com",
        household_id=1,
        subject="Frankland Update",
        body_text="Space Pirates musical rehearsal details are in the gym after lunch.",
    )
    db_session.add(source)
    db_session.flush()
    db_session.add(
        FollowupContext(
            household_id=1,
            source_message_id=source.id,
            origin_channel="email",
            response_channel="sms",
            thread_or_conversation_key="msg-followup-source",
            summary_title="Frankland Update",
            summary_items_shown=[{"text": "Space Pirates musical", "source_refs": [], "applies_to": [], "date_sort_key": None}],
            all_extracted_items=[{"title": "Space Pirates musical", "reason": "school musical", "start_at": None, "end_at": None}],
            section_snippets=[{"label": "music", "text": "Space Pirates musical rehearsal details are in the gym after lunch."}],
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        )
    )
    db_session.commit()

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-more-info-ctx"
    payload["provider_message_id"] = "sms-more-info-ctx"
    payload["body_text"] = "tell me more about Space Pirates musical"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "Space Pirates musical" in response.json()["message"]
    assert "rehearsal details" in response.json()["message"]


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
