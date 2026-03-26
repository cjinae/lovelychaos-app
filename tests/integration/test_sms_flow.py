from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from twilio.request_validator import RequestValidator

from app.config import settings
from app.models import Event, GoogleCredential, SourceMessage, User
from tests.fixtures import PAYLOAD_SMS_CONFIRM


def _twilio_signature_headers(payload: dict[str, str]) -> dict[str, str]:
    if not settings.twilio_auth_token:
        return {}
    validator = RequestValidator(settings.twilio_auth_token)
    signature = validator.compute_signature("http://testserver/webhooks/twilio/sms", payload)
    return {"x-twilio-signature": signature}


def _seed_event(db_session, title: str = "SMS Delete") -> Event:
    src = SourceMessage(
        provider="mock-email",
        provider_message_id=f"seed-sms-{title}",
        sender="admin@example.com",
        household_id=1,
        subject="seed",
        body_text="seed",
    )
    db_session.add(src)
    db_session.flush()
    event = Event(
        household_id=1,
        source_message_id=src.id,
        title=title,
        start_at=datetime.now(timezone.utc) + timedelta(days=1),
        end_at=datetime.now(timezone.utc) + timedelta(days=1, hours=1),
        timezone="UTC",
        status="calendar_synced",
        calendar_event_id="mock-sms-delete",
    )
    db_session.add(event)
    db_session.commit()
    return event


def test_sms_more_info_happy_path(client):
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["body_text"] = "more info about Space Pirates musical"
    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "Space Pirates musical" in response.json()["message"]


def test_sms_unverified_sender_rejected(client):
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-evt-unverified"
    payload["provider_message_id"] = "sms-msg-unverified"
    payload["sender_phone"] = "+19999999999"
    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "rejected_unverified_sender"


def test_sms_ambiguous_sender_rejected(client, db_session):
    db_session.add(
        User(
            household_id=1,
            email="another-admin@example.com",
            phone="+15550000001",
            is_admin=True,
            verified=True,
        )
    )
    db_session.commit()

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-evt-ambiguous"
    payload["provider_message_id"] = "sms-msg-ambiguous"
    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "rejected_ambiguous_sender"


def test_sms_non_primary_number_rejected(client):
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-evt-non-primary"
    payload["provider_message_id"] = "sms-msg-non-primary"
    payload["sender_phone"] = "+15550000002"
    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "rejected_unverified_sender"


def test_sms_legacy_confirm_is_unsupported(client):
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-evt-confirm"
    payload["provider_message_id"] = "sms-msg-confirm"
    payload["body_text"] = "confirm 9"
    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"


def test_twilio_sms_webhook_normalizes_into_more_info_flow(client):
    payload = {
        "MessageSid": "SMtwilio-more-info-1",
        "From": "+15550000001",
        "To": "+15551112222",
        "Body": "more info about Pizza Lunch",
    }
    response = client.post(
        "/webhooks/twilio/sms",
        data=payload,
        headers=_twilio_signature_headers(payload),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"


def test_sms_delete_command_still_supported(client, db_session):
    event = _seed_event(db_session)
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-delete-evt"
    payload["provider_message_id"] = "sms-delete-msg"
    payload["body_text"] = f"delete {event.id}"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"


def test_sms_add_command_bypasses_token_refresh_in_mock_calendar_mode(client, db_session):
    original_mode = settings.google_calendar_mode
    object.__setattr__(settings, "google_calendar_mode", "mock")
    credential = db_session.scalar(select(GoogleCredential).where(GoogleCredential.household_id == 1))
    assert credential is not None
    credential.token_expiry = datetime.now(timezone.utc) - timedelta(days=1)
    credential.refresh_token = None
    db_session.commit()

    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-add-evt"
    payload["provider_message_id"] = "sms-add-msg"
    payload["body_text"] = "add Family Field Trip to the cal for May 9, 2099"

    try:
        response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
        assert response.status_code == 200
        assert response.json()["status"] == "command_completed"
        event = db_session.scalar(select(Event).where(Event.household_id == 1, Event.title == "Family Field Trip"))
        assert event is not None
    finally:
        object.__setattr__(settings, "google_calendar_mode", original_mode)
