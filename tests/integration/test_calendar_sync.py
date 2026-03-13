from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.main import calendar_provider
from app.models import Event, SourceMessage
from tests.fixtures import PAYLOAD_CLEAN


def test_bucket_a_auto_add_syncs_calendar(client, db_session):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-calendar-a"
    payload["provider_message_id"] = "msg-calendar-a"
    payload["subject"] = "School Closure Alert"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    event = db_session.scalar(select(Event).where(Event.title == "School Closure Alert"))
    assert event is not None
    assert event.status == "calendar_synced"
    assert event.calendar_event_id is not None


def test_calendar_sync_failure_falls_back_to_pending(client):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-calendar-fail"
    payload["provider_message_id"] = "msg-calendar-fail"

    if hasattr(calendar_provider, "fail_next"):
        calendar_provider.fail_next = True

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"


def test_delete_event_command_removes_calendar_event(client, db_session):
    src = SourceMessage(
        provider="mock-email",
        provider_message_id="seed-msg-delete",
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
        title="Delete me",
        start_at=datetime.now(timezone.utc) + timedelta(days=1),
        end_at=datetime.now(timezone.utc) + timedelta(days=1, hours=1),
        timezone="UTC",
        status="calendar_synced",
        calendar_event_id="mock-to-delete",
    )
    db_session.add(event)
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-delete-cmd"
    payload["provider_message_id"] = "msg-delete-cmd"
    payload["body_text"] = f"delete {event.id}"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"

    db_session.refresh(event)
    assert event.status == "deleted"
