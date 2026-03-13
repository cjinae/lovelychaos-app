from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import Event, SourceMessage
from tests.fixtures import PAYLOAD_CLEAN, PAYLOAD_COMMAND_CONFIRM


def _seed_event(db_session, title: str = "Delete Me") -> Event:
    src = SourceMessage(
        provider="mock-email",
        provider_message_id=f"seed-command-{title}",
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
        calendar_event_id="mock-delete-event",
    )
    db_session.add(event)
    db_session.commit()
    return event


def test_command_more_info_completes_without_mutation(client):
    payload = dict(PAYLOAD_COMMAND_CONFIRM)
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "command_completed"
    assert body["mutation_executed"] is False
    assert "More info follow-up captured" in body["message"]


def test_legacy_confirm_command_is_unsupported(client):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-confirm-legacy"
    payload["provider_message_id"] = "msg-confirm-legacy"
    payload["body_text"] = "confirm 12"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"


def test_delete_command_still_deletes_calendar_event(client, db_session):
    event = _seed_event(db_session)
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-delete-command"
    payload["provider_message_id"] = "msg-delete-command"
    payload["body_text"] = f"delete {event.id}"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"

    refreshed = db_session.scalar(select(Event).where(Event.id == event.id))
    db_session.refresh(refreshed)
    assert refreshed is not None
    assert refreshed.status == "deleted"
