from datetime import datetime, timedelta, timezone

from app.models import Event, Household, SourceMessage, User


def test_no_tenant_data_leak_on_unverified_sender(client):
    payload = {
        "provider": "mock-email",
        "provider_event_id": "evt-sec-2",
        "provider_message_id": "msg-sec-2",
        "sender": "unknown@example.com",
        "recipient_alias": "schedule@example.com",
        "subject": "x",
        "body_text": "closure 2099-01-01 08:00",
    }
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    msg = response.json()["message"].lower()
    assert "household" not in msg
    assert "calendar id" not in msg


def test_calendar_delete_tenant_mismatch_blocks_mutation(client, db_session):
    db_session.add(Household(id=3, timezone="UTC"))
    db_session.add(User(household_id=3, email="third@example.com", is_admin=True, verified=True))
    src = SourceMessage(
        provider="mock-email",
        provider_message_id="msg-third",
        sender="third@example.com",
        household_id=3,
        subject="x",
        body_text="x",
    )
    db_session.add(src)
    db_session.flush()
    event = Event(
        household_id=3,
        source_message_id=src.id,
        title="Other household event",
        start_at=datetime.now(timezone.utc) + timedelta(days=1),
        end_at=datetime.now(timezone.utc) + timedelta(days=1, hours=1),
        timezone="UTC",
        status="calendar_synced",
        calendar_event_id="cal-3",
    )
    db_session.add(event)
    db_session.commit()

    payload = {
        "provider": "mock-email",
        "provider_event_id": "evt-sec-delete",
        "provider_message_id": "msg-sec-delete",
        "sender": "admin@example.com",
        "recipient_alias": "schedule@example.com",
        "subject": "Delete",
        "body_text": f"delete {event.id}",
    }
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "rejected_tenant_mismatch"
