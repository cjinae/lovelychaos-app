from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import Event, Reminder, SourceMessage
from tests.fixtures import PAYLOAD_CLEAN, PAYLOAD_SMS_CONFIRM


def _seed_event(db_session, title: str = "Reminder Event", with_calendar_id: bool = True) -> Event:
    src = SourceMessage(
        provider="mock-email",
        provider_message_id=f"seed-reminder-{title}",
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
        calendar_event_id="mock-calendar-event" if with_calendar_id else None,
    )
    db_session.add(event)
    db_session.commit()
    return event


def test_email_reminder_sms_channel_creates_reminder(client, db_session):
    event = _seed_event(db_session, title="Email Reminder")
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-remind-email"
    payload["provider_message_id"] = "msg-remind-email"
    payload["body_text"] = f"remind {event.id} 30m sms"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"

    reminder = db_session.scalar(select(Reminder).where(Reminder.event_id == event.id))
    assert reminder is not None
    assert reminder.channel == "sms"


def test_email_reminder_calendar_channel_creates_reminder(client, db_session):
    event = _seed_event(db_session, title="Calendar Reminder")
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-remind-cal"
    payload["provider_message_id"] = "msg-remind-cal"
    payload["body_text"] = f"remind {event.id} 45m calendar"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"


def test_sms_reminder_command(client, db_session):
    event = _seed_event(db_session, title="SMS Reminder")
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-remind-1"
    payload["provider_message_id"] = "sms-remind-1-msg"
    payload["body_text"] = f"remind {event.id} 20m sms"

    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"


def test_reminder_invalid_past_time_needs_clarification(client, db_session):
    src = SourceMessage(
        provider="mock-email",
        provider_message_id="seed-past-reminder",
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
        title="Soon Event",
        start_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        end_at=datetime.now(timezone.utc) + timedelta(minutes=40),
        timezone="UTC",
        status="calendar_synced",
        calendar_event_id="mock-calendar-event-2",
    )
    db_session.add(event)
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-remind-past"
    payload["provider_message_id"] = "msg-remind-past"
    payload["body_text"] = f"remind {event.id} 30m sms"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_needs_clarification"
