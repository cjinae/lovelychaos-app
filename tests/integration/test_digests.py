from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import DigestItem, Event, Household, SourceMessage


def _seed_event(db_session, title: str):
    src = SourceMessage(
        provider="mock-email",
        provider_message_id=f"digest-seed-{title}",
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
        start_at=datetime.now(timezone.utc) + timedelta(hours=6),
        end_at=datetime.now(timezone.utc) + timedelta(hours=7),
        timezone="UTC",
        status="calendar_synced",
        calendar_event_id="digest-event",
    )
    db_session.add(event)
    db_session.commit()


def test_daily_summary_job_creates_digest_items_and_notifications(client, db_session):
    _seed_event(db_session, "Daily Digest Event")

    res = client.post("/internal/jobs/daily-summary")
    assert res.status_code == 200
    body = res.json()
    assert body["sent_households"] >= 1
    assert body["digest_items_created"] >= 1

    items = db_session.scalars(select(DigestItem).where(DigestItem.household_id == 1)).all()
    assert len(items) >= 1


def test_weekly_digest_job_respects_toggle(client, db_session):
    household = db_session.scalar(select(Household).where(Household.id == 1))
    assert household is not None
    household.weekly_digest_enabled = False
    db_session.commit()

    res = client.post("/internal/jobs/weekly-digest")
    assert res.status_code == 200
    body = res.json()
    assert body["sent_households"] == 0
    assert body["skipped_households"] >= 1
