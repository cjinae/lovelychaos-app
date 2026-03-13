from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Event, InformationalItem, Reminder


def _safe_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def build_daily_summary(db: Session, household_id: int) -> dict:
    now = datetime.now(timezone.utc)
    until = now + timedelta(days=1)

    events = db.scalars(
        select(Event).where(
            Event.household_id == household_id,
            Event.status.in_(["calendar_synced", "intent_saved"]),
        )
    ).all()
    events = [e for e in events if now <= _safe_utc(e.start_at) <= until]

    reminders = db.scalars(
        select(Reminder).where(Reminder.household_id == household_id, Reminder.status == "scheduled")
    ).all()
    reminders = [r for r in reminders if now <= _safe_utc(r.trigger_at) <= until]
    informational = db.scalars(
        select(InformationalItem).where(InformationalItem.household_id == household_id, InformationalItem.status == "stored")
    ).all()
    informational = [i for i in informational if now - timedelta(days=1) <= _safe_utc(i.created_at) <= until]

    lines = ["Daily Summary"]
    if events:
        lines.append(f"Upcoming events (24h): {len(events)}")
        lines.extend([f"- Event #{e.id}: {e.title}" for e in events[:10]])
    else:
        lines.append("Upcoming events (24h): 0")

    lines.append(f"Scheduled reminders (24h): {len(reminders)}")
    lines.append(f"Informational updates (24h): {len(informational)}")

    items = []
    for e in events:
        items.append({"item_type": "event", "source_ref": f"event:{e.id}", "priority": 2})
    for r in reminders:
        items.append({"item_type": "reminder", "source_ref": f"reminder:{r.id}", "priority": 1})
    for i in informational:
        items.append({"item_type": "informational", "source_ref": f"info:{i.id}", "priority": i.priority})

    return {
        "subject": "LovelyChaos Daily Summary",
        "message": "\n".join(lines),
        "items": items,
    }


def build_weekly_digest(db: Session, household_id: int) -> dict:
    now = datetime.now(timezone.utc)
    until = now + timedelta(days=7)

    events = db.scalars(
        select(Event).where(
            Event.household_id == household_id,
            Event.status.in_(["calendar_synced", "intent_saved"]),
        )
    ).all()
    events = [e for e in events if now <= _safe_utc(e.start_at) <= until]
    informational = db.scalars(
        select(InformationalItem).where(InformationalItem.household_id == household_id, InformationalItem.status == "stored")
    ).all()
    informational = [i for i in informational if now <= _safe_utc(i.created_at) <= until]

    lines = ["Weekly Digest"]
    lines.append(f"Upcoming events (7d): {len(events)}")
    lines.append(f"Informational updates (7d): {len(informational)}")

    items = []
    for e in events:
        items.append({"item_type": "event", "source_ref": f"event:{e.id}", "priority": 2})
    for i in informational:
        items.append({"item_type": "informational", "source_ref": f"info:{i.id}", "priority": i.priority})

    return {
        "subject": "LovelyChaos Weekly Digest",
        "message": "\n".join(lines),
        "items": items,
    }
