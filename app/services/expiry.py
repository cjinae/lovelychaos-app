from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import PendingEvent


def expire_pending_events(db: Session) -> int:
    now = datetime.now(timezone.utc)
    pending = db.scalars(select(PendingEvent).where(PendingEvent.status == "pending")).all()
    count = 0
    for item in pending:
        expires_at = item.expires_at if item.expires_at.tzinfo else item.expires_at.replace(tzinfo=timezone.utc)
        event_start = item.event_start if item.event_start.tzinfo else item.event_start.replace(tzinfo=timezone.utc)
        if expires_at <= now or event_start < now:
            item.status = "expired"
            item.version += 1
            count += 1
    db.flush()
    return count
