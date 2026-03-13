from datetime import datetime, timedelta, timezone

from app.models import PendingEvent
from app.services.expiry import expire_pending_events


def test_expiry_marks_expired(db_session):
    db_session.add(
        PendingEvent(
            household_id=1,
            title="Old Item",
            event_start=datetime.now(timezone.utc) - timedelta(hours=1),
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        )
    )
    db_session.commit()

    expired = expire_pending_events(db_session)
    db_session.commit()

    assert expired == 1
