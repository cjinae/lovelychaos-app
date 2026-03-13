from __future__ import annotations

from datetime import datetime, timedelta, timezone
from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.models import IdempotencyKey, WebhookReceipt


def purge_old_records(db: Session, retention_days: int = 30) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    webhook_deleted = db.execute(
        delete(WebhookReceipt).where(WebhookReceipt.received_at < cutoff)
    ).rowcount or 0
    idempotency_deleted = db.execute(
        delete(IdempotencyKey).where(IdempotencyKey.first_seen_at < cutoff)
    ).rowcount or 0

    return {
        "retention_days": retention_days,
        "deleted": {
            "webhook_receipts": webhook_deleted,
            "idempotency_keys": idempotency_deleted,
        },
    }
