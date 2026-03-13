from datetime import datetime, timedelta, timezone

from app.models import IdempotencyKey, WebhookReceipt


def test_health_and_ready(client):
    health = client.get("/health")
    ready = client.get("/ready")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"


def test_retention_job_deletes_old_records(client, db_session):
    old = datetime.now(timezone.utc) - timedelta(days=45)
    db_session.add(
        WebhookReceipt(
            provider="mock-email",
            provider_event_id="evt-old",
            provider_message_id="msg-old",
            received_at=old,
            status="processed",
        )
    )
    db_session.add(
        IdempotencyKey(
            key="old-key",
            scope="webhook",
            household_id=1,
            action_type="ingest",
            target_ref="x",
            first_seen_at=old,
            last_seen_at=old,
            result_hash="h",
        )
    )
    db_session.commit()

    res = client.post("/internal/jobs/retention?retention_days=30")
    assert res.status_code == 200
    deleted = res.json()["deleted"]
    assert deleted["webhook_receipts"] >= 1
    assert deleted["idempotency_keys"] >= 1
