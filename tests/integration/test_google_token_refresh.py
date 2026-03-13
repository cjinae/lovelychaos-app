from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import Event, GoogleCredential, SourceMessage
from tests.fixtures import PAYLOAD_CLEAN


def test_internal_google_refresh_endpoint(client, db_session, monkeypatch):
    credential = db_session.scalar(select(GoogleCredential).where(GoogleCredential.household_id == 1))
    assert credential is not None
    credential.refresh_token = "refresh-token-1"
    credential.token_expiry = datetime.now(timezone.utc) - timedelta(minutes=1)
    db_session.commit()

    import app.main as main_module

    def fake_refresh(refresh_token, client_id, client_secret, timeout_sec=10):
        assert refresh_token == "refresh-token-1"
        return "new-access-token", datetime.now(timezone.utc) + timedelta(hours=1)

    monkeypatch.setattr(main_module, "refresh_google_access_token", fake_refresh)

    response = client.post("/internal/google/refresh?household_id=1")
    assert response.status_code == 200
    assert response.json()["status"] == "refreshed"


def test_calendar_mutation_auto_refreshes_expired_token(client, db_session, monkeypatch):
    credential = db_session.scalar(select(GoogleCredential).where(GoogleCredential.household_id == 1))
    assert credential is not None
    credential.refresh_token = "refresh-token-2"
    credential.token_expiry = datetime.now(timezone.utc) - timedelta(minutes=1)
    db_session.commit()

    import app.main as main_module

    def fake_refresh(refresh_token, client_id, client_secret, timeout_sec=10):
        assert refresh_token == "refresh-token-2"
        return "refreshed-for-mutation", datetime.now(timezone.utc) + timedelta(hours=1)

    monkeypatch.setattr(main_module, "refresh_google_access_token", fake_refresh)

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-refresh-mutation"
    payload["provider_message_id"] = "msg-refresh-mutation"
    payload["subject"] = "School Closure Refresh"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    db_session.refresh(credential)
    assert credential.access_token == "refreshed-for-mutation"

    message = db_session.scalar(select(SourceMessage).where(SourceMessage.provider_message_id == "msg-refresh-mutation"))
    assert message is not None
    event = db_session.scalar(select(Event).where(Event.source_message_id == message.id))
    assert event is not None
