from types import SimpleNamespace

from sqlalchemy import select

from app.models import SourceMessage


def test_resend_inbound_happy_path(client, monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(
        main_module,
        "settings",
        SimpleNamespace(resend_webhook_secret="", webhook_secret="local-dev-secret"),
    )
    payload = {
        "id": "resend-evt-1",
        "type": "email.received",
        "data": {
            "id": "resend-msg-1",
            "from": "Admin <admin@example.com>",
            "to": ["schedule@lovelychaos.ca"],
            "subject": "School closure",
            "text": "School closure 2099-10-02 09:00",
        },
    }
    response = client.post("/webhooks/resend/inbound", json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"


def test_resend_unsupported_event_is_ignored(client, monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(
        main_module,
        "settings",
        SimpleNamespace(resend_webhook_secret="", webhook_secret="local-dev-secret"),
    )
    payload = {"id": "resend-evt-2", "type": "email.delivered", "data": {"id": "resend-msg-2"}}
    response = client.post("/webhooks/resend/inbound", json=payload)
    assert response.status_code == 200
    assert response.json()["message"] == "Ignored unsupported Resend event type"


def test_resend_signature_required_when_configured(client, monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(
        main_module,
        "settings",
        SimpleNamespace(resend_webhook_secret="resend-secret", webhook_secret="local-dev-secret"),
    )
    payload = {
        "id": "resend-evt-3",
        "type": "email.received",
        "data": {
            "id": "resend-msg-3",
            "from": "admin@example.com",
            "to": ["schedule@lovelychaos.ca"],
            "subject": "School closure",
            "text": "School closure 2099-10-02 09:00",
        },
    }

    rejected = client.post("/webhooks/resend/inbound", json=payload)
    assert rejected.status_code == 401

    ok = client.post("/webhooks/resend/inbound", json=payload, headers={"X-Resend-Signature": "resend-secret"})
    assert ok.status_code == 200


def test_resend_inbound_extracts_html_body_text(client, db_session, monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(
        main_module,
        "settings",
        SimpleNamespace(resend_webhook_secret="", webhook_secret="local-dev-secret"),
    )
    payload = {
        "id": "resend-evt-html-1",
        "type": "email.received",
        "data": {
            "id": "resend-msg-html-1",
            "from": "Admin <admin@example.com>",
            "to": ["schedule@lovelychaos.ca"],
            "subject": "Forwarded digest",
            "html": "<div>Upcoming week:<br>- PA Day Friday<br>- Pizza Tuesday</div>",
        },
    }
    response = client.post("/webhooks/resend/inbound", json=payload)
    assert response.status_code == 200
    msg = db_session.scalar(select(SourceMessage).where(SourceMessage.provider_message_id == "resend-msg-html-1"))
    assert msg is not None
    assert "PA Day Friday" in msg.body_text


def test_resend_inbound_fetches_received_email_when_body_missing(client, db_session, monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(
        main_module,
        "settings",
        SimpleNamespace(resend_webhook_secret="", webhook_secret="local-dev-secret", resend_api_key="re_test"),
    )

    def fake_retrieve(email_id: str):
        assert email_id == "email-metadata-only"
        return {
            "text": "Frankland student updates: PA day Mar 6, Pizza Mar 10, Swim Mar 11, Terry Fox Mar 12.",
        }

    monkeypatch.setattr(main_module, "_retrieve_resend_received_email", fake_retrieve)

    payload = {
        "id": "resend-evt-fetch-1",
        "type": "email.received",
        "data": {
            "message_id": "resend-msg-fetch-1",
            "email_id": "email-metadata-only",
            "from": "admin@example.com",
            "to": ["schedule@lovelychaos.ca"],
            "subject": "Forwarded school info",
        },
    }
    response = client.post("/webhooks/resend/inbound", json=payload)
    assert response.status_code == 200
    msg = db_session.scalar(select(SourceMessage).where(SourceMessage.provider_message_id == "resend-msg-fetch-1"))
    assert msg is not None
    assert "PA day Mar 6" in msg.body_text
