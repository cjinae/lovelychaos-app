from tests.fixtures import PAYLOAD_CLEAN, PAYLOAD_SMS_CONFIRM


EXPECTED_WEBHOOK_FIELDS = {
    "status",
    "message",
    "request_id",
    "mutation_executed",
    "operation_id",
    "processing_state",
}


def test_webhook_contract_fields(client):
    response = client.post("/webhooks/email/inbound", json=PAYLOAD_CLEAN, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    body = response.json()
    assert EXPECTED_WEBHOOK_FIELDS.issubset(body.keys())
    assert body["status"] in {
        "ingestion_accepted",
        "command_accepted_for_processing",
        "command_completed",
        "command_needs_clarification",
        "rejected_unverified_sender",
        "rejected_ambiguous_sender",
        "rejected_unauthorized",
        "rejected_tenant_mismatch",
        "rejected_validation",
    }
    assert body["processing_state"] in {"queued", "in_progress", "completed", "failed"}


def test_operation_contract_fields(client):
    payload = dict(PAYLOAD_CLEAN)
    payload.update(
        {
            "provider_event_id": "evt-contract",
            "provider_message_id": "msg-contract",
            "body_text": "more info about Pizza Lunch later",
        }
    )
    queued = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    if queued.json()["operation_id"]:
        op = client.get(f"/operations/{queued.json()['operation_id']}")
        assert op.status_code == 200
        body = op.json()
        assert set(body.keys()) == {
            "operation_id",
            "status",
            "processing_state",
            "last_updated_at",
            "mutation_executed",
            "user_message",
        }


def test_sms_webhook_contract_fields(client):
    payload = dict(PAYLOAD_SMS_CONFIRM)
    payload["provider_event_id"] = "sms-contract-1"
    payload["provider_message_id"] = "sms-contract-1-msg"
    response = client.post("/webhooks/sms/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    body = response.json()
    assert EXPECTED_WEBHOOK_FIELDS.issubset(body.keys())
    assert body["status"] in {
        "command_accepted_for_processing",
        "command_completed",
        "command_needs_clarification",
        "rejected_unverified_sender",
        "rejected_ambiguous_sender",
        "rejected_unauthorized",
        "rejected_tenant_mismatch",
        "rejected_validation",
    }
