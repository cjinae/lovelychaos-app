from tests.fixtures import PAYLOAD_CLEAN


def test_admin_inbound_activity_returns_recent_records(client):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-activity-1"
    payload["provider_message_id"] = "msg-activity-1"
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200

    activity = client.get("/admin/inbound-activity?limit=5")
    assert activity.status_code == 200
    body = activity.json()
    assert "receipts" in body
    assert "messages" in body
    assert "events" in body
    assert "informational_items" in body
    assert "decision_audits" in body
    assert any(r["provider_event_id"] == "evt-activity-1" for r in body["receipts"])
    assert any(m["provider_message_id"] == "msg-activity-1" for m in body["messages"])
