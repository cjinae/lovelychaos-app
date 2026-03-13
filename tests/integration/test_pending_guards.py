def test_admin_pending_endpoints_are_gone(client):
    res = client.get("/admin/pending-events")
    assert res.status_code == 410
    assert "no longer part of the admin experience" in res.json()["detail"].lower()


def test_admin_pending_mutation_endpoints_are_gone(client):
    confirm = client.post("/admin/pending-events/1/confirm", json={"version": 1})
    reject = client.post("/admin/pending-events/1/reject", json={"version": 1})
    assert confirm.status_code == 410
    assert reject.status_code == 410
