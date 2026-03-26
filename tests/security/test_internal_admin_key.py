from types import SimpleNamespace


def test_internal_routes_require_admin_key_when_configured(client, monkeypatch):
    import app.main as main_module

    monkeypatch.setattr(main_module, "settings", SimpleNamespace(admin_api_key="top-secret"))

    no_key = client.post("/internal/jobs/retention")
    assert no_key.status_code == 401
    assert no_key.json()["detail"] == "Invalid admin key"

    wrong_key = client.post("/internal/jobs/retention", headers={"X-Admin-Key": "wrong"})
    assert wrong_key.status_code == 401

    ok = client.post("/internal/jobs/retention", headers={"X-Admin-Key": "top-secret"})
    assert ok.status_code == 200
    assert "deleted" in ok.json()
