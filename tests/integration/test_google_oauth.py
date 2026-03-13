from urllib.parse import parse_qs, urlparse
from types import SimpleNamespace

from app.main import _build_oauth_state


def test_google_oauth_start_redirects(client, monkeypatch):
    import app.main as main_module

    config = dict(main_module.settings.__dict__)
    config.update({"google_client_id": "client-123"})
    monkeypatch.setattr(
        main_module,
        "settings",
        SimpleNamespace(**config),
    )
    response = client.get("/auth/google/start", follow_redirects=False)
    assert response.status_code in {302, 307}

    location = response.headers["location"]
    query = parse_qs(urlparse(location).query)
    assert query["client_id"][0] == "client-123"
    assert query["scope"][0] == "openid email https://www.googleapis.com/auth/calendar"


def test_google_oauth_callback_upserts_credentials(client, db_session, monkeypatch):
    import app.main as main_module

    config = dict(main_module.settings.__dict__)
    config.update(
        {
            "google_client_id": "client-123",
            "google_client_secret": "secret-123",
            "google_oauth_redirect_uri": "http://localhost:8000/oauth/google/callback",
            "google_calendar_timeout_sec": 10,
        }
    )
    monkeypatch.setattr(
        main_module,
        "settings",
        SimpleNamespace(**config),
    )

    class MockResponse:
        def __init__(self, status_code: int, payload: dict):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    class MockClient:
        def __init__(self, timeout: int):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, data=None):
            assert "oauth2.googleapis.com/token" in url
            return MockResponse(200, {"access_token": "token-1", "refresh_token": "refresh-1", "expires_in": 3600})

        def get(self, url, headers=None):
            assert "googleapis.com/oauth2/v2/userinfo" in url
            return MockResponse(200, {"email": "admin@example.com"})

    monkeypatch.setattr(main_module.httpx, "Client", MockClient)

    state = _build_oauth_state(1)
    response = client.get(f"/oauth/google/callback?code=abc123&state={state}")
    assert response.status_code == 200
    assert response.json()["status"] == "connected"
    assert response.json()["provider_user_email"] == "admin@example.com"
