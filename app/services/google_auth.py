from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx


class GoogleAuthError(Exception):
    pass


def should_refresh_token(token_expiry: Optional[datetime], refresh_window_minutes: int = 5) -> bool:
    if token_expiry is None:
        return False
    expiry = token_expiry if token_expiry.tzinfo else token_expiry.replace(tzinfo=timezone.utc)
    return expiry <= datetime.now(timezone.utc) + timedelta(minutes=refresh_window_minutes)


def refresh_google_access_token(
    refresh_token: str,
    client_id: str,
    client_secret: str,
    timeout_sec: int = 10,
) -> tuple[str, Optional[datetime]]:
    if not refresh_token:
        raise GoogleAuthError("Missing refresh token")
    if not client_id or not client_secret:
        raise GoogleAuthError("Google OAuth client credentials are not configured")

    with httpx.Client(timeout=timeout_sec) as client:
        response = client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )
    if response.status_code >= 400:
        raise GoogleAuthError(f"Token refresh failed: {response.status_code}")

    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise GoogleAuthError("Token refresh response missing access_token")

    expires_in = payload.get("expires_in")
    expiry = None
    if isinstance(expires_in, int):
        expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    return access_token, expiry
