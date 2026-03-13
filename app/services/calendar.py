from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid

import httpx


class CalendarMutationError(Exception):
    pass


@dataclass
class CalendarCreateResult:
    calendar_event_id: str
    html_link: Optional[str] = None


class CalendarProvider:
    def create_event(
        self,
        access_token: str,
        calendar_id: str,
        title: str,
        start_at: datetime,
        end_at: datetime,
        timezone: str,
    ) -> CalendarCreateResult:
        raise NotImplementedError

    def delete_event(self, access_token: str, calendar_id: str, calendar_event_id: str) -> None:
        raise NotImplementedError

    def set_event_reminder(
        self,
        access_token: str,
        calendar_id: str,
        calendar_event_id: str,
        minutes_before: int,
    ) -> None:
        raise NotImplementedError


class MockCalendarProvider(CalendarProvider):
    def __init__(self) -> None:
        self.fail_next = False

    def create_event(
        self,
        access_token: str,
        calendar_id: str,
        title: str,
        start_at: datetime,
        end_at: datetime,
        timezone: str,
    ) -> CalendarCreateResult:
        if self.fail_next:
            self.fail_next = False
            raise CalendarMutationError("mock create failure")
        return CalendarCreateResult(calendar_event_id=f"mock-{uuid.uuid4()}")

    def delete_event(self, access_token: str, calendar_id: str, calendar_event_id: str) -> None:
        if self.fail_next:
            self.fail_next = False
            raise CalendarMutationError("mock delete failure")

    def set_event_reminder(
        self,
        access_token: str,
        calendar_id: str,
        calendar_event_id: str,
        minutes_before: int,
    ) -> None:
        if self.fail_next:
            self.fail_next = False
            raise CalendarMutationError("mock reminder failure")


class GoogleCalendarHttpProvider(CalendarProvider):
    def __init__(self, timeout_sec: int = 10) -> None:
        self.timeout_sec = timeout_sec

    def create_event(
        self,
        access_token: str,
        calendar_id: str,
        title: str,
        start_at: datetime,
        end_at: datetime,
        timezone: str,
    ) -> CalendarCreateResult:
        url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
        payload = {
            "summary": title,
            "start": {"dateTime": start_at.isoformat(), "timeZone": timezone},
            "end": {"dateTime": end_at.isoformat(), "timeZone": timezone},
        }
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        with httpx.Client(timeout=self.timeout_sec) as client:
            response = client.post(url, json=payload, headers=headers)
        if response.status_code >= 400:
            raise CalendarMutationError(f"google create failed: {response.status_code}")
        body = response.json()
        return CalendarCreateResult(calendar_event_id=body["id"], html_link=body.get("htmlLink"))

    def delete_event(self, access_token: str, calendar_id: str, calendar_event_id: str) -> None:
        url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{calendar_event_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        with httpx.Client(timeout=self.timeout_sec) as client:
            response = client.delete(url, headers=headers)
        if response.status_code >= 400:
            raise CalendarMutationError(f"google delete failed: {response.status_code}")

    def set_event_reminder(
        self,
        access_token: str,
        calendar_id: str,
        calendar_event_id: str,
        minutes_before: int,
    ) -> None:
        url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{calendar_event_id}"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        payload = {
            "reminders": {
                "useDefault": False,
                "overrides": [{"method": "popup", "minutes": int(minutes_before)}],
            }
        }
        with httpx.Client(timeout=self.timeout_sec) as client:
            response = client.patch(url, json=payload, headers=headers)
        if response.status_code >= 400:
            raise CalendarMutationError(f"google reminder update failed: {response.status_code}")
