from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import uuid
from zoneinfo import ZoneInfo

import httpx

from app.services.openai_tracing import function_trace_span


class CalendarMutationError(Exception):
    pass


@dataclass
class CalendarCreateResult:
    calendar_event_id: str
    html_link: Optional[str] = None


@dataclass
class CalendarEventResult:
    calendar_event_id: str
    title: str
    start_at: Optional[datetime]
    end_at: Optional[datetime]
    all_day: bool = False
    html_link: Optional[str] = None
    location: Optional[str] = None


class CalendarProvider:
    def create_event(
        self,
        access_token: str,
        calendar_id: str,
        title: str,
        start_at: datetime,
        end_at: datetime,
        timezone: str,
        all_day: bool = False,
    ) -> CalendarCreateResult:
        raise NotImplementedError

    def delete_event(self, access_token: str, calendar_id: str, calendar_event_id: str) -> None:
        raise NotImplementedError

    def find_events(
        self,
        access_token: str,
        calendar_id: str,
        *,
        query: str = "",
        time_min: Optional[datetime] = None,
        time_max: Optional[datetime] = None,
        max_results: int = 10,
    ) -> list[CalendarEventResult]:
        raise NotImplementedError

    def update_event(
        self,
        access_token: str,
        calendar_id: str,
        calendar_event_id: str,
        *,
        title: Optional[str] = None,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
        timezone: str = "UTC",
        all_day: Optional[bool] = None,
        location: Optional[str] = None,
    ) -> None:
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
        all_day: bool = False,
    ) -> CalendarCreateResult:
        if self.fail_next:
            self.fail_next = False
            raise CalendarMutationError("mock create failure")
        return CalendarCreateResult(calendar_event_id=f"mock-{uuid.uuid4()}")

    def delete_event(self, access_token: str, calendar_id: str, calendar_event_id: str) -> None:
        if self.fail_next:
            self.fail_next = False
            raise CalendarMutationError("mock delete failure")

    def find_events(
        self,
        access_token: str,
        calendar_id: str,
        *,
        query: str = "",
        time_min: Optional[datetime] = None,
        time_max: Optional[datetime] = None,
        max_results: int = 10,
    ) -> list[CalendarEventResult]:
        return []

    def update_event(
        self,
        access_token: str,
        calendar_id: str,
        calendar_event_id: str,
        *,
        title: Optional[str] = None,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
        timezone: str = "UTC",
        all_day: Optional[bool] = None,
        location: Optional[str] = None,
    ) -> None:
        if self.fail_next:
            self.fail_next = False
            raise CalendarMutationError("mock update failure")

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
        all_day: bool = False,
    ) -> CalendarCreateResult:
        url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
        if all_day:
            try:
                tz = ZoneInfo(timezone)
                start_local = start_at.astimezone(tz)
                end_local = end_at.astimezone(tz)
            except Exception:
                start_local = start_at
                end_local = end_at
            start_date = start_local.date().isoformat()
            if (
                end_local.date() == start_local.date()
                and end_local.hour == 23
                and end_local.minute == 59
                and end_local.second == 59
            ):
                end_date = (end_local.date() + timedelta(days=1)).isoformat()
            else:
                end_date = end_local.date().isoformat()
            payload = {
                "summary": title,
                "start": {"date": start_date},
                "end": {"date": end_date},
            }
        else:
            payload = {
                "summary": title,
                "start": {"dateTime": start_at.isoformat(), "timeZone": timezone},
                "end": {"dateTime": end_at.isoformat(), "timeZone": timezone},
            }
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        with function_trace_span(
            "calendar.create_event",
            input_text=f"calendar_id={calendar_id}\ntitle={title}\nall_day={all_day}",
        ):
            with httpx.Client(timeout=self.timeout_sec) as client:
                response = client.post(url, json=payload, headers=headers)
        if response.status_code >= 400:
            raise CalendarMutationError(f"google create failed: {response.status_code}")
        body = response.json()
        return CalendarCreateResult(calendar_event_id=body["id"], html_link=body.get("htmlLink"))

    def delete_event(self, access_token: str, calendar_id: str, calendar_event_id: str) -> None:
        url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{calendar_event_id}"
        headers = {"Authorization": f"Bearer {access_token}"}
        with function_trace_span(
            "calendar.delete_event",
            input_text=f"calendar_id={calendar_id}\ncalendar_event_id={calendar_event_id}",
        ):
            with httpx.Client(timeout=self.timeout_sec) as client:
                response = client.delete(url, headers=headers)
        if response.status_code >= 400:
            raise CalendarMutationError(f"google delete failed: {response.status_code}")

    def find_events(
        self,
        access_token: str,
        calendar_id: str,
        *,
        query: str = "",
        time_min: Optional[datetime] = None,
        time_max: Optional[datetime] = None,
        max_results: int = 10,
    ) -> list[CalendarEventResult]:
        url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {
            "singleEvents": "true",
            "orderBy": "startTime",
            "maxResults": max(1, min(int(max_results or 10), 25)),
        }
        if query:
            params["q"] = query
        if time_min is not None:
            params["timeMin"] = time_min.isoformat()
        if time_max is not None:
            params["timeMax"] = time_max.isoformat()
        with function_trace_span(
            "calendar.find_events",
            input_text=(
                f"calendar_id={calendar_id}\nquery={query}\ntime_min={time_min.isoformat() if time_min else ''}\n"
                f"time_max={time_max.isoformat() if time_max else ''}\nmax_results={params['maxResults']}"
            ),
        ):
            with httpx.Client(timeout=self.timeout_sec) as client:
                response = client.get(url, params=params, headers=headers)
        if response.status_code >= 400:
            raise CalendarMutationError(f"google find failed: {response.status_code}")
        body = response.json()
        items: list[CalendarEventResult] = []
        for item in list(body.get("items") or []):
            start_at, start_all_day = _parse_google_event_time(item.get("start"))
            end_at, _ = _parse_google_event_time(item.get("end"))
            items.append(
                CalendarEventResult(
                    calendar_event_id=str(item.get("id") or ""),
                    title=str(item.get("summary") or "").strip() or "Untitled event",
                    start_at=start_at,
                    end_at=end_at,
                    all_day=start_all_day,
                    html_link=item.get("htmlLink"),
                    location=str(item.get("location") or "").strip() or None,
                )
            )
        return items

    def update_event(
        self,
        access_token: str,
        calendar_id: str,
        calendar_event_id: str,
        *,
        title: Optional[str] = None,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
        timezone: str = "UTC",
        all_day: Optional[bool] = None,
        location: Optional[str] = None,
    ) -> None:
        url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{calendar_event_id}"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        payload: dict[str, object] = {}
        if title is not None:
            payload["summary"] = title
        if location is not None:
            payload["location"] = location
        if start_at is not None and end_at is not None:
            if bool(all_day):
                try:
                    tz = ZoneInfo(timezone)
                    start_local = start_at.astimezone(tz)
                    end_local = end_at.astimezone(tz)
                except Exception:
                    start_local = start_at
                    end_local = end_at
                payload["start"] = {"date": start_local.date().isoformat()}
                payload["end"] = {"date": end_local.date().isoformat()}
            else:
                payload["start"] = {"dateTime": start_at.isoformat(), "timeZone": timezone}
                payload["end"] = {"dateTime": end_at.isoformat(), "timeZone": timezone}
        with function_trace_span(
            "calendar.update_event",
            input_text=(
                f"calendar_id={calendar_id}\ncalendar_event_id={calendar_event_id}\n"
                f"title={title or ''}\nlocation={location or ''}"
            ),
        ):
            with httpx.Client(timeout=self.timeout_sec) as client:
                response = client.patch(url, json=payload, headers=headers)
        if response.status_code >= 400:
            raise CalendarMutationError(f"google update failed: {response.status_code}")

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
        with function_trace_span(
            "calendar.set_event_reminder",
            input_text=(
                f"calendar_id={calendar_id}\ncalendar_event_id={calendar_event_id}\nminutes_before={minutes_before}"
            ),
        ):
            with httpx.Client(timeout=self.timeout_sec) as client:
                response = client.patch(url, json=payload, headers=headers)
        if response.status_code >= 400:
            raise CalendarMutationError(f"google reminder update failed: {response.status_code}")


def _parse_google_event_time(payload: object) -> tuple[Optional[datetime], bool]:
    if not isinstance(payload, dict):
        return None, False
    date_time = str(payload.get("dateTime") or "").strip()
    if date_time:
        try:
            return datetime.fromisoformat(date_time.replace("Z", "+00:00")), False
        except ValueError:
            return None, False
    date_only = str(payload.get("date") or "").strip()
    if date_only:
        try:
            return datetime.fromisoformat(f"{date_only}T00:00:00+00:00"), True
        except ValueError:
            return None, True
    return None, False
