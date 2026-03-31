"""Reusable multi-step cross-channel test scenarios."""
from __future__ import annotations


SCENARIO_EMAIL_THEN_SMS_MORE_INFO: list[dict] = [
    {
        "channel": "email",
        "subject": "Frankland Newsletter March",
        "body_text": (
            "Pizza Lunch is on April 10, 2099 at 11:30. "
            "Field Trip to the zoo on April 15, 2099."
        ),
    },
    {
        "channel": "sms",
        "body_text": "tell me more about Pizza Lunch",
    },
]


SCENARIO_EMAIL_THEN_SMS_ADD: list[dict] = [
    {
        "channel": "email",
        "subject": "Spring Events",
        "body_text": "Spring Concert on May 15, 2099 at 18:00 in the auditorium.",
    },
    {
        "channel": "sms",
        "body_text": "add Spring Concert to the calendar",
    },
]


SCENARIO_MULTI_TURN_CROSS_CHANNEL: list[dict] = [
    {
        "channel": "email",
        "subject": "Weather Alert",
        "body_text": "School closure on 2099-10-01 08:30 due to severe weather.",
    },
    {
        "channel": "sms",
        "body_text": "when is the closure",
    },
    {
        "channel": "sms",
        "body_text": "add the closure to my calendar",
    },
]
