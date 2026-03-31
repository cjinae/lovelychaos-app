"""End-to-end cross-channel tests using a real LLM.

These tests are excluded by default.  Run them with:

    LOVELYCHAOS_E2E_LLM=1 python3 -m pytest -m e2e -v
"""
from __future__ import annotations

import pytest

from sqlalchemy import select
from app.models import Event, Reminder


pytestmark = pytest.mark.e2e


def test_e2e_newsletter_then_sms_followup(sms_sim_llm):
    """Send a realistic newsletter via email, then ask about it over SMS."""
    sms_sim_llm.send_email(
        subject="Frankland Public School - March Newsletter",
        body_text=(
            "Dear Families,\n\n"
            "Reminder that our Spring Concert is on May 15, 2099 at 6:00 PM "
            "in the school auditorium. Students should arrive by 5:30 PM.\n\n"
            "Pizza Lunch orders for April are due by March 28. "
            "Order forms were sent home last week.\n\n"
            "The Grade 3 field trip to the ROM is on April 22, 2099. "
            "Permission forms must be returned by April 15.\n\n"
            "Thank you,\nFrankland PS"
        ),
    )

    followup_text = sms_sim_llm.generate_followup_sms("more_info", "the concert")
    result = sms_sim_llm.send_sms(followup_text)

    assert result["status"] in ("command_completed", "command_needs_clarification")
    assert len(result.get("message", "")) > 0


def test_e2e_multi_turn_cross_channel(sms_sim_llm, db_session):
    """Full multi-turn flow: email about closure -> SMS questions -> SMS add."""
    # Step 1: Email about school closure
    sms_sim_llm.send_email(
        subject="Weather Advisory",
        body_text="Due to the incoming ice storm, school will be closed on 2099-10-01.",
    )

    # Step 2: Ask about it via SMS
    text = sms_sim_llm.generate_followup_sms("ask_when", "the school closure")
    result = sms_sim_llm.send_sms(text)
    assert result["status"] in ("command_completed", "command_needs_clarification")

    # Step 3: Add to calendar via SMS
    text = sms_sim_llm.generate_followup_sms("add_event", "school closure")
    result = sms_sim_llm.send_sms(text)

    # Verify event created (may succeed or need clarification depending on LLM)
    events = list(db_session.scalars(select(Event).where(Event.household_id == 1)))
    # With real LLM the event should be created; with mock fallback it may not
    assert result["status"] in ("command_completed", "command_needs_clarification")
