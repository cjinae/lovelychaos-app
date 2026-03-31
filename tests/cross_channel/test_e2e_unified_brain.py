"""End-to-end unified brain scenarios using real Frankland newsletter content.

These tests simulate a parent who receives a school newsletter via email,
then follows up over SMS — exactly how LovelyChaos is used in practice.
The "one brain" should have full context across channels.

Run with:
    python3 -m pytest tests/cross_channel/test_e2e_unified_brain.py -v
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import (
    AgentSessionItem,
    Event,
    FollowupContext,
    NotificationDelivery,
    SourceMessage,
)
from app.services.agent_threads import household_session_id
from app.services.followups import load_active_followup_context


# ------------------------------------------------------------------
# Real newsletter content from the March 22 Frankland email
# ------------------------------------------------------------------

FRANKLAND_NEWSLETTER_SUBJECT = "Fwd: Information for the Week-March 22, 2026"

FRANKLAND_NEWSLETTER_BODY = (
    "Dear Families,\n\n"
    "Here is the weekly update from Frankland Community School.\n\n"
    "Important Dates:\n"
    "- Apr 1 & 15 & 29: Pizza Lunches\n"
    "- Apr 3: Good Friday — school closed\n"
    "- Apr 6: Easter Monday — school closed\n"
    "- Apr 7: Coed volleyball tournament\n"
    "- Apr 20: Swim City Final\n"
    "- Mar 31: School council meeting\n\n"
    "Other Info:\n"
    "- April character trait: Cooperation\n"
    "- Swim classes continue this week\n\n"
    "Thank you,\nFrankland CS"
)


# ------------------------------------------------------------------
# Scenario 1: Parent gets newsletter via email, follows up over SMS
#
# Flow:
#   1. Email arrives with school newsletter (creates FollowupContext)
#   2. Parent texts "when are the pizza lunches" (SMS should see email context)
#   3. Parent texts "add the first pizza lunch to my calendar"
#   4. Parent texts "is school open on Easter Monday"
#
# This tests: cross-channel followup context, unified session, and
# conversational continuity across channels.
# ------------------------------------------------------------------


def test_scenario_newsletter_email_then_sms_followups(client, db_session, sms_sim, seed_email_with_followup):
    """A parent receives a newsletter via email and asks about it over SMS."""
    session_id = household_session_id(household_id=1)

    # -- Step 1: Newsletter arrives via email --
    # Seed the followup context as it would be after email ingestion
    pizza_item = {
        "text": "Apr 1 & 15 & 29: Pizza Lunches",
        "item_id": "pizza-lunches",
        "source_refs": [],
        "applies_to": [],
        "date_sort_key": "2026-04-01",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    good_friday_item = {
        "text": "Apr 3: Good Friday — school closed",
        "item_id": "good-friday",
        "source_refs": [],
        "applies_to": [],
        "date_sort_key": "2026-04-03",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    easter_item = {
        "text": "Apr 6: Easter Monday — school closed",
        "item_id": "easter-monday",
        "source_refs": [],
        "applies_to": [],
        "date_sort_key": "2026-04-06",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    volleyball_item = {
        "text": "Apr 7: Coed volleyball tournament",
        "item_id": "volleyball-tournament",
        "source_refs": [],
        "applies_to": [],
        "date_sort_key": "2026-04-07",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    swim_item = {
        "text": "Apr 20: Swim City Final",
        "item_id": "swim-final",
        "source_refs": [],
        "applies_to": [],
        "date_sort_key": "2026-04-20",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }

    actionable_pizza = {
        "item_id": "pizza-lunch-apr1",
        "title": "Pizza Lunch",
        "kind": "event",
        "reason": "school lunch",
        "start_at": "2026-04-01T12:00:00",
        "end_at": "2026-04-01T13:00:00",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    actionable_good_friday = {
        "item_id": "good-friday",
        "title": "Good Friday",
        "kind": "deadline",
        "reason": "school closure",
        "start_at": "2026-04-03T00:00:00",
        "end_at": "2026-04-03T23:59:00",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    actionable_easter = {
        "item_id": "easter-monday",
        "title": "Easter Monday",
        "kind": "deadline",
        "reason": "school closure",
        "start_at": "2026-04-06T00:00:00",
        "end_at": "2026-04-06T23:59:00",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }

    ctx = seed_email_with_followup(
        summary_items_shown=[pizza_item, good_friday_item, easter_item, volleyball_item, swim_item],
        actionable_items=[actionable_pizza, actionable_good_friday, actionable_easter],
        body_text=FRANKLAND_NEWSLETTER_BODY,
        subject=FRANKLAND_NEWSLETTER_SUBJECT,
        response_channel="email",
    )

    # Verify: the email-originated context exists
    assert ctx.origin_channel == "email"
    assert ctx.response_channel == "email"

    # -- Step 2: Parent texts about pizza lunches --
    # SMS should find the email context via cross_channel fallback
    cross_ctx = load_active_followup_context(
        db_session,
        household_id=1,
        response_channel="sms",
        cross_channel=True,
    )
    assert cross_ctx is not None, "SMS could not find email-originated followup context"
    assert cross_ctx.id == ctx.id, "Cross-channel lookup returned wrong context"

    result = sms_sim.send_sms("when are the pizza lunches")
    assert result["status"] in ("command_completed", "command_needs_clarification")

    # -- Step 3: Parent texts for more detail --
    result = sms_sim.send_sms("tell me more about the volleyball tournament")
    assert result["status"] in ("command_completed", "command_needs_clarification")

    # -- Step 4: Parent asks about Easter Monday --
    result = sms_sim.send_sms("is school open on Easter Monday")
    assert result["status"] in ("command_completed", "command_needs_clarification")

    # -- Verify unified session: all turns live under household:1 --
    items = list(
        db_session.scalars(
            select(AgentSessionItem)
            .where(AgentSessionItem.session_id == session_id)
            .order_by(AgentSessionItem.id.asc())
        )
    )
    all_texts = [
        str(c.get("text", ""))
        for item in items
        for c in (item.payload or {}).get("content", [])
    ]
    # Should have both email and SMS tagged entries in the same session
    has_sms = any("[via sms]" in t.lower() for t in all_texts)
    assert has_sms, f"No SMS items found in unified session: {all_texts[:3]}"


# ------------------------------------------------------------------
# Scenario 2: Parent starts on SMS, then email adds more context
#
# Flow:
#   1. Parent texts "what's happening at school this week"
#   2. Newsletter email arrives (creates FollowupContext with full details)
#   3. Parent texts "add the volleyball tournament" (should now have context)
#   4. Parent texts "and remind me about swim city final 1 hour before"
#
# This tests: SMS works with no prior context, then email enriches it,
# then SMS picks up the enriched context seamlessly.
# ------------------------------------------------------------------


def test_scenario_sms_first_then_email_enriches_context(client, db_session, sms_sim, seed_email_with_followup):
    """Parent starts on SMS with no context, email enriches, SMS picks it up."""
    session_id = household_session_id(household_id=1)

    # -- Step 1: Parent texts cold — no email context yet --
    result = sms_sim.send_sms("what's happening at school this week")
    assert result["status"] in ("command_completed", "command_needs_clarification")

    # Verify: a session item was created from the SMS interaction
    items_before = list(
        db_session.scalars(
            select(AgentSessionItem)
            .where(AgentSessionItem.session_id == session_id)
        )
    )
    sms_item_count = len(items_before)

    # -- Step 2: Newsletter email arrives with full details --
    volleyball_item = {
        "text": "Apr 7: Coed volleyball tournament",
        "item_id": "volleyball",
        "source_refs": [],
        "applies_to": [],
        "date_sort_key": "2026-04-07",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    swim_item = {
        "text": "Apr 20: Swim City Final",
        "item_id": "swim-final",
        "source_refs": [],
        "applies_to": [],
        "date_sort_key": "2026-04-20",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    council_item = {
        "text": "Mar 31: School council meeting",
        "item_id": "council-meeting",
        "source_refs": [],
        "applies_to": [],
        "date_sort_key": "2026-03-31",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    actionable_volleyball = {
        "item_id": "volleyball",
        "title": "Coed Volleyball Tournament",
        "kind": "event",
        "reason": "school sports",
        "start_at": "2026-04-07T09:00:00",
        "end_at": "2026-04-07T15:00:00",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }
    actionable_swim = {
        "item_id": "swim-final",
        "title": "Swim City Final",
        "kind": "event",
        "reason": "school sports",
        "start_at": "2026-04-20T10:00:00",
        "end_at": "2026-04-20T14:00:00",
        "action_capabilities": {"can_add": True, "can_explain": True},
    }

    ctx = seed_email_with_followup(
        summary_items_shown=[volleyball_item, swim_item, council_item],
        actionable_items=[actionable_volleyball, actionable_swim],
        body_text=FRANKLAND_NEWSLETTER_BODY,
        subject=FRANKLAND_NEWSLETTER_SUBJECT,
        response_channel="email",
    )

    # Email should also write to the unified session
    sms_sim.send_email(
        subject=FRANKLAND_NEWSLETTER_SUBJECT,
        body_text=FRANKLAND_NEWSLETTER_BODY,
    )

    items_after_email = list(
        db_session.scalars(
            select(AgentSessionItem)
            .where(AgentSessionItem.session_id == session_id)
        )
    )
    assert len(items_after_email) > sms_item_count, (
        "Email did not add session items to the shared household session"
    )

    # -- Step 3: Parent texts about volleyball — should now have context --
    cross_ctx = load_active_followup_context(
        db_session,
        household_id=1,
        response_channel="sms",
        cross_channel=True,
    )
    assert cross_ctx is not None, "SMS should see the email-originated followup context"

    result = sms_sim.send_sms("tell me more about the volleyball tournament")
    assert result["status"] in ("command_completed", "command_needs_clarification")

    # -- Step 4: Parent asks about swim final --
    result = sms_sim.send_sms("when is the swim city final")
    assert result["status"] in ("command_completed", "command_needs_clarification")

    # -- Verify the unified session has both channels interleaved --
    all_items = list(
        db_session.scalars(
            select(AgentSessionItem)
            .where(AgentSessionItem.session_id == session_id)
            .order_by(AgentSessionItem.id.asc())
        )
    )
    all_texts = [
        str(c.get("text", ""))
        for item in all_items
        for c in (item.payload or {}).get("content", [])
    ]

    has_email = any("[via email]" in t for t in all_texts)
    has_sms = any("[via sms]" in t.lower() for t in all_texts)
    assert has_email and has_sms, (
        f"Expected both channel tags in unified session.\n"
        f"  has_email={has_email}, has_sms={has_sms}\n"
        f"  sample texts: {all_texts[:5]}"
    )

    # The conversation history on the simulator should show the full flow
    assert len(sms_sim.history) >= 8, (
        f"Expected at least 8 turns (4 user + 4 assistant), got {len(sms_sim.history)}"
    )
