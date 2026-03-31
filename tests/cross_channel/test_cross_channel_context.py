"""P0: Verify followup context crosses channel boundaries."""
from __future__ import annotations

from sqlalchemy import select

from app.models import Event, FollowupContext, SmsConversationState
from app.services.followups import load_active_followup_context


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_PIZZA_ITEM = {
    "text": "Pizza Lunch on April 10",
    "item_id": "pizza-lunch",
    "source_refs": [],
    "applies_to": [],
    "date_sort_key": "2099-04-10",
    "action_capabilities": {"can_add": True, "can_explain": True},
}

_PIZZA_ACTIONABLE = {
    "item_id": "pizza-lunch",
    "title": "Pizza Lunch",
    "kind": "event",
    "reason": "school lunch",
    "start_at": "2099-04-10T11:30:00",
    "end_at": "2099-04-10T12:30:00",
    "action_capabilities": {"can_add": True, "can_explain": True},
}

_FIELD_TRIP_ITEM = {
    "text": "Field Trip to the zoo on April 15",
    "item_id": "zoo-trip",
    "source_refs": [],
    "applies_to": [],
    "date_sort_key": "2099-04-15",
    "action_capabilities": {"can_add": True, "can_explain": True},
}

_FIELD_TRIP_ACTIONABLE = {
    "item_id": "zoo-trip",
    "title": "Zoo Field Trip",
    "kind": "event",
    "reason": "field trip",
    "start_at": "2099-04-15T09:00:00",
    "end_at": "2099-04-15T14:00:00",
    "action_capabilities": {"can_add": True, "can_explain": True},
}


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


def test_email_followup_context_visible_to_sms(
    client, db_session, sms_sim, seed_email_with_followup
):
    """SMS ``more_info`` command should find a FollowupContext created by email."""
    seed_email_with_followup(
        summary_items_shown=[_PIZZA_ITEM],
        actionable_items=[_PIZZA_ACTIONABLE],
        body_text="Pizza Lunch is on April 10, 2099 at 11:30.",
        response_channel="email",
    )

    # The cross_channel fallback should let SMS find the email-originated context
    ctx = load_active_followup_context(
        db_session,
        household_id=1,
        response_channel="sms",
        cross_channel=True,
    )
    assert ctx is not None
    assert ctx.origin_channel == "email"

    result = sms_sim.send_sms("tell me more about Pizza Lunch")
    assert result["status"] in ("command_completed", "command_needs_clarification")


def test_sms_more_info_from_email_followup_context(
    client, db_session, sms_sim, seed_email_with_followup
):
    """SMS ``more_info`` about a topic should resolve from email FollowupContext."""
    seed_email_with_followup(
        summary_items_shown=[_FIELD_TRIP_ITEM],
        actionable_items=[_FIELD_TRIP_ACTIONABLE],
        body_text="Field Trip to the zoo on April 15, 2099.",
        response_channel="email",
    )

    result = sms_sim.send_sms("tell me more about the field trip")
    assert result["status"] in ("command_completed", "command_needs_clarification")


def test_sms_channel_specific_context_found_first(
    client, db_session, sms_sim, seed_email_with_followup
):
    """When an SMS-channel FollowupContext exists, it should be preferred over
    the cross-channel fallback."""
    # Email-originated context
    seed_email_with_followup(
        summary_items_shown=[_PIZZA_ITEM],
        actionable_items=[_PIZZA_ACTIONABLE],
        body_text="Pizza Lunch is on April 10.",
        response_channel="email",
    )
    # SMS-originated context (more recent)
    seed_email_with_followup(
        summary_items_shown=[_FIELD_TRIP_ITEM],
        actionable_items=[_FIELD_TRIP_ACTIONABLE],
        body_text="Field Trip to the zoo on April 15.",
        response_channel="sms",
    )

    ctx = load_active_followup_context(
        db_session,
        household_id=1,
        response_channel="sms",
        cross_channel=True,
    )
    assert ctx is not None
    # SMS-channel context should win
    assert ctx.response_channel == "sms"
