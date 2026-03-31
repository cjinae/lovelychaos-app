"""P2: Verify email and SMS share a unified household session."""
from __future__ import annotations

from sqlalchemy import select

from app.models import AgentSessionItem
from app.services.agent_threads import household_session_id


def test_email_and_sms_write_to_same_session(client, db_session, sms_sim):
    """Both channels should create AgentSessionItems under ``household:1``."""
    session_id = household_session_id(household_id=1)

    # Send an email — creates user turn + assistant turn
    sms_sim.send_email(
        subject="School Closure Alert",
        body_text="School closure on 2099-10-01 08:30 due to weather.",
    )

    email_items = list(
        db_session.scalars(
            select(AgentSessionItem).where(AgentSessionItem.session_id == session_id)
        )
    )
    assert len(email_items) >= 1

    email_count = len(email_items)

    # Send an SMS — creates assistant turn (user turn handled by SDK in live mode)
    sms_sim.send_sms("tell me more about the closure")

    all_items = list(
        db_session.scalars(
            select(AgentSessionItem).where(AgentSessionItem.session_id == session_id)
        )
    )
    # SMS should add at least an assistant response to the same session
    assert len(all_items) > email_count


def test_email_user_turn_tagged_with_channel(client, db_session, sms_sim):
    """Email user session items should be prefixed with ``[via email]``."""
    session_id = household_session_id(household_id=1)

    sms_sim.send_email(
        subject="Frankland Update",
        body_text="Spring concert on May 15.",
    )

    items = list(
        db_session.scalars(
            select(AgentSessionItem)
            .where(AgentSessionItem.session_id == session_id)
            .order_by(AgentSessionItem.id.asc())
        )
    )

    user_items = [i for i in items if (i.payload or {}).get("role") == "user"]
    texts = [
        str(c.get("text", ""))
        for item in user_items
        for c in (item.payload or {}).get("content", [])
    ]

    assert any("[via email]" in t for t in texts), f"No email-tagged item found in {texts}"


def test_sms_assistant_response_tagged_with_channel(client, db_session, sms_sim):
    """SMS assistant session items should include ``[via sms]`` tag."""
    session_id = household_session_id(household_id=1)

    sms_sim.send_sms("tell me more about Pizza Lunch")

    items = list(
        db_session.scalars(
            select(AgentSessionItem)
            .where(AgentSessionItem.session_id == session_id)
            .order_by(AgentSessionItem.id.asc())
        )
    )

    assistant_items = [i for i in items if (i.payload or {}).get("role") == "assistant"]
    texts = [
        str(c.get("text", ""))
        for item in assistant_items
        for c in (item.payload or {}).get("content", [])
    ]

    assert any("[via sms]" in t or "[via SMS]" in t for t in texts), (
        f"No channel-tagged assistant item found in {texts}"
    )


def test_cross_channel_items_coexist_in_session(client, db_session, sms_sim):
    """Email and SMS items should coexist in the same household session."""
    session_id = household_session_id(household_id=1)

    sms_sim.send_email(
        subject="School Update",
        body_text="School closure on 2099-10-01 08:30 due to weather.",
    )
    sms_sim.send_sms("tell me more about the closure")

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

    has_email = any("[via email]" in t for t in all_texts)
    has_sms = any("[via sms]" in t or "[via SMS]" in t for t in all_texts)
    assert has_email, f"No email-tagged item in session: {all_texts}"
    assert has_sms, f"No sms-tagged item in session: {all_texts}"
