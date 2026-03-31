"""P3: Verify channel-aware response formatting."""
from __future__ import annotations


def test_channel_tags_not_leaked_in_response(client, sms_sim):
    """The ``[via sms]`` / ``[via email]`` session tags should not appear
    in the user-facing response message."""

    result = sms_sim.send_sms("tell me more about Pizza Lunch")
    message = result.get("message", "")
    assert "[via sms]" not in message.lower()
    assert "[via email]" not in message.lower()

    result = sms_sim.send_email(
        subject="Frankland Update",
        body_text="School closure on 2099-10-01 08:30.",
    )
    message = result.get("message", "")
    assert "[via sms]" not in message.lower()
    assert "[via email]" not in message.lower()


def test_sms_response_is_string(client, sms_sim):
    """Basic structural check: SMS responses should always be strings."""
    result = sms_sim.send_sms("when is the next pizza lunch")
    assert isinstance(result.get("message"), str)
    assert len(result["message"]) > 0
