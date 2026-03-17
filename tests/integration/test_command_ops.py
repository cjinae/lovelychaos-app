from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import Event, FollowupContext, PreferenceProfile, SourceMessage
from tests.fixtures import PAYLOAD_CLEAN, PAYLOAD_COMMAND_CONFIRM


def _seed_event(db_session, title: str = "Delete Me") -> Event:
    src = SourceMessage(
        provider="mock-email",
        provider_message_id=f"seed-command-{title}",
        sender="admin@example.com",
        household_id=1,
        subject="seed",
        body_text="seed",
    )
    db_session.add(src)
    db_session.flush()
    event = Event(
        household_id=1,
        source_message_id=src.id,
        title=title,
        start_at=datetime.now(timezone.utc) + timedelta(days=1),
        end_at=datetime.now(timezone.utc) + timedelta(days=1, hours=1),
        timezone="UTC",
        status="calendar_synced",
        calendar_event_id="mock-delete-event",
    )
    db_session.add(event)
    db_session.commit()
    return event


def test_command_more_info_completes_without_mutation(client):
    payload = dict(PAYLOAD_COMMAND_CONFIRM)
    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "command_completed"
    assert body["mutation_executed"] is False
    assert "More info follow-up captured" in body["message"]


def test_legacy_confirm_command_is_unsupported(client):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-confirm-legacy"
    payload["provider_message_id"] = "msg-confirm-legacy"
    payload["body_text"] = "confirm 12"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"


def test_delete_command_still_deletes_calendar_event(client, db_session):
    event = _seed_event(db_session)
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-delete-command"
    payload["provider_message_id"] = "msg-delete-command"
    payload["body_text"] = f"delete {event.id}"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"

    refreshed = db_session.scalar(select(Event).where(Event.id == event.id))
    db_session.refresh(refreshed)
    assert refreshed is not None
    assert refreshed.status == "deleted"


def test_email_set_preference_command_persists_rule(client, db_session):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-email-pref"
    payload["provider_message_id"] = "msg-email-pref"
    payload["subject"] = "Re: LovelyChaos follow-up"
    payload["body_text"] = "I don't care about school council events"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    profile = db_session.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == 1))
    assert profile is not None
    prefs = list((profile.structured_json or {}).get("command_written_preferences") or [])
    assert any(item.get("key") == "school_council_events" and item.get("behavior") == "suppress" for item in prefs)


def test_plain_email_parser_first_accepts_natural_language_preference_variation(client, db_session):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-email-pref-natural"
    payload["provider_message_id"] = "msg-email-pref-natural"
    payload["subject"] = "Re: LovelyChaos follow-up"
    payload["body_text"] = "Please keep adding pizza lunches"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    profile = db_session.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == 1))
    assert profile is not None
    prefs = list((profile.structured_json or {}).get("command_written_preferences") or [])
    assert any(item.get("key") == "pizza_lunches" and item.get("behavior") == "auto_add" for item in prefs)


def test_plain_email_more_info_uses_parser_first_detection(client, db_session):
    source = SourceMessage(
        provider="mock-email",
        provider_message_id="msg-email-followup-source",
        source_channel="email",
        sender="admin@example.com",
        household_id=1,
        subject="Frankland Update",
        body_text="Space Pirates musical rehearsal details are in the gym after lunch.",
    )
    db_session.add(source)
    db_session.flush()
    db_session.add(
        FollowupContext(
            household_id=1,
            source_message_id=source.id,
            origin_channel="email",
            response_channel="email",
            thread_or_conversation_key="msg-email-followup-source",
            summary_title="Frankland Update",
            summary_items_shown=[{"text": "Space Pirates musical", "source_refs": [], "applies_to": [], "date_sort_key": None}],
            all_extracted_items=[{"title": "Space Pirates musical", "reason": "school musical", "start_at": None, "end_at": None}],
            section_snippets=[{"label": "music", "text": "Space Pirates musical rehearsal details are in the gym after lunch."}],
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        )
    )
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-email-more-info"
    payload["provider_message_id"] = "msg-email-more-info"
    payload["subject"] = "Re: Frankland Update"
    payload["body_text"] = "Tell me more about Space Pirates musical"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "Space Pirates musical" in response.json()["message"]
    assert "rehearsal details" in response.json()["message"]


def test_plain_email_add_command_uses_parser_first_detection(client, db_session):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-email-add-direct"
    payload["provider_message_id"] = "msg-email-add-direct"
    payload["subject"] = "Quick add"
    payload["body_text"] = "please add Family Field Trip to the calendar for May 9, 2026"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
    assert "Added to calendar" in response.json()["message"]
    event = db_session.scalar(select(Event).where(Event.household_id == 1, Event.title == "Family Field Trip"))
    assert event is not None


def test_plain_email_without_supported_command_falls_back_to_ingestion(client):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-email-fallback-ingest"
    payload["provider_message_id"] = "msg-email-fallback-ingest"
    payload["subject"] = "School Closure Alert"
    payload["body_text"] = "School closure on 2099-10-01 08:30 due to weather."

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"


def test_forwarded_email_without_preface_stays_in_ingestion(client):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-no-preface"
    payload["provider_message_id"] = "msg-forwarded-no-preface"
    payload["subject"] = "Fwd: School update"
    payload["body_text"] = (
        "---------- Forwarded message ----------\n"
        "From: Frankland Community School <donotreply@tdsb.on.ca>\n"
        "Date: Sun, Mar 8, 2026 at 6:07 PM\n"
        "Subject: Frankland Newsletter\n"
        "To: admin@example.com\n\n"
        "March Break starts Mar 16."
    )

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"


def test_forwarded_email_with_explicit_preface_command_still_runs_command_flow(client):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-forwarded-preface-command"
    payload["provider_message_id"] = "msg-forwarded-preface-command"
    payload["subject"] = "Fwd: Family field trip"
    payload["body_text"] = (
        "Add this to the calendar\n\n"
        "---------- Forwarded message ----------\n"
        "From: Frankland Community School <donotreply@tdsb.on.ca>\n"
        "Date: Sun, Mar 8, 2026 at 6:07 PM\n"
        "Subject: Family Field Trip\n"
        "To: admin@example.com\n\n"
        "Family Field Trip on 2099-05-09 09:00."
    )

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})

    assert response.status_code == 200
    assert response.json()["status"] == "command_completed"
