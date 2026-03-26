from sqlalchemy import select

from app.models import PreferenceProfile, PreferenceRule
from app.services.priorities import load_priority_preferences


def test_onboarding_preferences_form_redirects_back_with_interpreted_preferences(client, db_session):
    response = client.post(
        "/onboarding/preferences/form",
        data={
            "raw_text": "i care about pizza days, swim days. I don't care about cultural days.",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
        },
    )

    assert response.status_code == 200
    assert "Structured topics are available in admin if you want to review or edit them." in response.text
    assert "Topics will appear here as you type." not in response.text
    assert "LovelyChaos interpreted this as important" not in response.text
    assert "Cultural Days" not in response.text

    profile = db_session.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == 1))
    assert profile is not None
    structured = dict(profile.structured_json or {})

    assert structured["selected_priority_topics"] == []
    assert [item["label"] for item in structured["parsed_priority_topics"]] == ["Pizza Days", "Swim Days"]
    assert [item["label"] for item in structured["parsed_suppressed_topics"]] == ["Cultural Days"]
    assert [item["label"] for item in structured["user_priority_topics"]] == ["Pizza Days", "Swim Days"]

    rules = db_session.scalars(
        select(PreferenceRule).where(PreferenceRule.household_id == 1).order_by(PreferenceRule.source.asc())
    ).all()
    assert any(rule.source == "user_priority" and rule.category == "pizza_days" for rule in rules)
    assert any(rule.source == "user_priority" and rule.category == "swim_days" for rule in rules)
    assert any(
        rule.source == "user_note" and rule.mode == "preference_behavior" and rule.category == "cultural_days"
        and rule.behavior == "suppress"
        for rule in rules
    )


def test_admin_preferences_payload_returns_preset_and_parsed_topics(client):
    client.post(
        "/admin/preferences/form",
        data={
            "raw_text": "pizza lunches, hot lunch",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
        },
    )

    response = client.get("/admin/preferences")
    assert response.status_code == 200
    payload = response.json()

    assert payload["parsed_priority_topics"] == ["Pizza Lunches", "Hot Lunch"]
    assert payload["suppressed_priority_topics"] == []
    assert payload["user_priority_topics"] == ["Pizza Lunches", "Hot Lunch"]


def test_onboarding_preferences_form_shows_parse_error_when_parser_fails(client, db_session, monkeypatch):
    import app.main as main_module

    def _boom(_raw_text: str):
        return {
            "positive_topics": [],
            "negative_topics": [],
            "status": "error",
            "error": "RuntimeError: preference parser unavailable",
        }

    monkeypatch.setattr(main_module, "_parse_preference_notes", _boom)

    response = client.post(
        "/onboarding/preferences/form",
        data={
            "raw_text": "pizza days, I don't care about cultural days",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
        },
    )

    assert response.status_code == 200
    assert "Could not interpret last save." in response.text

    profile = db_session.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == 1))
    assert profile is not None
    structured = dict(profile.structured_json or {})
    assert structured["preference_parse_status"] == "error"
    assert structured["preference_parse_error"] == "RuntimeError: preference parser unavailable"
    assert structured["parsed_priority_topics"] == []
    assert structured["parsed_suppressed_topics"] == []


def test_admin_preferences_page_shows_parse_error_detail(client, monkeypatch):
    import app.main as main_module

    def _boom(_raw_text: str):
        return {
            "positive_topics": [],
            "negative_topics": [],
            "status": "error",
            "error": "ValueError: mock parse failed",
        }

    monkeypatch.setattr(main_module, "_parse_preference_notes", _boom)

    response = client.post(
        "/admin/preferences/form",
        data={
            "raw_text": "pizza days",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
        },
    )

    assert response.status_code == 200
    assert "Saved your notes, but LovelyChaos could not interpret them right now." in response.text
    assert "ValueError: mock parse failed" in response.text


def test_onboarding_preferences_form_falls_back_when_openai_parse_fails(client, db_session, monkeypatch):
    import app.main as main_module

    def _boom(*_args, **_kwargs):
        raise RuntimeError("OpenAI parse unavailable")

    monkeypatch.setattr(main_module.engine_llm, "parse_preference_notes", _boom)

    response = client.post(
        "/onboarding/preferences/form",
        data={
            "raw_text": "i care about pizza days, swim days. I don't care about cultural days.",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
        },
    )

    assert response.status_code == 200
    assert "Could not interpret last save." not in response.text
    assert "LovelyChaos saved your notes." in response.text

    profile = db_session.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == 1))
    assert profile is not None
    structured = dict(profile.structured_json or {})
    assert structured["preference_parse_status"] == "success"
    assert structured["preference_parse_error"] == ""
    assert [item["label"] for item in structured["parsed_priority_topics"]] == ["Pizza Days", "Swim Days"]
    assert [item["label"] for item in structured["parsed_suppressed_topics"]] == ["Cultural Days"]


def test_admin_preferences_form_falls_back_when_openai_parse_fails(client, monkeypatch):
    import app.main as main_module

    def _boom(*_args, **_kwargs):
        raise RuntimeError("OpenAI parse unavailable")

    monkeypatch.setattr(main_module.engine_llm, "parse_preference_notes", _boom)

    response = client.post(
        "/admin/preferences/form",
        data={
            "raw_text": "i care about pizza days, swim days. I don't care about cultural days.",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
        },
    )

    assert response.status_code == 200
    assert "LovelyChaos interpreted your notes successfully." in response.text
    assert "Pizza Days" in response.text
    assert "Swim Days" in response.text
    assert "Cultural Days" in response.text


def test_admin_topics_override_stays_authoritative_until_regenerated(client):
    response = client.post(
        "/admin/preferences/form",
        data={
            "raw_text": "pizza lunches, hot lunch",
            "admin_priority_topics_text": "School Council Meetings\nSports Events",
            "admin_suppressed_priority_topics_text": "Extra Curricular",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
            "prefs_action": "save",
        },
    )

    assert response.status_code == 200
    payload = client.get("/admin/preferences").json()
    assert payload["admin_override_active"] is True
    assert payload["parsed_priority_topics"] == ["Pizza Lunches", "Hot Lunch"]
    assert payload["admin_priority_topics"] == ["School Council Meetings", "Sports Events"]
    assert payload["admin_suppressed_priority_topics"] == ["Extra Curricular"]
    assert payload["user_priority_topics"] == ["School Council Meetings", "Sports Events"]
    assert payload["suppressed_priority_topics"] == ["Extra Curricular"]

    response = client.post(
        "/admin/preferences/form",
        data={
            "raw_text": "pizza lunches, hot lunch, ignore cultural days",
            "admin_priority_topics_text": "School Council Meetings\nSports Events",
            "admin_suppressed_priority_topics_text": "Extra Curricular",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
            "prefs_action": "save",
        },
    )

    assert response.status_code == 200
    payload = client.get("/admin/preferences").json()
    assert payload["admin_override_active"] is True
    assert payload["parsed_priority_topics"] == ["Pizza Lunches", "Hot Lunch"]
    assert payload["parsed_suppressed_priority_topics"] == ["Cultural Days"]
    assert payload["user_priority_topics"] == ["School Council Meetings", "Sports Events"]
    assert payload["suppressed_priority_topics"] == ["Extra Curricular"]

    response = client.post(
        "/admin/preferences/form",
        data={
            "raw_text": "pizza lunches, hot lunch, ignore cultural days",
            "admin_priority_topics_text": "School Council Meetings\nSports Events",
            "admin_suppressed_priority_topics_text": "Extra Curricular",
            "system_default_school_closures": "on",
            "system_default_grade_relevant": "on",
            "prefs_action": "regenerate",
        },
    )

    assert response.status_code == 200
    payload = client.get("/admin/preferences").json()
    assert payload["admin_override_active"] is False
    assert payload["parsed_priority_topics"] == ["Pizza Lunches", "Hot Lunch"]
    assert payload["parsed_suppressed_priority_topics"] == ["Cultural Days"]
    assert payload["admin_priority_topics"] == []
    assert payload["admin_suppressed_priority_topics"] == []
    assert payload["user_priority_topics"] == ["Pizza Lunches", "Hot Lunch"]
    assert payload["suppressed_priority_topics"] == ["Cultural Days"]


def test_load_priority_preferences_recomputes_effective_topics_from_bucketed_state(db_session):
    profile = db_session.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == 1))
    assert profile is not None
    profile.raw_text = "school council meetings, sports events, school concerts, i don't care about extra curricular"
    profile.structured_json = {
        "user_priority_topics": [
            {"key": "i_care_about_pizza_days", "label": "I Care About Pizza Days"},
            {"key": "swim_days_i_don_t_care_about_cultural_days", "label": "Swim Days. I Don't Care About Cultural Days."},
        ],
        "selected_priority_topics": [],
        "parsed_priority_topics": [
            {"key": "school_council_meetings", "label": "School Council Meetings"},
            {"key": "sports_events", "label": "Sports Events"},
            {"key": "school_concerts", "label": "School Concerts"},
        ],
        "parsed_suppressed_topics": [
            {"key": "extra_curricular", "label": "Extra Curricular"},
        ],
        "preference_parse_status": "success",
        "preference_parse_error": "",
    }
    db_session.commit()

    payload = load_priority_preferences(db_session, 1)
    refreshed = db_session.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == 1))
    assert refreshed is not None

    assert payload["user_priority_topics"] == ["School Council Meetings", "Sports Events", "School Concerts"]
    assert payload["suppressed_priority_topics"] == ["Extra Curricular"]
    assert [item["label"] for item in refreshed.structured_json["user_priority_topics"]] == [
        "School Council Meetings",
        "Sports Events",
        "School Concerts",
    ]
