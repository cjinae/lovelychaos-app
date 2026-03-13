from app.models import PreferenceRule
from app.services.routing import resolve_execution_disposition


def test_routing_pref_override_wins(db_session):
    db_session.add(
        PreferenceRule(
            household_id=1,
            source="user_priority",
            category="school_closure",
            priority=100,
            enabled=True,
        )
    )
    db_session.commit()

    route = resolve_execution_disposition(db_session, 1, "school_closure", "pending_event")
    assert route["execution_disposition"] == "pending_event"
    assert route["source"] == "user_priority"
