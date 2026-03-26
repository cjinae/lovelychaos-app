from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.models import Child, TeacherContact
from app.services.auto_add import evaluate_auto_add_candidate
from app.services.llm import ExtractedEvent
from app.services.relevancy import compute_relevancy_evidence


def _child(child_id: int, name: str, school: str, grade: str, teachers: list[tuple[str, str]] | None = None) -> Child:
    return Child(
        id=child_id,
        household_id=1,
        name=name,
        school_name=school,
        grade=grade,
        status="active",
        teacher_contacts=[
            TeacherContact(teacher_name=teacher_name, teacher_email=teacher_email, status="active")
            for teacher_name, teacher_email in (teachers or [])
        ],
    )


def _event(title: str, *, reason: str = "", scope: str = "school_global", grades=None, schools=None, preference=False):
    start = datetime.now(timezone.utc) + timedelta(days=7)
    return ExtractedEvent(
        title=title,
        start_at=start,
        end_at=start + timedelta(hours=1),
        category="school",
        confidence=0.95,
        target_scope=scope,
        target_grades=grades or [],
        mentioned_schools=schools or [],
        preference_match=preference,
        model_reason=reason,
    )


def test_auto_add_disabled_for_school_breaks():
    # Auto-add is globally disabled; all items require explicit user confirmation.
    children = [_child(1, "Nolan", "Frankland", "1")]
    event = _event("March Break", reason="school break", preference=True)
    relevancy = compute_relevancy_evidence(
        event_text="March Break school break",
        target_grades=[],
        model_preference_match=True,
        children=children,
        positive_preference_topics=["School Closures"],
    )
    decision = evaluate_auto_add_candidate(event, relevancy, children)
    assert decision.allow is False
    assert decision.reason == "auto_add_disabled"


def test_auto_add_disabled_for_volunteer_items():
    children = [_child(1, "Nolan", "Frankland", "1")]
    event = _event(
        "Spring Swap volunteer setup shift",
        reason="Volunteer need for setup shift",
        scope="school_specific",
        schools=["Frankland"],
    )
    relevancy = compute_relevancy_evidence(
        event_text="Spring Swap volunteer setup shift Frankland",
        target_grades=[],
        model_preference_match=False,
        children=children,
        positive_preference_topics=["Pizza Days"],
    )
    decision = evaluate_auto_add_candidate(event, relevancy, children)
    assert decision.allow is False
    assert decision.reason == "auto_add_disabled"


def test_auto_add_disabled_for_grade_mismatch():
    children = [_child(1, "Nolan", "Frankland", "1"), _child(2, "Jayden", "Frankland", "JK")]
    event = _event(
        "Grade 5 girls volleyball tournament",
        reason="Explicit dated sports event for Grade 5 girls at Frankland.",
        scope="grade_specific",
        grades=["5"],
        schools=["Frankland"],
    )
    relevancy = compute_relevancy_evidence(
        event_text="Grade 5 girls volleyball tournament at Frankland",
        target_grades=["5"],
        model_preference_match=False,
        children=children,
        positive_preference_topics=["Pizza Days", "Swim Days"],
    )
    decision = evaluate_auto_add_candidate(event, relevancy, children)
    assert decision.allow is False
    assert decision.reason == "auto_add_disabled"


def test_auto_add_disabled_for_suppressed_preferences():
    children = [_child(1, "Nolan", "Frankland", "1")]
    event = _event("Spirit Day", reason="theme day", scope="school_global")
    relevancy = compute_relevancy_evidence(
        event_text="Spirit Day at Frankland",
        target_grades=[],
        model_preference_match=False,
        children=children,
        positive_preference_topics=["Spirit Days"],
    )
    decision = evaluate_auto_add_candidate(event, relevancy, children, suppressed_match=True)
    assert decision.allow is False
    assert decision.reason == "auto_add_disabled"


def test_auto_add_disabled_for_teacher_linked_preference_event():
    children = [
        _child(
            1,
            "Nolan",
            "Frankland",
            "1",
            teachers=[("Helen Poulos", "helen.poulos@tdsb.on.ca")],
        )
    ]
    event = _event("Swim Day", reason="class swim tomorrow", scope="child_specific")
    relevancy = compute_relevancy_evidence(
        event_text="Swim Day class swim tomorrow",
        target_grades=[],
        model_preference_match=False,
        children=children,
        positive_preference_topics=["Swim Days"],
        sender_email="helen.poulos@tdsb.on.ca",
        sender_display_name="Helen Poulos",
        target_scope="child_specific",
    )

    decision = evaluate_auto_add_candidate(event, relevancy, children)
    assert relevancy.teacher_match is True
    assert decision.allow is False
    assert decision.reason == "auto_add_disabled"
