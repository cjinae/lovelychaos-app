from datetime import datetime, timedelta, timezone

from app.models import Child
from app.services.auto_add import evaluate_auto_add_candidate
from app.services.llm import ExtractedEvent
from app.services.relevancy import compute_relevancy_evidence


def _child(child_id: int, name: str, school: str, grade: str) -> Child:
    return Child(id=child_id, household_id=1, name=name, school_name=school, grade=grade, status="active")


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


def test_auto_add_allows_school_breaks_and_holidays():
    children = [_child(1, "Nolan", "Frankland", "1")]
    event = _event("March Break", reason="school break", preference=True)
    relevancy = compute_relevancy_evidence(
        event_text="March Break school break",
        target_grades=[],
        model_preference_match=True,
        children=children,
        preference_text="school closures",
    )
    decision = evaluate_auto_add_candidate(event, relevancy, children)
    assert decision.allow is True
    assert decision.reason == "closure_or_break"


def test_auto_add_blocks_optional_schoolwide_and_volunteer_items():
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
        preference_text="school closures, pizza day",
    )
    decision = evaluate_auto_add_candidate(event, relevancy, children)
    assert decision.allow is False
    assert decision.reason == "optional_or_admin_event"


def test_auto_add_blocks_grade_mismatch():
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
        preference_text="pizza day, swim schedule",
    )
    decision = evaluate_auto_add_candidate(event, relevancy, children)
    assert decision.allow is False
    assert decision.reason == "grade_mismatch"
