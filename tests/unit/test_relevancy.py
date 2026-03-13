from app.models import Child
from app.services.relevancy import compute_relevancy_evidence, normalize_grade, parse_grade_range


def _child(child_id: int, name: str, school: str, grade: str) -> Child:
    return Child(id=child_id, household_id=1, name=name, school_name=school, grade=grade, status="active")


def test_relevancy_matches_name_school_and_grade():
    children = [
        _child(1, "Nolan Frankland", "Frankland", "1"),
        _child(2, "Jayden Frankland", "Frankland", "JK"),
    ]
    evidence = compute_relevancy_evidence(
        event_text="Nolan Frankland has swim day at Frankland PS for Grade 1",
        target_grades=["grade 1"],
        model_preference_match=False,
        children=children,
        preference_text="School closures are critical",
    )
    assert evidence.name_match is True
    assert evidence.school_match is True
    assert evidence.grade_match is True
    assert evidence.is_relevant is True


def test_relevancy_preference_match_without_profile_match():
    children = [_child(1, "Nolan", "Frankland", "1")]
    evidence = compute_relevancy_evidence(
        event_text="Lunch menu notice for next week",
        target_grades=[],
        model_preference_match=False,
        children=children,
        preference_text="lunch menu",
    )
    assert evidence.preference_match is True
    assert evidence.is_relevant is True


def test_name_matching_avoids_substring_false_positive():
    children = [_child(1, "Ann", "Frankland", "1")]
    evidence = compute_relevancy_evidence(
        event_text="Annual fundraiser details",
        target_grades=[],
        model_preference_match=False,
        children=children,
        preference_text="",
    )
    assert evidence.name_match is False


def test_grade_normalization_and_ranges():
    assert normalize_grade("Grade 1") == "1"
    assert normalize_grade("G1") == "1"
    assert normalize_grade("JK") == "JK"
    grade_range = parse_grade_range("K-3")
    assert {"1", "2", "3"}.issubset(grade_range)
