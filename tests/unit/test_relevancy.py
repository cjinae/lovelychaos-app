from __future__ import annotations

from app.models import Child, TeacherContact
from app.services.relevancy import compute_relevancy_evidence, normalize_grade, parse_grade_range


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
        positive_preference_topics=["School Closures"],
    )
    assert evidence.name_match is True
    assert evidence.school_match is True
    assert evidence.grade_match is True
    assert evidence.is_relevant is True


def test_relevancy_preference_match_without_profile_match():
    children = [_child(1, "Nolan", "Frankland", "1")]
    evidence = compute_relevancy_evidence(
        event_text="Hot lunch menu notice for next week",
        target_grades=[],
        model_preference_match=False,
        children=children,
        positive_preference_topics=["Hot Lunch Programs"],
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
        positive_preference_topics=[],
    )
    assert evidence.name_match is False


def test_grade_normalization_and_ranges():
    assert normalize_grade("Grade 1") == "1"
    assert normalize_grade("G1") == "1"
    assert normalize_grade("JK") == "JK"
    grade_range = parse_grade_range("K-3")
    assert {"1", "2", "3"}.issubset(grade_range)


def test_relevancy_matches_linked_teacher_for_classroom_email():
    children = [
        _child(
            1,
            "Nolan",
            "Frankland Community School Junior",
            "1",
            teachers=[("Helen Poulos", "helen.poulos@tdsb.on.ca")],
        )
    ]

    evidence = compute_relevancy_evidence(
        event_text="Room 106 swim tomorrow for the class",
        target_grades=[],
        model_preference_match=False,
        children=children,
        positive_preference_topics=[],
        sender_email="helen.poulos@tdsb.on.ca",
        sender_display_name="Helen Poulos",
        target_scope="school_specific",
    )

    assert evidence.teacher_match is True
    assert evidence.teacher_child_ids == [1]
    assert evidence.is_relevant is True


def test_relevancy_does_not_use_teacher_match_for_school_global_event():
    children = [
        _child(
            1,
            "Nolan",
            "Frankland Community School Junior",
            "1",
            teachers=[("Helen Poulos", "helen.poulos@tdsb.on.ca")],
        )
    ]

    evidence = compute_relevancy_evidence(
        event_text="Pink Shirt Day for the whole school",
        target_grades=[],
        model_preference_match=False,
        children=children,
        positive_preference_topics=[],
        sender_email="helen.poulos@tdsb.on.ca",
        sender_display_name="Helen Poulos",
        target_scope="school_global",
    )

    assert evidence.teacher_match is False
