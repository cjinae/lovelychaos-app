from datetime import datetime, timezone
from types import SimpleNamespace

from app.services.brief_summary import build_brief_summary
from app.services.content_analysis import AnalysisSection
from app.services.llm import ExtractedEvent


class _FailingSummaryEngine:
    def extract_summary_candidates(self, _summary_context):
        raise RuntimeError("force deterministic fallback")

    def compress_summary(self, _summary_context):
        raise RuntimeError("force deterministic fallback")


def _utc(year: int, month: int, day: int, hour: int = 4, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_build_brief_summary_renders_compact_parent_filtered_output():
    engine = _FailingSummaryEngine()
    children = [
        SimpleNamespace(name="Nolan", school_name="Frankland Community School Junior", grade="1"),
        SimpleNamespace(name="Jayden", school_name="Frankland Community School Junior", grade="JK"),
    ]
    events = [
        ExtractedEvent(
            title="Open House with Curriculum sharing",
            start_at=_utc(2025, 10, 7, 22, 0),
            end_at=_utc(2025, 10, 7, 23, 0),
            category="school",
            confidence=0.98,
            target_scope="school_global",
            mentioned_schools=["Frankland Community School Junior"],
            model_reason="explicit 6 PM event",
        ),
        ExtractedEvent(
            title="Pizza Lunch",
            start_at=_utc(2025, 10, 15),
            end_at=_utc(2025, 10, 15),
            category="school",
            confidence=0.96,
            target_scope="child_specific",
            mentioned_names=["Nolan"],
            preference_match=True,
            model_reason="pizza day",
        ),
        ExtractedEvent(
            title="Pizza Lunch",
            start_at=_utc(2025, 10, 29),
            end_at=_utc(2025, 10, 29),
            category="school",
            confidence=0.96,
            target_scope="child_specific",
            mentioned_names=["Nolan"],
            preference_match=True,
            model_reason="pizza day",
        ),
        ExtractedEvent(
            title="Tamil Heritage Month",
            start_at=_utc(2026, 1, 1),
            end_at=_utc(2026, 1, 31, 4, 0),
            category="school",
            confidence=0.9,
            target_scope="school_global",
            model_reason="awareness month",
        ),
    ]
    sections = [
        AnalysisSection(
            index=1,
            source_kind="attachment_text",
            section_kind="schedule",
            label="UPCOMING DATES",
            priority_score=120,
            text="October 7 Open House. October 15 and 29 Pizza Lunch.",
        ),
        AnalysisSection(
            index=2,
            source_kind="email_body",
            section_kind="narrative",
            label="Greeting",
            priority_score=10,
            text="Hello wonderful families, thank you for your incredible support.",
        ),
    ]

    result, audit_payload = build_brief_summary(
        engine=engine,
        subject="Fwd: Sunday, October 5/25 Frankland Newsletter",
        timezone_name="America/Toronto",
        household_preferences="Camp",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=["Open House", "Pizza Lunch"],
        children=children,
        extracted_events=events,
        per_event_outcomes=[],
        sections=sections,
        analysis_text=(
            "UPCOMING DATES\nOctober 7 Open House with Curriculum sharing 6:00 PM.\n"
            "October 15 and 29 Pizza Lunch.\nTamil Heritage Month is recognized this January.\n"
            "Safe arrival procedures remain in effect."
        ),
        chunk_notes=[],
    )

    assert result.title == "Frankland CS Update (October 5)"
    assert result.rendered_message.startswith("Frankland CS Update (October 5)")
    assert "\n\nImportant Dates\n" in result.rendered_message
    assert "- Oct 7: Open House with Curriculum sharing (6:00 PM to 7:00 PM)" in result.rendered_message
    assert "- Oct 15 & 29: Pizza Lunches" in result.rendered_message
    assert "\n\nOther Logistics / Topics Mentioned\n" in result.rendered_message
    assert "- Heritage months mentioned" in result.rendered_message
    assert "- Safe arrival/absence procedures" in result.rendered_message
    assert "Let me know if you want me to add any of these to the calendar" in result.rendered_message
    assert "Hello wonderful families" not in result.rendered_message
    assert result.missing_requested_topics == []
    assert audit_payload["prefilter"]["kept_sections"][0]["label"] == "UPCOMING DATES"
    assert audit_payload["prefilter"]["dropped_sections"][0]["label"] == "Greeting"


def test_build_brief_summary_promotes_grade_specific_events():
    engine = _FailingSummaryEngine()
    children = [SimpleNamespace(name="Nolan", school_name="Frankland", grade="1")]
    events = [
        ExtractedEvent(
            title="Riverdale classic cross country race",
            start_at=_utc(2025, 10, 16, 17, 0),
            end_at=_utc(2025, 10, 16, 18, 0),
            category="school",
            confidence=0.95,
            target_scope="grade_specific",
            target_grades=["1", "2"],
            model_reason="Grades 1 and 2 at 1 PM",
        )
    ]

    result, _ = build_brief_summary(
        engine=engine,
        subject="Frankland Newsletter",
        timezone_name="America/Toronto",
        household_preferences="Cross country",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        children=children,
        extracted_events=events,
        per_event_outcomes=[],
        sections=[],
        analysis_text="October 16 Riverdale classic cross country race for Grades 1 and 2 at 1:00 pm.",
        chunk_notes=[],
    )

    assert result.important_dates[0].text.startswith("Oct 16: Gr 1 Riverdale classic cross country race")
