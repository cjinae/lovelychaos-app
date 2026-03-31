from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from app.services.brief_summary import (
    _normalize_compressed_summary,
    _absolute_date_from_text,
    _prune_redundant_dated_candidates,
    _summary_text_for_event,
    _upgrade_rendered_summary_with_candidates,
    build_brief_summary,
)
from app.services.content_analysis import AnalysisSection
from app.services.llm import ExtractedEvent


class _FailingSummaryEngine:
    def extract_summary_candidates(self, _summary_context):
        raise RuntimeError("force deterministic fallback")

    def compress_summary(self, _summary_context):
        raise RuntimeError("force deterministic fallback")


class _CompressionDropEngine:
    def extract_summary_candidates(self, summary_context):
        return {
            "title": summary_context["title_hint"],
            "candidates": list(summary_context["fallback_candidates"]),
            "notes": [],
            "missing_requested_topics": [],
        }

    def compress_summary(self, summary_context):
        important_info = [
            item
            for item in list(summary_context["candidates"] or [])
            if item.get("consolidated_priority") == "important"
        ]
        return {
            "title": summary_context["title_hint"],
            "important_info": important_info[:1],
            "other_dates": [],
            "other_topics": [],
            "missing_requested_topics": [],
            "notes": [],
        }


class _KnowledgeCaptureEngine:
    def __init__(self):
        self.extract_context = None
        self.compress_context = None

    def extract_summary_candidates(self, summary_context):
        self.extract_context = summary_context
        return {
            "title": summary_context["title_hint"],
            "candidates": list(summary_context["fallback_candidates"]),
            "notes": [],
            "missing_requested_topics": list(summary_context["missing_requested_topics"]),
        }

    def compress_summary(self, summary_context):
        self.compress_context = summary_context
        return {
            "title": summary_context["title_hint"],
            "important_info": [],
            "other_dates": [],
            "other_topics": [],
            "missing_requested_topics": [],
            "notes": [],
        }


class _OverlappingCompressionEngine:
    def extract_summary_candidates(self, summary_context):
        return {
            "title": summary_context["title_hint"],
            "candidates": list(summary_context["fallback_candidates"]),
            "notes": [],
            "missing_requested_topics": [],
        }

    def compress_summary(self, summary_context):
        return {
            "title": summary_context["title_hint"],
            "important_info": [],
            "other_dates": [],
            "other_topics": [
                {
                    "text": "Sep 25: Terry Fox Walk, Run, Roll (1:00 PM to 2:00 PM)",
                    "source_refs": ["event:Terry Fox Walk, Run, Roll"],
                    "applies_to": [],
                    "date_sort_key": "2025-09-25T17:00:00+00:00",
                },
                {
                    "text": "Sep 25: Terry Fox Walk/Run/Roll at 1 pm at Withrow Park; rain date Sept 29; $3,000 fundraising goal.",
                    "source_refs": ["event:Terry Fox Run, Walk, Roll", "section:23"],
                    "applies_to": [],
                    "date_sort_key": "2025-09-25T17:00:00+00:00",
                },
            ],
            "missing_requested_topics": [],
            "notes": [],
        }


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
    assert "\n\nImportant Info\n" in result.rendered_message
    assert "- Oct 7: Open House with Curriculum sharing (6:00 PM to 7:00 PM)" in result.rendered_message
    assert "- Oct 15 & 29: Pizza Lunches" in result.rendered_message
    assert "Let me know if you want me to add any of these to the calendar" in result.rendered_message
    assert "Hello wonderful families" not in result.rendered_message
    assert result.missing_requested_topics == []
    assert audit_payload["prefilter"]["kept_sections"][0]["label"] == "UPCOMING DATES"
    assert audit_payload["prefilter"]["dropped_sections"][0]["label"] == "Greeting"


def test_build_brief_summary_prepends_assistant_intro_and_uses_document_topics():
    engine = _FailingSummaryEngine()

    result, audit_payload = build_brief_summary(
        engine=engine,
        subject="Frankland Family Math Night Recap",
        timezone_name="America/Toronto",
        household_preferences="",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        children=[],
        extracted_events=[],
        per_event_outcomes=[],
        sections=[],
        analysis_text="Family Math Night recap and slideshow links were shared.",
        chunk_notes=[],
        document_understanding={
            "document_kind": "recap",
            "overall_intent": "informational",
            "assistant_summary": "This looks like a recap with resources to review later.",
            "assistant_intro": "This update mostly looks informational, but I pulled out the key topic below.",
            "actionable_topics": [],
            "informational_topics": [
                {
                    "title": "Family Math Night 2026 recap/feedback",
                    "why_it_matters": "The school shared recap resources and feedback follow-up.",
                    "action_hint": None,
                    "timing_hint": None,
                    "scope_hint": "school_global",
                }
            ],
            "routing_hints": {
                "recap_like": True,
                "resource_share_like": True,
                "contains_calendar_relevant_items": False,
            },
            "notes": ["No new date or time was found in the packet."],
        },
    )

    assert result.assistant_intro.startswith("This update mostly looks informational")
    assert result.assistant_intro in result.rendered_message
    assert "Family Math Night 2026 recap/feedback" in result.rendered_message
    assert audit_payload["input_context"]["document_understanding"]["document_kind"] == "recap"


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

    assert result.important_info[0].text.startswith("Oct 16: Gr 1 Riverdale classic cross country race")


def test_summary_text_for_single_day_all_day_event_omits_phantom_evening_times():
    text = _summary_text_for_event(
        {
            "title": "PA Day",
            "start_at": "2025-09-26T04:00:00+00:00",
            "end_at": "2025-09-27T04:00:00+00:00",
            "applies_to": [],
        },
        "America/Toronto",
    )

    assert text == "Sep 26: PA Day"


def test_build_brief_summary_uses_informational_footer_for_fyi_only_updates():
    engine = _FailingSummaryEngine()
    children = [SimpleNamespace(name="Nolan", school_name="Frankland", grade="1")]

    result, _ = build_brief_summary(
        engine=engine,
        subject="Room 106 Update",
        timezone_name="America/Toronto",
        household_preferences="",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        children=children,
        extracted_events=[],
        per_event_outcomes=[],
        sections=[
            AnalysisSection(
                index=1,
                source_kind="email_body",
                section_kind="narrative",
                label="Reminder",
                priority_score=60,
                text="Safe arrival still applies for absences tomorrow.",
            )
        ],
        analysis_text="Safe arrival still applies for absences tomorrow.",
        chunk_notes=[],
        informational_only=True,
    )

    assert "Let me know if you want me to add any of these to the calendar" not in result.rendered_message


def test_relative_date_resolution_uses_reference_datetime_hint():
    zone = ZoneInfo("America/Toronto")
    reference_dt = datetime(2026, 3, 1, 18, 53, tzinfo=zone)

    tomorrow = _absolute_date_from_text("Tomorrow is Day 3 - no swim.", zone, reference_dt)
    thursday = _absolute_date_from_text("Hot lunch for Thursday.", zone, reference_dt)

    assert tomorrow == datetime(2026, 3, 2, 0, 0, tzinfo=zone)
    assert thursday == datetime(2026, 3, 5, 0, 0, tzinfo=zone)


def test_relative_date_resolution_does_not_treat_weekday_labels_as_dates():
    zone = ZoneInfo("America/Toronto")
    reference_dt = datetime(2026, 3, 18, 18, 53, tzinfo=zone)

    friday_lunch = _absolute_date_from_text("Students Leaving School Property During Lunch & Grade 6 Friday Lunch", zone, reference_dt)

    assert friday_lunch is None


def test_build_brief_summary_suppresses_parsed_negative_topics_but_keeps_system_defaults():
    engine = _FailingSummaryEngine()
    children = [SimpleNamespace(name="Nolan", school_name="Frankland", grade="1")]
    events = [
        ExtractedEvent(
            title="Tamil Heritage Month",
            start_at=_utc(2026, 1, 1),
            end_at=_utc(2026, 1, 31, 4, 0),
            category="school",
            confidence=0.92,
            target_scope="school_global",
            model_reason="heritage month recognition",
        ),
        ExtractedEvent(
            title="PA Day",
            start_at=_utc(2026, 2, 13),
            end_at=_utc(2026, 2, 13),
            category="school_closure",
            confidence=0.95,
            target_scope="school_global",
            model_reason="school closure",
        ),
    ]

    result, audit_payload = build_brief_summary(
        engine=engine,
        subject="Frankland Newsletter",
        timezone_name="America/Toronto",
        household_preferences="I don't care about cultural days",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        suppressed_priority_topics=["Heritage Months"],
        children=children,
        extracted_events=events,
        per_event_outcomes=[
            {"execution_disposition": "informational_item"},
            {"execution_disposition": "informational_item"},
        ],
        sections=[],
        analysis_text="Tamil Heritage Month is recognized in January. PA Day is on February 13.",
        chunk_notes=[],
    )

    assert all("Heritage" not in item.text for item in result.other_topics)
    assert any("PA Day" in item.text for item in result.important_info)
    assert audit_payload["input_context"]["suppressed_priority_topics"] == ["Heritage Months"]


def test_build_brief_summary_uses_routed_preference_matches_for_custom_topics():
    engine = _FailingSummaryEngine()
    events = [
        ExtractedEvent(
            title="Brick Labs club registration opens",
            start_at=_utc(2026, 4, 10),
            end_at=_utc(2026, 4, 10),
            category="school",
            confidence=0.92,
            target_scope="school_global",
            model_reason="after-school enrichment registration",
        )
    ]

    result, audit_payload = build_brief_summary(
        engine=engine,
        subject="Frankland Newsletter",
        timezone_name="America/Toronto",
        household_preferences="Bricklabs",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=["Bricklabs"],
        suppressed_priority_topics=[],
        children=[],
        extracted_events=events,
        per_event_outcomes=[
            {
                "execution_disposition": "followup_available",
                "relevancy_evidence": {"preference_match": True},
                "suppressed_match": False,
                "matched_positive_topics": ["Bricklabs"],
                "matched_suppressed_topics": [],
            }
        ],
        sections=[],
        analysis_text="Brick Labs club registration opens this week.",
        chunk_notes=[],
    )

    assert any("Brick Labs club registration opens" in item.text for item in result.other_topics + result.important_info + result.other_dates)
    assert audit_payload["consolidated_priority_items"][0]["matched_user_priorities"] == ["Bricklabs"]


def test_build_brief_summary_does_not_promote_non_matching_grade_specific_events():
    engine = _FailingSummaryEngine()
    children = [SimpleNamespace(name="Nolan", school_name="Frankland", grade="1")]
    events = [
        ExtractedEvent(
            title="Grade 5 girls volleyball tournament",
            start_at=_utc(2026, 3, 26),
            end_at=_utc(2026, 3, 26),
            category="school",
            confidence=0.95,
            target_scope="grade_specific",
            target_grades=["5"],
            model_reason="Grade 5 tournament at Frankland",
        )
    ]

    result, audit_payload = build_brief_summary(
        engine=engine,
        subject="Frankland Newsletter",
        timezone_name="America/Toronto",
        household_preferences="",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        children=children,
        extracted_events=events,
        per_event_outcomes=[],
        sections=[],
        analysis_text="March 26 Grade 5 girls volleyball tournament at Frankland.",
        chunk_notes=[],
    )

    assert result.important_info == []
    assert audit_payload["consolidated_priority_items"] == []


def test_build_brief_summary_rescues_single_event_email_sections():
    engine = _FailingSummaryEngine()
    children = [
        SimpleNamespace(name="Nolan", school_name="Frankland Community School Junior", grade="1"),
        SimpleNamespace(name="Jayden", school_name="Frankland Community School Junior", grade="JK"),
    ]
    sections = [
        AnalysisSection(
            index=1,
            source_kind="email_body",
            section_kind="narrative",
            label="Original email date: Tue, Mar 10, 2026 at 8:17 AM",
            priority_score=45,
            text="Original email date: Tue, Mar 10, 2026 at 8:17 AM",
        ),
        AnalysisSection(
            index=2,
            source_kind="email_body",
            section_kind="narrative",
            label="Please Join Us for FAMILY MATH NIGHT",
            priority_score=10,
            text="Please Join Us for FAMILY MATH NIGHT",
        ),
        AnalysisSection(
            index=3,
            source_kind="email_body",
            section_kind="heading_block",
            label="Please save the date: Wednesday, March 11th, 2026",
            priority_score=55,
            text="Please save the date: Wednesday, March 11th, 2026",
        ),
        AnalysisSection(
            index=4,
            source_kind="email_body",
            section_kind="bullet_block",
            label="Doors will open at 5:20 pm",
            priority_score=65,
            text=(
                "Doors will open at 5:20 pm, and we will begin promptly with a welcome "
                "and a brief presentation at 5:30 pm. The evening will end at 6:30 pm."
            ),
        ),
    ]

    result, audit_payload = build_brief_summary(
        engine=engine,
        subject="Fwd: Family Math Night Tomorrow!",
        timezone_name="America/Toronto",
        household_preferences="School closures, PA Days, Pizza Day, Swim schedule",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=["School closures", "PA Days", "Pizza Day", "Swim schedule"],
        children=children,
        extracted_events=[],
        per_event_outcomes=[],
        sections=sections,
        analysis_text=(
            "Original email date: Tue, Mar 10, 2026 at 8:17 AM\n\n"
            "Please Join Us for FAMILY MATH NIGHT\n\n"
            "Please save the date: Wednesday, March 11th, 2026\n\n"
            "Doors will open at 5:20 pm, and we will begin promptly with a welcome and a brief presentation at 5:30 pm. "
            "The evening will end at 6:30 pm."
        ),
        chunk_notes=[],
    )

    assert "- Mar 11: Family Math Night (5:30 PM to 6:30 PM)" in result.rendered_message
    assert "I found a school update but couldn't extract a clean summary" not in result.rendered_message
    assert audit_payload["prefilter"]["kept_sections"]


def test_build_brief_summary_surfaces_dated_lines_from_kept_sections():
    engine = _FailingSummaryEngine()
    sections = [
        AnalysisSection(
            index=1,
            source_kind="attachment_text",
            section_kind="heading_block",
            label="Page 5:",
            priority_score=55,
            text=(
                "SPRING HOT LUNCH ORDERING DEADLINE  - Wednesday, April 1st.\n"
                "Ordering is online at www.thelunchmom.com."
            ),
        )
    ]

    result, audit_payload = build_brief_summary(
        engine=engine,
        subject="Frankland Newsletter",
        timezone_name="America/Toronto",
        household_preferences="",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        children=[],
        extracted_events=[],
        per_event_outcomes=[],
        sections=sections,
        analysis_text="SPRING HOT LUNCH ORDERING DEADLINE  - Wednesday, April 1st.",
        chunk_notes=[],
    )

    assert "- Apr 1: Spring Hot Lunch Ordering Deadline" in result.rendered_message
    assert any(
        item["text"] == "Apr 1: Spring Hot Lunch Ordering Deadline"
        for item in audit_payload["consolidated_priority_items"]
    )


def test_prune_redundant_dated_candidates_prefers_richer_non_section_match():
    candidates = [
        {
            "text": "Mar 19: Registration deadline",
            "consolidated_priority": "mentioned",
            "matched_system_defaults": [],
            "matched_user_priorities": [],
            "source_refs": ["section:13"],
            "applies_to": [],
            "date_sort_key": "2026-03-19T00:00:00+00:00",
            "has_date": True,
            "reason": "dated_section_match",
        },
        {
            "text": "Mar 19: Extra Ed Science Club registration deadline",
            "consolidated_priority": "mentioned",
            "matched_system_defaults": [],
            "matched_user_priorities": [],
            "source_refs": ["section:13", "section:12"],
            "applies_to": [],
            "date_sort_key": "2026-03-19T00:00:00+00:00",
            "has_date": True,
            "reason": "Concrete registration deadline for an optional school program should be included as a brief mention.",
        },
    ]

    pruned = _prune_redundant_dated_candidates(candidates)

    assert [item["text"] for item in pruned] == ["Mar 19: Extra Ed Science Club registration deadline"]


def test_build_brief_summary_collapses_overlapping_same_day_summary_lines():
    engine = _OverlappingCompressionEngine()

    result, _ = build_brief_summary(
        engine=engine,
        subject="Fwd: Frankland CS Newsletter - Sept. 8, 2025",
        timezone_name="America/Toronto",
        household_preferences="",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        children=[],
        extracted_events=[],
        per_event_outcomes=[],
        sections=[],
        analysis_text="Terry Fox Walk/Run/Roll on Sep 25 at 1 pm at Withrow Park. Rain date Sept 29.",
        chunk_notes=[],
    )

    assert result.rendered_message.count("Terry Fox") == 1
    assert "rain date Sept 29" in result.rendered_message


def test_build_brief_summary_normalizes_messy_same_day_model_output():
    class _MessyCompressionEngine:
        def extract_summary_candidates(self, summary_context):
            return {
                "title": summary_context["title_hint"],
                "candidates": list(summary_context["fallback_candidates"]),
                "notes": [],
                "missing_requested_topics": [],
            }

        def compress_summary(self, summary_context):
            return {
                "title": summary_context["title_hint"],
                "important_info": [],
                "other_dates": [
                    {
                        "text": "Sep 10: School Council Meeting (6:00 PM to 7:00 PM)",
                        "source_refs": ["event:School Council Meeting", "section:10"],
                        "applies_to": [],
                        "date_sort_key": "2025-09-10T22:00:00+00:00",
                    },
                    {
                        "text": "Sep 10: School Council Meeting 6 00 to 7 00 pm in the Library (7:00 PM)",
                        "source_refs": ["section:10"],
                        "applies_to": [],
                        "date_sort_key": "2025-09-10T23:00:00+00:00",
                    },
                    {
                        "text": "Sep 25: Terry Fox Walk Run Roll - Rain is on Sept. 29",
                        "source_refs": ["section:10"],
                        "applies_to": [],
                        "date_sort_key": "2025-09-25T04:00:00+00:00",
                    },
                    {
                        "text": "Sep 25: The Frankland Terry Fox Run is on",
                        "source_refs": ["section:23"],
                        "applies_to": [],
                        "date_sort_key": "2025-09-25T04:00:00+00:00",
                    },
                    {
                        "text": "Sep 25: Terry Fox Run, Walk, Roll (1:00 PM, Withrow Park; rain date Sep 29)",
                        "source_refs": ["event:Terry Fox Run, Walk, Roll", "section:23"],
                        "applies_to": [],
                        "date_sort_key": "2025-09-25T17:00:00+00:00",
                    },
                    {
                        "text": "Sep 29: Over the next few weeks students will be working on a school wide art project. Students in",
                        "source_refs": ["section:23"],
                        "applies_to": [],
                        "date_sort_key": "2025-09-29T04:00:00+00:00",
                    },
                ],
                "missing_requested_topics": [],
                "notes": [],
            }

    result, _ = build_brief_summary(
        engine=_MessyCompressionEngine(),
        subject="Fwd: Frankland CS Newsletter - Sept. 8, 2025",
        timezone_name="America/Toronto",
        household_preferences="",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        children=[],
        extracted_events=[],
        per_event_outcomes=[],
        sections=[],
        analysis_text="Frankland newsletter summary content.",
        chunk_notes=[],
    )

    assert result.rendered_message.count("School Council Meeting") == 1
    assert result.rendered_message.count("Terry Fox") == 1
    assert "6 00 to 7 00 pm" not in result.rendered_message
    assert "Rain is on Sept. 29" not in result.rendered_message
    assert "Over the next few weeks" not in result.rendered_message


def test_upgrade_rendered_summary_with_candidates_prefers_stronger_source_backed_lines():
    payload = {
        "title": "Frankland CS Update (Sept 8)",
        "important_info": [
            {
                "text": "Sep 26: PA Day",
                "source_refs": ["event:PA Day"],
                "applies_to": [],
                "date_sort_key": "2025-09-26T04:00:00+00:00",
            }
        ],
        "other_dates": [
            {
                "text": "Sep 10: School Council Meeting 6 00 to 7 00 pm in the Library (7:00 PM)",
                "source_refs": ["section:10"],
                "applies_to": [],
                "date_sort_key": "2025-09-10T23:00:00+00:00",
            },
            {
                "text": "Sep 25: Terry Fox Walk Run Roll - Rain is on Sept. 29",
                "source_refs": ["section:10"],
                "applies_to": [],
                "date_sort_key": "2025-09-25T04:00:00+00:00",
            },
            {
                "text": "Sep 26: Pa Day (no school for students)",
                "source_refs": ["section:10"],
                "applies_to": [],
                "date_sort_key": "2025-09-26T04:00:00+00:00",
            },
            {
                "text": "Sep 29: Over the next few weeks students will be working on a school wide art project. Students in",
                "source_refs": ["section:23"],
                "applies_to": [],
                "date_sort_key": "2025-09-29T04:00:00+00:00",
            },
        ],
        "missing_requested_topics": [],
        "notes": [],
    }
    candidates = [
        {
            "text": "Sep 10: School Council Meeting (6:00 PM to 7:00 PM)",
            "source_refs": ["event:School Council Meeting"],
            "applies_to": [],
            "date_sort_key": "2025-09-10T22:00:00+00:00",
            "has_date": True,
        },
        {
            "text": "Sep 25: Terry Fox Run, Walk, Roll (1:00 PM, Withrow Park; rain date Sep 29)",
            "source_refs": ["event:Terry Fox Run, Walk, Roll", "section:23"],
            "applies_to": [],
            "date_sort_key": "2025-09-25T17:00:00+00:00",
            "has_date": True,
        },
    ]

    upgraded = _upgrade_rendered_summary_with_candidates(payload, candidates, "America/Toronto")
    normalized = _normalize_compressed_summary(upgraded, "America/Toronto")
    rendered_lines = [item["text"] for item in normalized["other_dates"]]

    assert "Sep 10: School Council Meeting (6:00 PM to 7:00 PM)" in rendered_lines
    assert "Sep 25: Terry Fox Run, Walk, Roll (1:00 PM, Withrow Park; rain date Sep 29)" in rendered_lines
    assert not any("6 00 to 7 00 pm" in line for line in rendered_lines)
    assert not any("Rain is on Sept. 29" in line for line in rendered_lines)
    assert not any("Over the next few weeks" in line for line in rendered_lines)
    all_rendered = [item["text"] for item in normalized["important_info"] + normalized["other_dates"] + normalized["other_topics"]]
    assert not any("Pa Day (no school for students)" in line for line in all_rendered)


def test_build_brief_summary_includes_school_knowledge_in_context_and_audit():
    engine = _KnowledgeCaptureEngine()

    result, audit_payload = build_brief_summary(
        engine=engine,
        subject="Family Movie Night",
        timezone_name="America/Toronto",
        household_preferences="",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=[],
        children=[],
        extracted_events=[],
        per_event_outcomes=[],
        sections=[],
        analysis_text=(
            "UPCOMING DATES\n"
            "Thursday January 15 - Family Movie Night in the school gym.\n"
            "Doors open at 5:45 PM and the movie starts at 6:00 PM."
        ),
        chunk_notes=[],
    )

    assert result.title == "Family Movie Night"
    assert engine.extract_context is not None
    assert "parent engagement" in engine.extract_context["domain_taxonomy_hints"]
    assert "family event" in engine.extract_context["matched_event_types"]
    assert engine.extract_context["retrieved_examples"][0]["entry_id"] == "movie-night"
    assert engine.extract_context["retrieved_examples"][0]["snippet"]
    assert len(engine.extract_context["retrieved_examples"]) <= 3
    assert len(engine.extract_context["retrieved_examples"][0]["snippet"]) <= 220
    assert engine.compress_context is not None
    assert audit_payload["knowledge_retrieval"]["matches"][0]["entry_id"] == "movie-night"
    assert "retrieved_examples" not in audit_payload["knowledge_retrieval"]


def test_build_brief_summary_limits_kept_sections_and_notes_for_prompt_context():
    engine = _KnowledgeCaptureEngine()
    sections = [
        AnalysisSection(
            index=index,
            source_kind="attachment_text",
            section_kind="schedule",
            label=f"Section {index}",
            priority_score=100 - index,
            text=(f"Registration deadline for spring club {index}. " * 80).strip(),
        )
        for index in range(1, 9)
    ]

    build_brief_summary(
        engine=engine,
        subject="Frankland spring roundup",
        timezone_name="America/Toronto",
        household_preferences="bricklabs, school lunches",
        system_defaults={"school_closures": True, "grade_relevant": True},
        user_priority_topics=["Bricklabs", "School Lunches"],
        children=[],
        extracted_events=[],
        per_event_outcomes=[],
        sections=sections,
        analysis_text="\n\n".join(section.text for section in sections),
        chunk_notes=["note 1", "note 2", "note 3", "note 4", "note 5"],
        document_understanding={
            "assistant_summary": "Summary note",
            "assistant_intro": "Short intro",
            "actionable_topics": [],
            "informational_topics": [],
            "routing_hints": {},
            "notes": ["doc note 1", "doc note 2", "doc note 3"],
        },
    )

    assert engine.extract_context is not None
    assert len(engine.extract_context["kept_sections"]) == 6
    assert all(len(section["text"]) <= 900 for section in engine.extract_context["kept_sections"])
    assert len(engine.extract_context["notes"]) == 4
