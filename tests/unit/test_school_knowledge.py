from pathlib import Path

from app.services import school_knowledge
from app.services.llm import ExtractedEvent


def test_retrieve_knowledge_context_matches_movie_night_example():
    school_knowledge.clear_school_knowledge_cache()
    context = school_knowledge.retrieve_knowledge_context(
        subject="Family Movie Night",
        sections=[],
        analysis_text=(
            "UPCOMING DATES\n"
            "Thursday January 15 - Family Movie Night in the school gym.\n"
            "Doors open at 5:45 PM and the movie starts at 6:00 PM."
        ),
        extracted_events=[],
    )

    assert context.matches
    assert context.matches[0].entry_id == "movie-night"
    assert "parent engagement" in context.matched_topics
    assert "family event" in context.matched_event_types
    assert any("matched" in note for note in context.retrieval_notes)


def test_retrieve_knowledge_context_matches_health_notice_example():
    school_knowledge.clear_school_knowledge_cache()
    context = school_knowledge.retrieve_knowledge_context(
        subject="Head Lice Screening Day",
        sections=[],
        analysis_text=(
            "Monday November 10 - Head lice screening day.\n"
            "A registered nurse will screen all students and families may opt out."
        ),
        extracted_events=[],
    )

    assert context.matches
    assert context.matches[0].entry_id == "head-lice-screening"
    assert "health/safety" in context.matched_topics
    assert any(hint["commonness"] == "exceptional" for hint in context.commonness_hints)


def test_retrieve_knowledge_context_matches_reporting_example_from_event_titles():
    school_knowledge.clear_school_knowledge_cache()
    context = school_knowledge.retrieve_knowledge_context(
        subject="November Update",
        sections=[],
        analysis_text="Families can book interviews online.",
        extracted_events=[
            ExtractedEvent(
                title="Progress Reports and Parent-Teacher Interviews",
                start_at=None,
                end_at=None,
                category="school",
                confidence=0.95,
            )
        ],
    )

    assert context.matches
    assert context.matches[0].entry_id == "progress-reports-interviews"
    assert "academic reporting" in context.matched_topics
    assert "reporting deadline" in context.matched_event_types


def test_retrieve_knowledge_context_falls_back_cleanly_when_corpus_is_missing(monkeypatch, tmp_path):
    missing_path = tmp_path / "missing-corpus.json"
    monkeypatch.setattr(school_knowledge, "CORPUS_PATH", missing_path)
    school_knowledge.clear_school_knowledge_cache()

    context = school_knowledge.retrieve_knowledge_context(
        subject="Anything",
        sections=[],
        analysis_text="Anything",
        extracted_events=[],
    )

    assert context.matches == []
    assert context.retrieved_examples == []
    school_knowledge.clear_school_knowledge_cache()
