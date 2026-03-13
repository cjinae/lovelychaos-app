from datetime import datetime, timedelta, timezone

from app.services.llm import ExtractedEvent
from app.services.validation import validate_candidate


def test_validation_rejects_low_confidence():
    event = ExtractedEvent(
        title="x",
        start_at=datetime.now(timezone.utc) + timedelta(days=1),
        end_at=datetime.now(timezone.utc) + timedelta(days=1, hours=1),
        category="general",
        confidence=0.2,
    )
    result = validate_candidate(event)
    assert result["valid"] is False
    assert "low_confidence" in result["issues"]


def test_validation_rejects_temporal_conflict():
    start = datetime.now(timezone.utc) + timedelta(days=1)
    event = ExtractedEvent(title="x", start_at=start, end_at=start - timedelta(hours=1), category="general", confidence=0.9)
    result = validate_candidate(event)
    assert result["valid"] is False
    assert "end_before_start" in result["issues"]
