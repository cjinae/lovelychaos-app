from datetime import datetime, timezone


def validate_candidate(event) -> dict:
    issues = []
    if not event.title:
        issues.append("missing_title")
    if not event.start_at or not event.end_at:
        issues.append("missing_time")
    if event.start_at and event.end_at and event.end_at < event.start_at:
        issues.append("end_before_start")
    if event.confidence < 0.6:
        issues.append("low_confidence")
    if event.start_at and event.start_at < datetime.now(timezone.utc):
        issues.append("event_in_past")
    return {"valid": len(issues) == 0, "issues": issues}
