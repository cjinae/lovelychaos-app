from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata

from app.models import Child
from app.services.llm import ExtractedEvent
from app.services.relevancy import RelevancyEvidence, normalize_grade, parse_grade_range


AUTO_ADD_CLOSURE_TERMS = (
    "pa day",
    "school closed",
    "no school",
    "school closure",
    "holiday",
    "march break",
    "winter break",
    "mid winter break",
    "spring break",
    "family day",
    "good friday",
    "easter monday",
    "thanksgiving",
    "classes resume",
    "first day back",
    "resume after",
)
AUTO_ADD_PREFERENCE_TERMS = (
    "pizza",
    "swim",
    "lunch",
)
AUTO_ADD_BLOCKED_TERMS = (
    "volunteer",
    "setup shift",
    "cleanup shift",
    "event running shift",
    "drop off period",
    "drop-off period",
    "donation drop",
    "deadline",
    "registration",
    "application",
    "window",
    "portal",
    "forms",
    "form",
    "payment",
    "wristband",
    "council meeting",
    "school council",
    "meeting",
    "swap",
    "movie night",
    "open house",
    "boo bash",
    "family math night",
    "gardening program",
    "welcome for new students",
    "awareness",
    "heritage",
    "world ",
    "international ",
    "webinar",
    "virtual connect",
)


@dataclass
class AutoAddDecision:
    allow: bool
    reason: str


def evaluate_auto_add_candidate(
    event: ExtractedEvent,
    relevancy: RelevancyEvidence,
    children: list[Child],
) -> AutoAddDecision:
    if not event.start_at or not event.end_at:
        return AutoAddDecision(False, "missing_schedule_window")

    normalized = _normalize_text(
        " ".join(
            [
                event.title or "",
                event.category or "",
                event.model_reason or "",
                " ".join(event.target_grades or []),
            ]
        )
    )

    if _has_grade_mismatch(normalized, event, children, relevancy):
        return AutoAddDecision(False, "grade_mismatch")
    if event.target_scope == "child_specific" and not relevancy.name_match and not relevancy.grade_match:
        return AutoAddDecision(False, "child_scope_mismatch")
    if event.target_scope == "grade_specific" and not relevancy.grade_match:
        return AutoAddDecision(False, "grade_scope_mismatch")

    if any(term in normalized for term in AUTO_ADD_CLOSURE_TERMS):
        return AutoAddDecision(True, "closure_or_break")

    if any(term in normalized for term in AUTO_ADD_BLOCKED_TERMS):
        return AutoAddDecision(False, "optional_or_admin_event")

    if (relevancy.name_match or relevancy.grade_match) and any(term in normalized for term in AUTO_ADD_PREFERENCE_TERMS):
        return AutoAddDecision(True, "household_specific_preference_event")

    if relevancy.preference_match and any(term in normalized for term in AUTO_ADD_PREFERENCE_TERMS):
        return AutoAddDecision(True, "school_preference_event")

    return AutoAddDecision(False, "needs_confirmation")


def _normalize_text(value: str) -> str:
    lowered = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii").lower()
    lowered = re.sub(r"[^\w\s-]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _has_grade_mismatch(
    normalized_event_text: str,
    event: ExtractedEvent,
    children: list[Child],
    relevancy: RelevancyEvidence,
) -> bool:
    if relevancy.grade_match:
        return False

    child_grades = {normalize_grade(child.grade) for child in children if normalize_grade(child.grade)}
    referenced_grades: set[str] = set()
    for value in list(event.target_grades or []):
        referenced_grades.update(parse_grade_range(value))

    if not referenced_grades:
        referenced_grades.update(_extract_grade_tokens(normalized_event_text))

    if not referenced_grades:
        return False

    return referenced_grades.isdisjoint(child_grades)


def _extract_grade_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for match in re.finditer(r"\bgrade\s+(\d+|jk|sk|k)\b", text):
        tokens.add(normalize_grade(match.group(1)))
    for match in re.finditer(r"\bgr\s*(\d+)\b", text):
        tokens.add(normalize_grade(match.group(1)))
    return {token for token in tokens if token}
