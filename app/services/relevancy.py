from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata

from app.models import Child
from app.services.priorities import topic_matches_text


def _normalize_text(value: str) -> str:
    lowered = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii").lower()
    lowered = re.sub(r"[^\w\s-]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def _token_set(value: str) -> set[str]:
    normalized = _normalize_text(value)
    if not normalized:
        return set()
    return set(normalized.split())


def _stem_tokens(tokens: set[str]) -> set[str]:
    stems: set[str] = set()
    for token in tokens:
        stems.add(token)
        if token.endswith("s") and len(token) > 3:
            stems.add(token[:-1])
    return stems


def normalize_grade(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    compact = text.replace("grade", "").replace("g", "").strip()
    compact = compact.upper().replace(" ", "")
    if compact in {"JK", "JUNIORKINDERGARTEN"}:
        return "JK"
    if compact in {"SK", "SENIORKINDERGARTEN", "K", "KINDERGARTEN"}:
        return "SK"
    digits = re.findall(r"\d+", compact)
    if digits:
        return str(int(digits[0]))
    return compact


def parse_grade_range(value: str) -> set[str]:
    text = _normalize_text(value)
    if not text:
        return set()
    raw = text.upper().replace("GRADE", "").replace(" ", "")
    for sep in ("-", "TO"):
        if sep in raw:
            parts = raw.split(sep)
            if len(parts) == 2:
                left = normalize_grade(parts[0])
                right = normalize_grade(parts[1])
                grade_order = {"JK": -1, "SK": 0}
                reverse_grade_order = {-1: "JK", 0: "SK"}
                if left.isdigit() and right.isdigit():
                    start, end = int(left), int(right)
                    if start <= end:
                        return {str(i) for i in range(start, end + 1)}
                if left in grade_order and right.isdigit():
                    start, end = grade_order[left], int(right)
                    return {reverse_grade_order.get(i, str(i)) for i in range(start, end + 1)}
                if left.isdigit() and right in grade_order:
                    start, end = int(left), grade_order[right]
                    if start <= end:
                        return {reverse_grade_order.get(i, str(i)) for i in range(start, end + 1)}
    single = normalize_grade(raw)
    return {single} if single else set()


def _name_match_score(event_text: str, child_name: str) -> bool:
    if not event_text or not child_name:
        return False
    event_norm = f" {_normalize_text(event_text)} "
    name_tokens = [t for t in _normalize_text(child_name).split() if len(t) >= 2]
    if not name_tokens:
        return False
    for token in name_tokens:
        if f" {token} " in event_norm:
            return True
    full_name = _normalize_text(child_name)
    return f" {full_name} " in event_norm


def _school_match_score(event_text: str, school_name: str) -> bool:
    if not event_text or not school_name:
        return False
    event_tokens = _token_set(event_text)
    school_norm = _normalize_text(school_name)
    school_tokens = [t for t in school_norm.split() if len(t) >= 3]
    if school_norm and f" {school_norm} " in f" {_normalize_text(event_text)} ":
        return True
    if len(school_tokens) == 1:
        return school_tokens[0] in event_tokens
    if school_tokens:
        overlap = len(set(school_tokens) & event_tokens)
        return overlap >= max(1, len(school_tokens) - 1)
    return False


@dataclass
class RelevancyEvidence:
    name_match: bool
    name_child_ids: list[int]
    teacher_match: bool
    teacher_child_ids: list[int]
    school_match: bool
    school_child_ids: list[int]
    grade_match: bool
    grade_child_ids: list[int]
    preference_match: bool

    @property
    def is_relevant(self) -> bool:
        return self.name_match or self.teacher_match or self.school_match or self.grade_match or self.preference_match

    def as_dict(self) -> dict:
        return {
            "name_match": self.name_match,
            "name_child_ids": self.name_child_ids,
            "teacher_match": self.teacher_match,
            "teacher_child_ids": self.teacher_child_ids,
            "school_match": self.school_match,
            "school_child_ids": self.school_child_ids,
            "grade_match": self.grade_match,
            "grade_child_ids": self.grade_child_ids,
            "preference_match": self.preference_match,
        }


def _teacher_contact_matches(child: Child, sender_email: str, sender_display_name: str) -> bool:
    contacts = list(getattr(child, "teacher_contacts", []) or [])
    if not contacts:
        return False

    sender_email_norm = _normalize_text(sender_email)
    sender_name_norm = _normalize_text(sender_display_name)
    for contact in contacts:
        if str(getattr(contact, "status", "active") or "active").strip().lower() != "active":
            continue
        contact_email = _normalize_text(getattr(contact, "teacher_email", ""))
        contact_name = _normalize_text(getattr(contact, "teacher_name", ""))
        if sender_email_norm and contact_email and sender_email_norm == contact_email:
            return True
        if sender_name_norm and contact_name and sender_name_norm == contact_name:
            return True
    return False


def compute_relevancy_evidence(
    event_text: str,
    target_grades: list[str],
    model_preference_match: bool,
    children: list[Child],
    positive_preference_topics: list[str],
    sender_email: str = "",
    sender_display_name: str = "",
    target_scope: str = "unknown",
) -> RelevancyEvidence:
    event_norm = _normalize_text(event_text)

    name_ids: list[int] = []
    teacher_ids: list[int] = []
    school_ids: list[int] = []
    grade_ids: list[int] = []

    target_grade_set: set[str] = set()
    for grade in target_grades:
        target_grade_set.update(parse_grade_range(grade))

    for child in children:
        if _name_match_score(event_norm, child.name):
            name_ids.append(child.id)
        child_grade = normalize_grade(child.grade)
        if child_grade and child_grade in target_grade_set:
            grade_ids.append(child.id)
        if not target_grade_set and child_grade and child_grade in event_norm.split():
            grade_ids.append(child.id)
        grade_conflict = bool(target_grade_set) and bool(child_grade) and child_grade not in target_grade_set
        if (
            target_scope != "school_global"
            and not grade_conflict
            and _teacher_contact_matches(child, sender_email, sender_display_name)
        ):
            teacher_ids.append(child.id)
        if _school_match_score(event_norm, child.school_name):
            school_ids.append(child.id)

    preference_match = any(topic_matches_text(topic, event_text) for topic in positive_preference_topics)
    if not preference_match and not positive_preference_topics:
        preference_match = bool(model_preference_match)

    return RelevancyEvidence(
        name_match=bool(name_ids),
        name_child_ids=sorted(set(name_ids)),
        teacher_match=bool(teacher_ids),
        teacher_child_ids=sorted(set(teacher_ids)),
        school_match=bool(school_ids),
        school_child_ids=sorted(set(school_ids)),
        grade_match=bool(grade_ids),
        grade_child_ids=sorted(set(grade_ids)),
        preference_match=preference_match,
    )
