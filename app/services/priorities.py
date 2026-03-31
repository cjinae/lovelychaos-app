from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import PreferenceProfile, PreferenceRule


SYSTEM_DEFAULT_PRIORITY_CONFIG = {
    "school_closures": {
        "label": "School closures",
        "description": "PA Days, March Break, Winter Break, and school-closed holidays.",
        "examples": ["PA Day", "March Break", "Winter Break", "Stat holiday where school is closed"],
    },
    "grade_relevant": {
        "label": "Grade-relevant items",
        "description": "Events and info that match the grades of children in the household.",
        "examples": ["Grade 1 trip", "Primary concert", "JK classroom event"],
    },
    "report_cards": {
        "label": "Report cards",
        "description": "Report card distribution dates and progress reports.",
        "examples": ["Term 1 report cards", "Progress reports go home", "Report card day"],
    },
    "parent_teacher_interviews": {
        "label": "Parent-teacher interviews",
        "description": "Parent-teacher conferences, interviews, and meeting nights.",
        "examples": ["Parent-teacher conferences", "Interview evening", "Parent-teacher night"],
    },
}

PRESET_PRIORITY_TOPIC_CONFIG = (
    {
        "key": "pizza_days",
        "label": "Pizza Days",
        "aliases": ("pizza day", "pizza days", "pizza lunch", "pizza lunches"),
    },
    {
        "key": "hot_lunch_programs",
        "label": "Hot Lunch Programs",
        "aliases": ("hot lunch", "hot lunches", "hot lunch program", "hot lunch programs"),
    },
    {
        "key": "swim_days",
        "label": "Swim Days",
        "aliases": ("swim day", "swim days", "swimming", "swim schedule"),
    },
    {
        "key": "spirit_days",
        "label": "Spirit Days",
        "aliases": ("spirit day", "spirit days", "spirit wear day", "theme day", "theme days"),
    },
)

GENERAL_PRIORITY_TOPIC_CONFIG = (
    {
        "key": "heritage_months",
        "label": "Heritage Months",
        "aliases": (
            "heritage month",
            "heritage months",
            "awareness month",
            "awareness months",
            "cultural day",
            "cultural days",
            "cultural event",
            "cultural events",
        ),
    },
)

SCHOOL_CLOSURE_TERMS = (
    "pa day",
    "march break",
    "winter break",
    "spring break",
    "holiday",
    "school closed",
    "school closure",
    "good friday",
    "easter monday",
    "family day",
    "thanksgiving",
    "classes resume after",
    "classes resume",
    "no school",
)


@dataclass
class PriorityMatch:
    source: str
    rule_key: str
    label: str
    matched_by: str

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "rule_key": self.rule_key,
            "label": self.label,
            "matched_by": self.matched_by,
        }


def normalize_priority_topic(value: str) -> str:
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", "_", lowered)
    lowered = re.sub(r"_+", "_", lowered).strip("_")
    return lowered


def _normalize_topic_phrase(value: str) -> str:
    return normalize_priority_topic(value).replace("_", " ").strip()


def _priority_topic_configs() -> tuple[dict, ...]:
    return PRESET_PRIORITY_TOPIC_CONFIG + GENERAL_PRIORITY_TOPIC_CONFIG


def priority_topic_catalog() -> list[dict]:
    return [{"key": item["key"], "label": item["label"]} for item in PRESET_PRIORITY_TOPIC_CONFIG]


def _humanize_priority_label(value: str) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    if not text:
        return ""
    words = []
    for word in text.split(" "):
        if word.isupper() and len(word) <= 4:
            words.append(word)
        else:
            words.append(word[:1].upper() + word[1:].lower())
    return " ".join(words)


def canonicalize_priority_topic(value: str) -> str:
    label = (value or "").strip()
    if not label:
        return ""
    return _humanize_priority_label(label)


def _normalize_topic_objects(values: Iterable[str], topic_aliases: dict[str, list[str]] | None = None) -> list[dict]:
    seen: set[str] = set()
    topics: list[dict] = []
    alias_map = topic_aliases or {}
    for value in values:
        label = canonicalize_priority_topic(value)
        if not label:
            continue
        key = normalize_priority_topic(label)
        if not key or key in seen:
            continue
        seen.add(key)
        aliases = alias_map.get(label.lower(), [])
        topic_obj: dict = {"key": key, "label": label}
        if aliases:
            topic_obj["aliases"] = aliases
        topics.append(topic_obj)
    return topics


def normalize_priority_topics(values: Iterable[str]) -> list[dict]:
    return _normalize_topic_objects(values)


def _topic_objects_from_value(value: object) -> list[dict]:
    if not isinstance(value, list):
        return []
    labels: list[str] = []
    existing_aliases: dict[str, list[str]] = {}
    for item in value:
        if isinstance(item, dict):
            label = str(item.get("label") or item.get("key") or "")
            labels.append(label)
            aliases = list(item.get("aliases") or [])
            if aliases and label:
                existing_aliases[label.lower()] = aliases
        else:
            labels.append(str(item))
    return _normalize_topic_objects(labels, existing_aliases or None)


def _migrate_legacy_priority_structure(structured_json: dict | None) -> dict:
    structured = dict(structured_json or {})
    if any(
        key in structured for key in ("selected_priority_topics", "parsed_priority_topics", "parsed_suppressed_topics")
    ):
        return structured

    legacy_topics = _topic_objects_from_value(structured.get("user_priority_topics"))
    if not legacy_topics:
        return structured

    preset_keys = {item["key"] for item in PRESET_PRIORITY_TOPIC_CONFIG}
    structured["selected_priority_topics"] = [item for item in legacy_topics if item["key"] in preset_keys]
    structured["parsed_priority_topics"] = [item for item in legacy_topics if item["key"] not in preset_keys]
    structured["parsed_suppressed_topics"] = []
    structured["user_priority_topics"] = legacy_topics
    return structured


def selected_priority_topics(structured_json: dict | None) -> list[dict]:
    structured = _migrate_legacy_priority_structure(structured_json)
    return _topic_objects_from_value(structured.get("selected_priority_topics"))


def parsed_priority_topics(structured_json: dict | None) -> list[dict]:
    structured = _migrate_legacy_priority_structure(structured_json)
    return _topic_objects_from_value(structured.get("parsed_priority_topics"))


def parsed_suppressed_priority_topics(structured_json: dict | None) -> list[dict]:
    structured = _migrate_legacy_priority_structure(structured_json)
    return _topic_objects_from_value(structured.get("parsed_suppressed_topics"))


def admin_override_priority_topics(structured_json: dict | None) -> list[dict]:
    structured = _migrate_legacy_priority_structure(structured_json)
    return _topic_objects_from_value(structured.get("admin_priority_topics"))


def admin_override_suppressed_priority_topics(structured_json: dict | None) -> list[dict]:
    structured = _migrate_legacy_priority_structure(structured_json)
    return _topic_objects_from_value(structured.get("admin_suppressed_topics"))


def admin_override_active(structured_json: dict | None) -> bool:
    structured = _migrate_legacy_priority_structure(structured_json)
    return bool(structured.get("admin_override_active"))


def effective_user_priority_topics(structured_json: dict | None) -> list[dict]:
    structured = _migrate_legacy_priority_structure(structured_json)
    if not any(
        key in structured for key in ("selected_priority_topics", "parsed_priority_topics", "parsed_suppressed_topics")
    ):
        return _topic_objects_from_value(structured.get("user_priority_topics"))

    if admin_override_active(structured):
        selected = selected_priority_topics(structured)
        admin_topics = admin_override_priority_topics(structured)
        suppressed_keys = {item["key"] for item in effective_suppressed_priority_topics(structured)}
        merged = _normalize_topic_objects([item["label"] for item in selected + admin_topics])
        return [item for item in merged if item["key"] not in suppressed_keys]

    selected = selected_priority_topics(structured)
    parsed = parsed_priority_topics(structured)
    suppressed_keys = {item["key"] for item in parsed_suppressed_priority_topics(structured)}
    merged = _normalize_topic_objects([item["label"] for item in selected + parsed])
    return [item for item in merged if item["key"] not in suppressed_keys]


def effective_suppressed_priority_topics(structured_json: dict | None) -> list[dict]:
    structured = _migrate_legacy_priority_structure(structured_json)
    parsed = (
        admin_override_suppressed_priority_topics(structured)
        if admin_override_active(structured)
        else parsed_suppressed_priority_topics(structured)
    )
    command_written = list(structured.get("command_written_preferences") or [])
    command_suppressed = [
        str(item.get("label") or item.get("key") or "")
        for item in command_written
        if str(item.get("behavior") or "").strip().lower() == "suppress"
    ]
    return _normalize_topic_objects([item["label"] for item in parsed] + command_suppressed)


def _collect_topic_aliases(structured: dict) -> dict[str, list[str]]:
    """Build a map of topic label (lowercase) -> aliases from all stored topic objects."""
    alias_map: dict[str, list[str]] = {}
    for source_key in ("parsed_priority_topics", "parsed_suppressed_topics", "selected_priority_topics"):
        for item in list((structured or {}).get(source_key) or []):
            if not isinstance(item, dict):
                continue
            aliases = list(item.get("aliases") or [])
            if aliases:
                label = str(item.get("label") or "").strip().lower()
                if label:
                    existing = alias_map.get(label, [])
                    merged = list(dict.fromkeys(existing + aliases))
                    alias_map[label] = merged
    return alias_map


def sync_effective_priority_structure(structured_json: dict | None) -> dict:
    structured = _migrate_legacy_priority_structure(structured_json)
    structured["selected_priority_topics"] = selected_priority_topics(structured)
    structured["parsed_priority_topics"] = parsed_priority_topics(structured)
    structured["parsed_suppressed_topics"] = parsed_suppressed_priority_topics(structured)
    structured["admin_priority_topics"] = admin_override_priority_topics(structured)
    structured["admin_suppressed_topics"] = admin_override_suppressed_priority_topics(structured)
    structured["admin_override_active"] = admin_override_active(structured)
    structured["user_priority_topics"] = effective_user_priority_topics(structured)
    structured["preference_parse_status"] = str(structured.get("preference_parse_status") or "success")
    structured["preference_parse_error"] = str(structured.get("preference_parse_error") or "")
    return structured


def ensure_priority_rules(db: Session, household_id: int) -> None:
    profile = db.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == household_id))
    if profile is None:
        return

    existing_rules = db.scalars(select(PreferenceRule).where(PreferenceRule.household_id == household_id)).all()
    by_source_category = {(rule.source, rule.category): rule for rule in existing_rules}

    for key in SYSTEM_DEFAULT_PRIORITY_CONFIG:
        rule = by_source_category.get(("system_default", key))
        if rule is None:
            db.add(
                PreferenceRule(
                    household_id=household_id,
                    source="system_default",
                    scope="household",
                    mode="priority",
                    priority=1,
                    confidence=1.0,
                    enabled=True,
                    category=key,
                    behavior="mention",
                )
            )

    structured = sync_effective_priority_structure(profile.structured_json)
    profile.structured_json = structured

    effective_topics = effective_user_priority_topics(structured)
    suppressed_topics = effective_suppressed_priority_topics(structured)
    effective_keys = {item["key"] for item in effective_topics}
    suppressed_keys = {item["key"] for item in suppressed_topics}

    for item in effective_topics:
        rule = by_source_category.get(("user_priority", item["key"]))
        if rule is None:
            db.add(
                PreferenceRule(
                    household_id=household_id,
                    source="user_priority",
                    scope="household",
                    mode="priority",
                    priority=1,
                    confidence=1.0,
                    enabled=True,
                    category=item["key"],
                    behavior="mention",
                )
            )
        else:
            rule.enabled = True
            rule.mode = "priority"
            rule.behavior = "mention"

    for item in suppressed_topics:
        rule = by_source_category.get(("user_note", item["key"]))
        if rule is None:
            db.add(
                PreferenceRule(
                    household_id=household_id,
                    source="user_note",
                    scope="household",
                    mode="preference_behavior",
                    priority=1,
                    confidence=1.0,
                    enabled=True,
                    category=item["key"],
                    behavior="suppress",
                )
            )
        else:
            rule.enabled = True
            rule.mode = "preference_behavior"
            rule.behavior = "suppress"

    for rule in existing_rules:
        if rule.source == "user_priority" and rule.category not in effective_keys:
            db.delete(rule)
        if rule.source == "user_note" and rule.mode == "preference_behavior" and rule.category not in suppressed_keys:
            db.delete(rule)


def load_priority_preferences(db: Session, household_id: int) -> dict:
    ensure_priority_rules(db, household_id)
    profile = db.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == household_id))
    assert profile is not None
    rules = db.scalars(
        select(PreferenceRule)
        .where(
            PreferenceRule.household_id == household_id,
            PreferenceRule.mode == "priority",
        )
        .order_by(PreferenceRule.source.asc(), PreferenceRule.category.asc())
    ).all()

    system_defaults = []
    system_enabled = {(rule.category, rule.enabled) for rule in rules if rule.source == "system_default"}
    for key, config in SYSTEM_DEFAULT_PRIORITY_CONFIG.items():
        enabled = True
        for category, value in system_enabled:
            if category == key:
                enabled = value
                break
        system_defaults.append(
            {
                "key": key,
                "label": config["label"],
                "description": config["description"],
                "examples": list(config["examples"]),
                "enabled": enabled,
            }
        )

    structured = sync_effective_priority_structure(profile.structured_json)
    profile.structured_json = structured
    selected = selected_priority_topics(structured)
    parsed = parsed_priority_topics(structured)
    parsed_suppressed = parsed_suppressed_priority_topics(structured)
    admin_topics = admin_override_priority_topics(structured)
    admin_suppressed = admin_override_suppressed_priority_topics(structured)
    effective = effective_user_priority_topics(structured)
    selected_keys = {item["key"] for item in selected}
    command_prefs = list((structured or {}).get("command_written_preferences") or [])
    return {
        "raw_text": profile.raw_text,
        "structured_json": dict(structured),
        "version": profile.version,
        "system_defaults": system_defaults,
        "preference_parse_status": str(structured.get("preference_parse_status") or "success"),
        "preference_parse_error": str(structured.get("preference_parse_error") or ""),
        "admin_override_active": bool(structured.get("admin_override_active")),
        "preset_priority_topics": [
            {
                "key": item["key"],
                "label": item["label"],
                "selected": item["key"] in selected_keys,
            }
            for item in priority_topic_catalog()
        ],
        "parsed_priority_topics": [item["label"] for item in parsed],
        "parsed_suppressed_priority_topics": [item["label"] for item in parsed_suppressed],
        "admin_priority_topics": [item["label"] for item in admin_topics],
        "admin_suppressed_priority_topics": [item["label"] for item in admin_suppressed],
        "user_priority_topics": [item["label"] for item in effective],
        "effective_suppressed_priority_topics": [item["label"] for item in effective_suppressed_priority_topics(structured)],
        "suppressed_priority_topics": [item["label"] for item in effective_suppressed_priority_topics(structured)],
        "topic_aliases": _collect_topic_aliases(structured),
        "command_written_preferences": command_prefs,
    }


def save_priority_preferences(
    db: Session,
    household_id: int,
    *,
    raw_text: str,
    system_defaults: dict[str, bool],
    user_priority_topics: list[str],
    parsed_priority_topics: list[str] | None = None,
    parsed_suppressed_topics: list[str] | None = None,
    admin_priority_topics: list[str] | None = None,
    admin_suppressed_topics: list[str] | None = None,
    admin_override_active: bool | None = None,
    parse_status: str = "success",
    parse_error: str = "",
    topic_aliases: dict[str, list[str]] | None = None,
) -> PreferenceProfile:
    profile = db.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == household_id))
    assert profile is not None
    profile.raw_text = raw_text

    structured = _migrate_legacy_priority_structure(profile.structured_json)
    structured["selected_priority_topics"] = _normalize_topic_objects(user_priority_topics)
    structured["parsed_priority_topics"] = _normalize_topic_objects(parsed_priority_topics or [], topic_aliases)
    structured["parsed_suppressed_topics"] = _normalize_topic_objects(parsed_suppressed_topics or [], topic_aliases)
    if admin_priority_topics is not None:
        structured["admin_priority_topics"] = _normalize_topic_objects(admin_priority_topics)
    if admin_suppressed_topics is not None:
        structured["admin_suppressed_topics"] = _normalize_topic_objects(admin_suppressed_topics)
    if admin_override_active is not None:
        structured["admin_override_active"] = bool(admin_override_active)
    structured["preference_parse_status"] = parse_status if parse_status in {"success", "error"} else "success"
    structured["preference_parse_error"] = (parse_error or "").strip()
    structured = sync_effective_priority_structure(structured)
    profile.structured_json = structured
    profile.version += 1

    ensure_priority_rules(db, household_id)

    system_rules = db.scalars(
        select(PreferenceRule).where(
            PreferenceRule.household_id == household_id,
            PreferenceRule.source == "system_default",
            PreferenceRule.mode == "priority",
        )
    ).all()
    by_category = {rule.category: rule for rule in system_rules}
    for key in SYSTEM_DEFAULT_PRIORITY_CONFIG:
        rule = by_category.get(key)
        if rule is None:
            rule = PreferenceRule(
                household_id=household_id,
                source="system_default",
                scope="household",
                mode="priority",
                priority=1,
                confidence=1.0,
                enabled=True,
                category=key,
                behavior="mention",
            )
            db.add(rule)
        rule.enabled = bool(system_defaults.get(key, True))
        rule.behavior = "mention"

    return profile


def save_command_written_preference(
    db: Session,
    household_id: int,
    *,
    topic: str,
    behavior: str,
) -> PreferenceProfile:
    profile = db.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == household_id))
    assert profile is not None

    normalized_topic = normalize_priority_topic(topic)
    label = (topic or "").strip()
    if not normalized_topic or not label:
        return profile

    behavior_value = behavior if behavior in {"auto_add", "mention", "suppress"} else "mention"
    structured = dict(profile.structured_json or {})
    command_written = list(structured.get("command_written_preferences") or [])
    command_written = [item for item in command_written if str(item.get("key") or "") != normalized_topic]
    command_written.append(
        {
            "key": normalized_topic,
            "label": label,
            "behavior": behavior_value,
            "enabled": True,
        }
    )
    structured["command_written_preferences"] = command_written
    profile.structured_json = structured
    profile.version += 1

    rule = db.scalar(
        select(PreferenceRule).where(
            PreferenceRule.household_id == household_id,
            PreferenceRule.source == "user_command",
            PreferenceRule.mode == "preference_behavior",
            PreferenceRule.category == normalized_topic,
        )
    )
    if rule is None:
        rule = PreferenceRule(
            household_id=household_id,
            source="user_command",
            scope="household",
            mode="preference_behavior",
            priority=1,
            confidence=1.0,
            enabled=True,
            category=normalized_topic,
            behavior=behavior_value,
        )
        db.add(rule)
    else:
        rule.enabled = True
        rule.behavior = behavior_value

    return profile


def topic_matches_text(topic_label: str, *values: str) -> bool:
    normalized_topic = _normalize_topic_phrase(topic_label)
    if not normalized_topic:
        return False

    haystack = " ".join(_normalize_topic_phrase(str(value or "")) for value in values)
    if normalized_topic in haystack:
        return True

    for item in _priority_topic_configs():
        if item["label"] != canonicalize_priority_topic(topic_label):
            continue
        for alias in item.get("aliases") or ():
            normalized_alias = _normalize_topic_phrase(alias)
            if normalized_alias and normalized_alias in haystack:
                return True

    topic_tokens = [token for token in normalized_topic.split() if len(token) >= 4]
    if len(topic_tokens) >= 2 and all(token in haystack.split() for token in topic_tokens):
        return True
    return False


def school_closure_matches(*values: str) -> bool:
    haystack = " ".join(str(value or "").strip().lower() for value in values)
    return any(term in haystack for term in SCHOOL_CLOSURE_TERMS)


REPORT_CARD_TERMS = (
    "report card",
    "report cards",
    "progress report",
    "progress reports",
)


def report_card_matches(*values: str) -> bool:
    haystack = " ".join(str(value or "").strip().lower() for value in values)
    return any(term in haystack for term in REPORT_CARD_TERMS)


PARENT_TEACHER_TERMS = (
    "parent-teacher",
    "parent teacher",
    "parent conference",
    "parent-teacher conference",
    "parent-teacher interview",
    "parent teacher interview",
    "interview evening",
    "conference evening",
)


def parent_teacher_matches(*values: str) -> bool:
    haystack = " ".join(str(value or "").strip().lower() for value in values)
    return any(term in haystack for term in PARENT_TEACHER_TERMS)
