from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from sqlalchemy import delete, select
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
}

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


def normalize_priority_topics(values: Iterable[str]) -> list[dict]:
    seen: set[str] = set()
    topics: list[dict] = []
    for value in values:
        label = (value or "").strip()
        if not label:
            continue
        key = normalize_priority_topic(label)
        if not key or key in seen:
            continue
        seen.add(key)
        topics.append({"key": key, "label": label})
    return topics


def derive_user_priority_topics(raw_text: str, structured_json: dict | None) -> list[dict]:
    structured = structured_json or {}
    topics = structured.get("user_priority_topics")
    if isinstance(topics, list):
        values: list[str] = []
        for item in topics:
            if isinstance(item, dict):
                values.append(str(item.get("label") or item.get("key") or ""))
            else:
                values.append(str(item))
        normalized = normalize_priority_topics(values)
        if normalized:
            return normalized

    chunks = re.split(r"[,;\n]+", raw_text or "")
    return normalize_priority_topics([chunk.strip() for chunk in chunks if chunk.strip()])


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

    structured = dict(profile.structured_json or {})
    topics = derive_user_priority_topics(profile.raw_text, structured)
    structured["user_priority_topics"] = topics
    profile.structured_json = structured

    current_topic_keys = {item["key"] for item in topics}
    for item in topics:
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

    for rule in existing_rules:
        if rule.source == "user_priority" and rule.category not in current_topic_keys:
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

    topics = derive_user_priority_topics(profile.raw_text, profile.structured_json)
    command_prefs = list((profile.structured_json or {}).get("command_written_preferences") or [])
    return {
        "raw_text": profile.raw_text,
        "structured_json": dict(profile.structured_json or {}),
        "version": profile.version,
        "system_defaults": system_defaults,
        "user_priority_topics": [item["label"] for item in topics],
        "user_priority_topic_objects": topics,
        "command_written_preferences": command_prefs,
    }


def save_priority_preferences(
    db: Session,
    household_id: int,
    *,
    raw_text: str,
    system_defaults: dict[str, bool],
    user_priority_topics: list[str],
) -> PreferenceProfile:
    profile = db.scalar(select(PreferenceProfile).where(PreferenceProfile.household_id == household_id))
    assert profile is not None
    profile.raw_text = raw_text
    normalized_topics = normalize_priority_topics(user_priority_topics)
    structured = dict(profile.structured_json or {})
    structured["user_priority_topics"] = normalized_topics
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

    db.execute(
        delete(PreferenceRule).where(
            PreferenceRule.household_id == household_id,
            PreferenceRule.source == "user_priority",
            PreferenceRule.mode == "priority",
        )
    )
    for topic in normalized_topics:
        db.add(
            PreferenceRule(
                household_id=household_id,
                source="user_priority",
                scope="household",
                mode="priority",
                priority=1,
                confidence=1.0,
                enabled=True,
                category=topic["key"],
                behavior="mention",
            )
        )

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
    normalized_topic = normalize_priority_topic(topic_label).replace("_", " ").strip()
    if not normalized_topic:
        return False
    haystack = " ".join(str(value or "").strip().lower() for value in values)
    return normalized_topic in haystack


def school_closure_matches(*values: str) -> bool:
    haystack = " ".join(str(value or "").strip().lower() for value in values)
    return any(term in haystack for term in SCHOOL_CLOSURE_TERMS)
