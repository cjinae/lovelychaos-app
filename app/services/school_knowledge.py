from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re
import unicodedata
from typing import Iterable


CORPUS_PATH = Path(__file__).resolve().parent.parent / "data" / "school_comm_corpus_v1.json"


@dataclass(frozen=True)
class SchoolCorpusEntry:
    entry_id: str
    doc_type: str
    scope: str
    topics: list[str]
    event_types: list[str]
    commonness: str
    action_required: str
    audience: str
    redacted_subject: str
    redacted_body: str
    salient_phrases: list[str]
    source_kind: str
    redaction_report: dict


@dataclass(frozen=True)
class KnowledgeMatch:
    entry_id: str
    score: int
    doc_type: str
    scope: str
    topics: list[str]
    event_types: list[str]
    commonness: str
    action_required: str
    audience: str
    source_kind: str
    redacted_subject: str
    snippet: str

    def as_prompt_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "score": self.score,
            "doc_type": self.doc_type,
            "scope": self.scope,
            "topics": list(self.topics),
            "event_types": list(self.event_types),
            "commonness": self.commonness,
            "action_required": self.action_required,
            "audience": self.audience,
            "source_kind": self.source_kind,
            "redacted_subject": self.redacted_subject,
            "snippet": self.snippet,
        }

    def as_audit_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "score": self.score,
            "doc_type": self.doc_type,
            "scope": self.scope,
            "topics": list(self.topics),
            "event_types": list(self.event_types),
            "commonness": self.commonness,
            "action_required": self.action_required,
            "audience": self.audience,
            "source_kind": self.source_kind,
        }


@dataclass(frozen=True)
class KnowledgeContext:
    matched_topics: list[str]
    matched_event_types: list[str]
    commonness_hints: list[dict]
    retrieved_examples: list[dict]
    retrieval_notes: list[str]
    matches: list[KnowledgeMatch]

    def as_prompt_dict(self) -> dict:
        return {
            "matched_topics": list(self.matched_topics),
            "matched_event_types": list(self.matched_event_types),
            "commonness_hints": list(self.commonness_hints),
            "retrieved_examples": list(self.retrieved_examples),
            "retrieval_notes": list(self.retrieval_notes),
        }

    def as_audit_dict(self) -> dict:
        return {
            "matched_topics": list(self.matched_topics),
            "matched_event_types": list(self.matched_event_types),
            "commonness_hints": list(self.commonness_hints),
            "retrieval_notes": list(self.retrieval_notes),
            "matches": [match.as_audit_dict() for match in self.matches],
        }


@dataclass(frozen=True)
class SchoolKnowledgeCorpus:
    taxonomy: dict
    entries: list[SchoolCorpusEntry]


def clear_school_knowledge_cache() -> None:
    _load_school_knowledge_corpus.cache_clear()


def retrieve_knowledge_context(
    *,
    subject: str,
    sections: Iterable,
    analysis_text: str,
    extracted_events: Iterable,
    max_matches: int = 5,
) -> KnowledgeContext:
    corpus = _load_school_knowledge_corpus()
    if not corpus.entries:
        return KnowledgeContext([], [], [], [], [], [])

    query_text = _build_query_text(
        subject=subject,
        sections=sections,
        analysis_text=analysis_text,
        extracted_events=extracted_events,
    )
    query_norm = _normalize_text(query_text)
    query_tokens = _tokenize(query_norm)
    if not query_tokens:
        return KnowledgeContext([], [], [], [], [], [])

    scored: list[KnowledgeMatch] = []
    for entry in corpus.entries:
        score = _score_entry(entry, query_norm, query_tokens)
        if score <= 0:
            continue
        scored.append(
            KnowledgeMatch(
                entry_id=entry.entry_id,
                score=score,
                doc_type=entry.doc_type,
                scope=entry.scope,
                topics=list(entry.topics),
                event_types=list(entry.event_types),
                commonness=entry.commonness,
                action_required=entry.action_required,
                audience=entry.audience,
                source_kind=entry.source_kind,
                redacted_subject=entry.redacted_subject,
                snippet=_snippet(entry.redacted_body),
            )
        )

    if not scored:
        return KnowledgeContext([], [], [], [], [], [])

    ranked = sorted(scored, key=lambda item: (-item.score, item.entry_id))[: max(1, min(max_matches, 5))]
    matched_topics = _ordered_unique(topic for item in ranked for topic in item.topics)
    matched_event_types = _ordered_unique(event_type for item in ranked for event_type in item.event_types)
    commonness_hints = _commonness_hints(ranked)
    retrieval_notes = _build_retrieval_notes(ranked, matched_topics, matched_event_types)

    return KnowledgeContext(
        matched_topics=matched_topics,
        matched_event_types=matched_event_types,
        commonness_hints=commonness_hints,
        retrieved_examples=[item.as_prompt_dict() for item in ranked],
        retrieval_notes=retrieval_notes,
        matches=ranked,
    )


@lru_cache(maxsize=1)
def _load_school_knowledge_corpus() -> SchoolKnowledgeCorpus:
    if not CORPUS_PATH.exists():
        return SchoolKnowledgeCorpus(taxonomy={}, entries=[])
    try:
        payload = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return SchoolKnowledgeCorpus(taxonomy={}, entries=[])

    entries: list[SchoolCorpusEntry] = []
    for item in list(payload.get("entries") or []):
        try:
            entries.append(
                SchoolCorpusEntry(
                    entry_id=str(item.get("entry_id") or "").strip(),
                    doc_type=str(item.get("doc_type") or "").strip(),
                    scope=str(item.get("scope") or "").strip(),
                    topics=[str(value).strip() for value in list(item.get("topics") or []) if str(value).strip()],
                    event_types=[
                        str(value).strip() for value in list(item.get("event_types") or []) if str(value).strip()
                    ],
                    commonness=str(item.get("commonness") or "").strip(),
                    action_required=str(item.get("action_required") or "").strip(),
                    audience=str(item.get("audience") or "").strip(),
                    redacted_subject=str(item.get("redacted_subject") or "").strip(),
                    redacted_body=str(item.get("redacted_body") or "").strip(),
                    salient_phrases=[
                        str(value).strip() for value in list(item.get("salient_phrases") or []) if str(value).strip()
                    ],
                    source_kind=str(item.get("source_kind") or "").strip(),
                    redaction_report=dict(item.get("redaction_report") or {}),
                )
            )
        except Exception:
            continue
    return SchoolKnowledgeCorpus(taxonomy=dict(payload.get("taxonomy") or {}), entries=entries)


def _build_query_text(*, subject: str, sections: Iterable, analysis_text: str, extracted_events: Iterable) -> str:
    parts: list[str] = []
    if subject:
        parts.append(subject)
    for event in extracted_events or []:
        title = str(getattr(event, "title", "") or "").strip()
        category = str(getattr(event, "category", "") or "").strip()
        if title:
            parts.append(title)
        if category:
            parts.append(category)
    for section in list(sections or [])[:8]:
        label = str(getattr(section, "label", "") or "").strip()
        section_kind = str(getattr(section, "section_kind", "") or "").strip()
        text = str(getattr(section, "text", "") or "").strip()
        if label:
            parts.append(label)
        if section_kind:
            parts.append(section_kind)
        if text:
            parts.append(text[:600])
    if analysis_text:
        parts.append(str(analysis_text)[:2400])
    return "\n".join(part for part in parts if part).strip()


def _score_entry(entry: SchoolCorpusEntry, query_norm: str, query_tokens: set[str]) -> int:
    score = 0
    entry_subject_norm = _normalize_text(entry.redacted_subject)
    entry_body_norm = _normalize_text(entry.redacted_body)
    entry_topic_norms = [_normalize_text(topic) for topic in entry.topics]
    entry_event_type_norms = [_normalize_text(value) for value in entry.event_types]
    salient_norms = [_normalize_text(value) for value in entry.salient_phrases]
    entry_tokens = _tokenize(
        " ".join(
            [
                entry_subject_norm,
                entry_body_norm[:1200],
                " ".join(entry_topic_norms),
                " ".join(entry_event_type_norms),
                " ".join(salient_norms),
                _normalize_text(entry.doc_type),
                _normalize_text(entry.source_kind),
                _normalize_text(entry.scope),
            ]
        )
    )
    overlap = len(query_tokens & entry_tokens)
    score += min(overlap, 12) * 2

    for phrase in salient_norms:
        if phrase and phrase in query_norm:
            score += 10
    for topic in entry_topic_norms:
        if topic and topic in query_norm:
            score += 6
    for event_type in entry_event_type_norms:
        if event_type and event_type in query_norm:
            score += 5
    if entry_subject_norm and entry_subject_norm in query_norm:
        score += 8
    if entry.doc_type == "newsletter" and "upcoming dates" in query_norm:
        score += 2
    if entry.scope == "school_wide" and any(token in query_tokens for token in {"school", "family", "parents"}):
        score += 1
    if entry.source_kind == "attachment_text" and "attachment" in query_norm:
        score += 1
    return score


def _commonness_hints(matches: list[KnowledgeMatch]) -> list[dict]:
    topic_commonness: Counter[tuple[str, str]] = Counter()
    for match in matches:
        for topic in match.topics:
            topic_commonness[(topic, match.commonness)] += 1
    hints: list[dict] = []
    for topic in _ordered_unique(topic for match in matches for topic in match.topics):
        ranked = sorted(
            ((commonness, count) for (hint_topic, commonness), count in topic_commonness.items() if hint_topic == topic),
            key=lambda item: (-item[1], item[0]),
        )
        if not ranked:
            continue
        hints.append({"label": topic, "commonness": ranked[0][0]})
    return hints[:6]


def _build_retrieval_notes(
    matches: list[KnowledgeMatch], matched_topics: list[str], matched_event_types: list[str]
) -> list[str]:
    notes: list[str] = []
    notes.append(f"matched {len(matches)} school communication example(s)")
    if matched_topics:
        notes.append("topics: " + ", ".join(matched_topics[:5]))
    if matched_event_types:
        notes.append("event types: " + ", ".join(matched_event_types[:5]))
    return notes


def _snippet(value: str, limit: int = 280) -> str:
    compact = re.sub(r"\s+", " ", (value or "").strip())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9\s/-]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _tokenize(value: str) -> set[str]:
    tokens = {token for token in re.split(r"[\s/-]+", value or "") if len(token) >= 3}
    return tokens


def _ordered_unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        key = str(value).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(str(value).strip())
    return ordered
