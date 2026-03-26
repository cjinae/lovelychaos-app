#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import zipfile

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.content_analysis import maybe_extract_pdf_text  # noqa: E402


SUPPORTED_EXTENSIONS = {".txt", ".pdf", ".docx"}
DEFAULT_TAXONOMY = {
    "topics": [
        "operations/calendar",
        "parent action items",
        "parent engagement",
        "academic reporting",
        "classroom logistics",
        "extracurriculars/clubs",
        "sports",
        "arts/performances",
        "assemblies/culture",
        "fundraising",
        "giving/community care",
        "health/safety",
        "emergency/disruption",
        "heritage/inclusion",
        "parent resources",
        "special one-off notices",
    ],
    "event_types": [
        "newsletter roundup",
        "family event",
        "registration",
        "reporting deadline",
        "health notice",
        "weather closure",
        "fundraising drive",
        "community care drive",
        "club flyer",
        "assembly invitation",
        "parent resource session",
    ],
    "commonness": ["routine", "seasonal", "exceptional"],
    "doc_types": [
        "newsletter",
        "event_invite",
        "registration_flyer",
        "closure_notice",
        "health_notice",
        "fundraising_notice",
        "resource_notice",
    ],
    "scopes": ["classroom", "school_wide", "board_level", "external_partner"],
}

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
PHONE_RE = re.compile(r"(?:(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?){2}\d{4})")
URL_RE = re.compile(r"https?://[^\s<>()]+", re.I)
ACCESS_CODE_RE = re.compile(
    r"\b(?:school\s+code|parent\s+code|access\s+code|code)\s*(?:is|:|-)?\s*([A-Za-z0-9_-]{4,})\b",
    re.I,
)
CLASSROOM_RE = re.compile(r"\b(?:room|classroom)\s+([A-Za-z0-9-]{1,12})\b", re.I)


class RedactionError(RuntimeError):
    pass


@dataclass
class RedactionReport:
    placeholder_counts: dict[str, int]
    manual_review_required: bool
    review_status: str
    source_filename: str

    def as_dict(self) -> dict:
        return {
            "placeholder_counts": dict(self.placeholder_counts),
            "manual_review_required": self.manual_review_required,
            "review_status": self.review_status,
            "source_filename": self.source_filename,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a redacted school communications corpus from local raw files.")
    parser.add_argument("--input", required=True, help="Directory containing raw txt/pdf/docx files and optional manifest.json")
    parser.add_argument("--output", required=True, help="Output corpus JSON path")
    parser.add_argument("--eval-output", required=True, help="Output eval JSON path")
    args = parser.parse_args()

    input_dir = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    eval_output_path = Path(args.eval_output).expanduser().resolve()

    corpus, eval_fixtures, qa_report = build_school_corpus(input_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    eval_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(corpus, indent=2), encoding="utf-8")
    eval_output_path.write_text(json.dumps(eval_fixtures, indent=2), encoding="utf-8")
    qa_path = output_path.with_name(output_path.stem + "_qa_report.json")
    qa_path.write_text(json.dumps(qa_report, indent=2), encoding="utf-8")
    return 0


def build_school_corpus(input_dir: Path) -> tuple[dict, dict, dict]:
    manifest = _load_manifest(input_dir / "manifest.json")
    entries: list[dict] = []
    eval_items: list[dict] = []
    qa_items: list[dict] = []

    for path in sorted(input_dir.iterdir()):
        if path.name == "manifest.json" or path.suffix.lower() not in SUPPORTED_EXTENSIONS or not path.is_file():
            continue
        metadata = dict(manifest.get(path.name) or {})
        raw_text = extract_text_from_path(path)
        redacted_subject = _redact_filename(path.name)
        redacted_body, report = redact_document(raw_text, metadata=metadata, source_filename=path.name)
        validate_redaction(redacted_body, metadata=metadata)
        entry = {
            "entry_id": metadata.get("entry_id") or path.stem.lower().replace(" ", "-").replace("_", "-"),
            "doc_type": metadata.get("doc_type") or infer_doc_type(path.name, raw_text),
            "scope": metadata.get("scope") or infer_scope(raw_text),
            "topics": list(metadata.get("topics") or infer_topics(raw_text)),
            "event_types": list(metadata.get("event_types") or infer_event_types(raw_text)),
            "commonness": metadata.get("commonness") or infer_commonness(raw_text),
            "action_required": metadata.get("action_required") or infer_action_required(raw_text),
            "audience": metadata.get("audience") or infer_audience(raw_text),
            "redacted_subject": metadata.get("redacted_subject") or redacted_subject,
            "redacted_body": redacted_body,
            "salient_phrases": list(metadata.get("salient_phrases") or infer_salient_phrases(redacted_body)),
            "source_kind": metadata.get("source_kind") or infer_source_kind(path),
            "redaction_report": report.as_dict(),
        }
        entries.append(entry)
        eval_items.append(
            {
                "entry_id": entry["entry_id"],
                "expected_topics": list(entry["topics"]),
                "expected_event_types": list(entry["event_types"]),
                "expected_commonness": entry["commonness"],
                "expected_action_required": entry["action_required"],
                "summary_weight": metadata.get("summary_weight") or infer_summary_weight(entry),
            }
        )
        qa_items.append(
            {
                "entry_id": entry["entry_id"],
                "source_filename": path.name,
                "placeholder_counts": dict(report.placeholder_counts),
                "manual_review_required": True,
            }
        )

    corpus = {
        "version": "v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "taxonomy": dict(DEFAULT_TAXONOMY),
        "entries": entries,
    }
    eval_payload = {
        "version": "v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "items": eval_items,
    }
    qa_payload = {
        "version": "v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "items": qa_items,
    }
    return corpus, eval_payload, qa_payload


def extract_text_from_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".txt":
        return path.read_text(encoding="utf-8")
    if suffix == ".pdf":
        return maybe_extract_pdf_text(path.read_bytes())
    if suffix == ".docx":
        return _extract_docx_text(path)
    raise RedactionError(f"Unsupported file type: {path.suffix}")


def redact_document(text: str, *, metadata: dict, source_filename: str) -> tuple[str, RedactionReport]:
    placeholder_counts: dict[str, int] = {}
    replacement_maps = {
        "EMAIL": {},
        "PHONE": {},
        "LINK": {},
        "REGISTRATION_LINK": {},
        "ACCESS_CODE": {},
        "CHILD_NAME": {},
        "STAFF_NAME": {},
        "CONTACT_NAME": {},
        "CLASSROOM_ID": {},
    }

    def next_placeholder(kind: str, value: str) -> str:
        bucket = replacement_maps[kind]
        key = value.strip()
        if key not in bucket:
            bucket[key] = f"{kind}_{len(bucket) + 1}"
            placeholder_counts[kind] = placeholder_counts.get(kind, 0) + 1
        return bucket[key]

    redacted = text or ""

    def replace_urls(match: re.Match) -> str:
        value = match.group(0).rstrip(".,)")
        kind = "REGISTRATION_LINK" if any(token in value.lower() for token in ("schoolmessenger", "forms.gle", "parentinterview", "schoolcashonline", "thelunchmom")) else "LINK"
        return next_placeholder(kind, value)

    redacted = URL_RE.sub(replace_urls, redacted)
    redacted = EMAIL_RE.sub(lambda match: next_placeholder("EMAIL", match.group(0)), redacted)
    redacted = PHONE_RE.sub(lambda match: next_placeholder("PHONE", match.group(0)), redacted)
    redacted = ACCESS_CODE_RE.sub(
        lambda match: match.group(0).replace(match.group(1), next_placeholder("ACCESS_CODE", match.group(1))),
        redacted,
    )
    redacted = CLASSROOM_RE.sub(
        lambda match: match.group(0).replace(match.group(1), next_placeholder("CLASSROOM_ID", match.group(1))),
        redacted,
    )

    school_names = list(metadata.get("school_names") or [])
    if school_names:
        for school_name in sorted(school_names, key=len, reverse=True):
            redacted = re.sub(rf"\b{re.escape(school_name)}\b", "SCHOOL_NAME", redacted, flags=re.I)
        if "SCHOOL_NAME" in redacted:
            placeholder_counts["SCHOOL_NAME"] = 1

    for kind, field in (
        ("CHILD_NAME", "child_names"),
        ("STAFF_NAME", "staff_names"),
        ("CONTACT_NAME", "contact_names"),
    ):
        for name in list(metadata.get(field) or []):
            placeholder = next_placeholder(kind, name)
            redacted = re.sub(rf"\b{re.escape(name)}\b", placeholder, redacted, flags=re.I)

    for code in list(metadata.get("access_codes") or []):
        placeholder = next_placeholder("ACCESS_CODE", code)
        redacted = re.sub(rf"\b{re.escape(code)}\b", placeholder, redacted, flags=re.I)

    redacted = re.sub(r"\s+", " ", redacted).strip()
    report = RedactionReport(
        placeholder_counts=placeholder_counts,
        manual_review_required=True,
        review_status="manual_review_required",
        source_filename=source_filename,
    )
    return redacted, report


def validate_redaction(text: str, *, metadata: dict) -> None:
    if EMAIL_RE.search(text):
        raise RedactionError("Unredacted email detected")
    if URL_RE.search(text):
        raise RedactionError("Unredacted URL detected")
    if PHONE_RE.search(text):
        raise RedactionError("Unredacted phone number detected")
    for school_name in list(metadata.get("school_names") or []):
        if school_name and re.search(rf"\b{re.escape(school_name)}\b", text, flags=re.I):
            raise RedactionError(f"Unredacted school name detected: {school_name}")
    for field in ("child_names", "staff_names", "contact_names"):
        for name in list(metadata.get(field) or []):
            if name and re.search(rf"\b{re.escape(name)}\b", text, flags=re.I):
                raise RedactionError(f"Unredacted name detected: {name}")
    for code in list(metadata.get("access_codes") or []):
        if code and re.search(rf"\b{re.escape(code)}\b", text, flags=re.I):
            raise RedactionError(f"Unredacted access code detected: {code}")


def infer_doc_type(filename: str, text: str) -> str:
    lowered = f"{filename} {text}".lower()
    if "closed due to weather" in lowered or "school closed" in lowered:
        return "closure_notice"
    if "screening" in lowered or "pediculosis" in lowered:
        return "health_notice"
    if "register" in lowered or "registration" in lowered or "flyer" in lowered:
        return "registration_flyer"
    if "donation" in lowered or "food drive" in lowered or "hamper" in lowered:
        return "fundraising_notice"
    if "resource" in lowered or "roadmap" in lowered or "special education" in lowered:
        return "resource_notice"
    if "newsletter" in lowered or "upcoming dates" in lowered:
        return "newsletter"
    return "event_invite"


def infer_scope(text: str) -> str:
    lowered = text.lower()
    if "grade " in lowered or "room " in lowered or "classroom" in lowered:
        return "classroom"
    if "tdsb" in lowered or "board" in lowered:
        return "board_level"
    if "brick labs" in lowered or "extra ed" in lowered or "external" in lowered:
        return "external_partner"
    return "school_wide"


def infer_topics(text: str) -> list[str]:
    lowered = text.lower()
    topics: list[str] = []
    mapping = [
        ("operations/calendar", ("first day back", "pa day", "march break", "winter break", "upcoming dates")),
        ("parent action items", ("register", "rsvp", "deadline", "code", "please complete")),
        ("parent engagement", ("family math night", "open house", "movie night", "parents and caregivers are invited")),
        ("academic reporting", ("report cards", "progress reports", "interviews")),
        ("classroom logistics", ("swim", "indoor shoes", "routine")),
        ("extracurriculars/clubs", ("club", "coding", "chess", "science club", "crafters")),
        ("sports", ("tournament", "volleyball", "basketball", "cross country", "soccer")),
        ("arts/performances", ("concert", "musical", "performance")),
        ("assemblies/culture", ("assembly", "character trait", "kindness")),
        ("fundraising", ("fundraising", "donation", "direct donation")),
        ("giving/community care", ("food drive", "hamper", "generosity")),
        ("health/safety", ("weather", "safety", "head lice", "screening")),
        ("emergency/disruption", ("school closed", "closure", "due to weather")),
        ("heritage/inclusion", ("heritage month", "truth and reconciliation", "world down syndrome day")),
        ("parent resources", ("social media", "special education", "well-being", "mental health")),
    ]
    for label, phrases in mapping:
        if any(phrase in lowered for phrase in phrases):
            topics.append(label)
    return topics or ["special one-off notices"]


def infer_event_types(text: str) -> list[str]:
    lowered = text.lower()
    event_types: list[str] = []
    mapping = [
        ("newsletter roundup", ("newsletter", "upcoming dates")),
        ("family event", ("math night", "movie night", "open house")),
        ("registration", ("register", "registration")),
        ("reporting deadline", ("report cards", "progress reports", "interviews")),
        ("health notice", ("screening", "head lice")),
        ("weather closure", ("closed due to weather", "school closed")),
        ("fundraising drive", ("direct donation", "fundraising")),
        ("community care drive", ("food drive", "holiday hamper")),
        ("club flyer", ("club", "coding", "chess", "science club")),
        ("assembly invitation", ("assembly", "doors open")),
        ("parent resource session", ("roadmap", "parents and caregivers", "special education")),
    ]
    for label, phrases in mapping:
        if any(phrase in lowered for phrase in phrases):
            event_types.append(label)
    return event_types or ["newsletter roundup"]


def infer_commonness(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("school closed", "closure", "head lice", "special visitors")):
        return "exceptional"
    if any(term in lowered for term in ("holiday", "winter", "movie night", "concert", "donation drive")):
        return "seasonal"
    return "routine"


def infer_action_required(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("must", "do not send", "required", "keep students home")):
        return "required"
    if any(term in lowered for term in ("deadline", "register", "rsvp", "book interviews", "schedule appointments")):
        return "deadline"
    if any(term in lowered for term in ("join us", "invited", "donate", "support")):
        return "optional"
    return "none"


def infer_audience(text: str) -> str:
    lowered = text.lower()
    if "parents and caregivers" in lowered:
        return "parents and caregivers"
    if "grade " in lowered:
        return "grade-specific families"
    return "all families"


def infer_salient_phrases(text: str) -> list[str]:
    phrases: list[str] = []
    lowered = text.lower()
    for phrase in (
        "movie night",
        "family math night",
        "progress reports",
        "parent-teacher interviews",
        "head lice screening",
        "school closed",
        "food drive",
        "holiday hampers",
        "science club",
        "social media and safety",
    ):
        if phrase in lowered:
            phrases.append(phrase)
    if not phrases:
        phrases.extend([part.strip() for part in text.split(".")[:2] if part.strip()])
    return phrases[:6]


def infer_source_kind(path: Path) -> str:
    return "attachment_text" if path.suffix.lower() in {".pdf", ".docx"} else "email_body"


def infer_summary_weight(entry: dict) -> str:
    if entry["action_required"] in {"required", "deadline"} or entry["doc_type"] == "closure_notice":
        return "important"
    if entry["doc_type"] in {"event_invite", "newsletter", "health_notice"}:
        return "mentioned"
    return "mentioned"


def _extract_docx_text(path: Path) -> str:
    with zipfile.ZipFile(path) as archive:
        xml = archive.read("word/document.xml").decode("utf-8", errors="replace")
    xml = re.sub(r"</w:p>", "\n", xml)
    xml = re.sub(r"<[^>]+>", " ", xml)
    return re.sub(r"\s+", " ", xml).strip()


def _redact_filename(filename: str) -> str:
    stem = Path(filename).stem.replace("_", " ").strip()
    return re.sub(r"\s+", " ", stem)


def _load_manifest(path: Path) -> dict:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload.get("documents") or payload)


if __name__ == "__main__":
    raise SystemExit(main())
