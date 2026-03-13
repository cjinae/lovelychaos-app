from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.config import settings
from app.services.content_analysis import (
    DownloadedAttachment,
    build_prioritized_chunks,
    dedupe_extracted_events,
    maybe_extract_pdf_text,
)
from app.services.llm import OpenAIDecisionEngine


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local PDF through the LovelyChaos extractor.")
    parser.add_argument("pdf_path", help="Path to the local PDF file")
    parser.add_argument("--subject", default="Local PDF Replay", help="Synthetic subject for extraction")
    parser.add_argument("--preferences", default="", help="Household preferences to pass into extraction")
    parser.add_argument("--timezone", default="UTC", help="Timezone hint")
    parser.add_argument("--max-chars", type=int, default=1600, help="Chunk size for prioritized sections")
    args = parser.parse_args()

    pdf_path = Path(args.pdf_path)
    content = pdf_path.read_bytes()
    extracted_text = maybe_extract_pdf_text(content)
    attachment = DownloadedAttachment(
        filename=pdf_path.name,
        content_type="application/pdf",
        content=content,
        source_url=str(pdf_path.resolve()),
        status_reason="local_file",
        extracted_text=extracted_text,
    )

    sections, chunks = build_prioritized_chunks("", [attachment], max_chars=args.max_chars)
    engine = OpenAIDecisionEngine(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        timeout_sec=settings.openai_timeout_sec,
        base_url=settings.openai_base_url,
    )

    events = []
    notes: list[str] = []
    chunk_summaries = []
    chunk_failures = []
    for chunk in chunks:
        try:
            result = engine.extract_events(
                body_text=chunk.text,
                subject=args.subject,
                household_preferences=args.preferences,
                timezone_hint=args.timezone,
            )
            chunk_events = result.get("events") or []
            events.extend(chunk_events)
            if result.get("email_level_notes"):
                notes.append(str(result["email_level_notes"]))
            chunk_summaries.append(
                {
                    "index": chunk.index,
                    "label": chunk.label,
                    "priority_score": chunk.priority_score,
                    "section_labels": chunk.section_labels,
                    "char_count": len(chunk.text),
                    "event_count": len(chunk_events),
                    "status": "ok",
                }
            )
        except Exception as exc:
            chunk_failures.append(
                {
                    "index": chunk.index,
                    "label": chunk.label,
                    "priority_score": chunk.priority_score,
                    "section_labels": chunk.section_labels,
                    "char_count": len(chunk.text),
                    "error_type": exc.__class__.__name__,
                    "detail": str(exc),
                }
            )

    deduped_events = dedupe_extracted_events(events)
    output = {
        "pdf_path": str(pdf_path.resolve()),
        "extracted_char_count": len(extracted_text),
        "section_count": len(sections),
        "sections": [
            {
                "index": section.index,
                "label": section.label,
                "section_kind": section.section_kind,
                "priority_score": section.priority_score,
                "char_count": len(section.text),
            }
            for section in sections
        ],
        "chunk_summaries": chunk_summaries,
        "chunk_failures": chunk_failures,
        "event_count": len(deduped_events),
        "events": [
            {
                "title": event.title,
                "start_at": event.start_at.isoformat() if event.start_at else None,
                "end_at": event.end_at.isoformat() if event.end_at else None,
                "category": event.category,
                "confidence": event.confidence,
                "target_scope": event.target_scope,
                "mentioned_names": event.mentioned_names,
                "mentioned_schools": event.mentioned_schools,
                "target_grades": event.target_grades,
                "preference_match": event.preference_match,
                "model_batch": event.model_batch,
                "model_reason": event.model_reason,
            }
            for event in deduped_events
        ],
        "notes": notes,
    }
    print(json.dumps(output, indent=2))
    return 0 if deduped_events else 1


if __name__ == "__main__":
    raise SystemExit(main())
