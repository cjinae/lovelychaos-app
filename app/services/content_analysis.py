from __future__ import annotations

from dataclasses import dataclass
import base64
from io import BytesIO
import os
import re
import subprocess
import tempfile
from typing import Iterable, Optional
import unicodedata
from urllib.parse import parse_qs, urlparse

import httpx

from app.services.llm import ExtractedEvent


SCHOOLMESSENGER_HOST_PATTERNS = (
    "track.spe.schoolmessenger.com",
    "msg.schoolmessenger.ca",
    ".schoolmessenger.ca",
    ".schoolmessenger.com",
)
MIN_EXTRACTED_TEXT_CHARS = 50
OCR_PAGE_TEXT_PROMPT = """<output_contract>
- Return only the OCR text for this single page as plain text.
- Do not add commentary, markdown, summaries, or page labels beyond the extracted page text itself.
- If no readable text is present, return an empty string.
</output_contract>

<task>
Extract all readable text from this PDF page image in natural reading order.
Preserve headings, bullet lists, table-like rows, dates, times, and line breaks when they are readable.
</task>

<grounding_rules>
- Transcribe only text that is visible on the page.
- Do not summarize, normalize away important formatting cues, or infer missing words.
- If a word or number is unclear, preserve only the visible text rather than inventing content.
</grounding_rules>

<verification_loop>
Before finalizing:
- Do a quick second pass for missed dates, times, bullets, headings, and footer or header text.
- Check that the output is plain text only.
</verification_loop>
"""


@dataclass
class DownloadedAttachment:
    filename: str
    content_type: str
    content: bytes
    source_url: str
    status_reason: str
    extracted_text: str = ""


@dataclass
class LinkAttempt:
    url: str
    host: str
    outcome: str
    status_reason: str
    final_url: str = ""


@dataclass
class LinkResolutionReport:
    attachments: list[DownloadedAttachment]
    attempts: list[LinkAttempt]


@dataclass
class AnalysisSection:
    index: int
    source_kind: str
    section_kind: str
    label: str
    priority_score: int
    text: str


@dataclass
class AnalysisChunk:
    index: int
    source_kind: str
    label: str
    priority_score: int
    section_labels: list[str]
    text: str


def extract_candidate_links(body_text: str) -> list[str]:
    matches = re.findall(r"https?://[^\s<>()]+", body_text or "", flags=re.IGNORECASE)
    seen: set[str] = set()
    links: list[str] = []
    for link in matches:
        normalized = link.rstrip(").,>")
        if normalized not in seen:
            seen.add(normalized)
            links.append(normalized)
    return links


def is_schoolmessenger_host(host: str) -> bool:
    host = (host or "").lower().strip()
    return any(
        host == pattern or host.endswith(pattern)
        for pattern in SCHOOLMESSENGER_HOST_PATTERNS
    )


def resolve_and_download_links(
    links: Iterable[str],
    timeout_sec: int = 15,
    client: Optional[httpx.Client] = None,
) -> LinkResolutionReport:
    own_client = client is None
    active_client = client or httpx.Client(timeout=timeout_sec, follow_redirects=True)
    attachments: list[DownloadedAttachment] = []
    attempts: list[LinkAttempt] = []
    try:
        for link in links:
            host = urlparse(link).netloc.lower()
            if not is_schoolmessenger_host(host):
                attempts.append(LinkAttempt(url=link, host=host, outcome="skipped", status_reason="schoolmessenger_unsupported"))
                continue
            try:
                response = active_client.get(link)
                final_url = str(response.url)
                download = _download_schoolmessenger_attachment(active_client, response, final_url)
                if download is None:
                    attempts.append(
                        LinkAttempt(
                            url=link,
                            host=host,
                            outcome="failed",
                            status_reason="schoolmessenger_directlink_missing",
                            final_url=final_url,
                        )
                    )
                    continue
                attachments.append(download)
                attempts.append(
                    LinkAttempt(
                        url=link,
                        host=host,
                        outcome="downloaded",
                        status_reason=download.status_reason,
                        final_url=final_url,
                    )
                )
            except Exception:
                attempts.append(
                    LinkAttempt(
                        url=link,
                        host=host,
                        outcome="failed",
                        status_reason="schoolmessenger_directlink_download_failed",
                    )
                )
    finally:
        if own_client:
            active_client.close()
    return LinkResolutionReport(attachments=attachments, attempts=attempts)


def build_analysis_text(body_text: str, attachments: Iterable[DownloadedAttachment]) -> str:
    cleaned_body = _clean_body_text(body_text)
    sections: list[str] = []
    if cleaned_body:
        sections.append(cleaned_body)

    normalized_existing = {_normalize_for_compare(cleaned_body)} if cleaned_body else set()
    for attachment in attachments:
        attachment_text = (attachment.extracted_text or "").strip()
        if not attachment_text:
            continue
        normalized_attachment = _normalize_for_compare(attachment_text)
        if not normalized_attachment or normalized_attachment in normalized_existing:
            continue
        normalized_existing.add(normalized_attachment)
        sections.append(
            f"Attachment: {attachment.filename}\n"
            f"Source: {attachment.source_url}\n"
            f"Content type: {attachment.content_type}\n\n"
            f"{attachment_text}"
        )

    return "\n\n".join(section for section in sections if section).strip()


def extract_analysis_sections(body_text: str, attachments: Iterable[DownloadedAttachment]) -> list[AnalysisSection]:
    sections: list[AnalysisSection] = []
    for source_kind, raw_text in _iter_analysis_sources(body_text, attachments):
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", raw_text) if part.strip()]
        for paragraph in paragraphs:
            normalized = paragraph.strip()
            section_kind = _classify_section_kind(normalized)
            priority_score = _priority_for_section(normalized, section_kind)
            if priority_score <= 0:
                continue
            label = _section_label(normalized, section_kind)
            sections.append(
                AnalysisSection(
                    index=len(sections) + 1,
                    source_kind=source_kind,
                    section_kind=section_kind,
                    label=label,
                    priority_score=priority_score,
                    text=normalized,
                )
            )
    return sorted(sections, key=lambda section: (-section.priority_score, section.index))


def segment_analysis_text(text: str, max_chars: int = 5000) -> list[AnalysisChunk]:
    stripped = (text or "").strip()
    if not stripped:
        return []

    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", stripped) if part.strip()]
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(paragraph) <= max_chars:
            current = paragraph
            continue
        lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
        current = ""
        for line in lines:
            line_candidate = line if not current else f"{current}\n{line}"
            if len(line_candidate) <= max_chars:
                current = line_candidate
            else:
                if current:
                    chunks.append(current)
                current = line
        if current and len(current) > max_chars:
            for start in range(0, len(current), max_chars):
                chunks.append(current[start : start + max_chars])
            current = ""
    if current:
        chunks.append(current)

    return [
        AnalysisChunk(
            index=idx,
            source_kind="analysis_text",
            label=f"chunk_{idx}",
            priority_score=0,
            section_labels=[f"chunk_{idx}"],
            text=chunk,
        )
        for idx, chunk in enumerate(chunks, start=1)
    ]


def build_prioritized_chunks(
    body_text: str,
    attachments: Iterable[DownloadedAttachment],
    max_chars: int = 5000,
) -> tuple[list[AnalysisSection], list[AnalysisChunk]]:
    sections = extract_analysis_sections(body_text, attachments)
    if not sections:
        analysis_text = build_analysis_text(body_text, attachments)
        return [], segment_analysis_text(analysis_text, max_chars=max_chars)

    high_priority = [section for section in sections if section.priority_score >= 80]
    medium_priority = [section for section in sections if 50 <= section.priority_score < 80]
    low_priority = [section for section in sections if 0 < section.priority_score < 50]
    ordered_sections = high_priority + medium_priority
    if not ordered_sections:
        ordered_sections = low_priority
    chunks = _sections_to_chunks(ordered_sections, max_chars=max_chars)
    return sections, chunks


def dedupe_extracted_events(events: Iterable[ExtractedEvent]) -> list[ExtractedEvent]:
    deduped: dict[tuple, ExtractedEvent] = {}
    for event in events:
        key = (
            _canonical_event_title(event.title or ""),
            event.start_at.isoformat() if event.start_at else "",
            event.end_at.isoformat() if event.end_at else "",
        )
        existing = deduped.get(key)
        if existing is None or _event_score(event) > _event_score(existing):
            deduped[key] = event
    return list(deduped.values())


def _canonical_event_title(value: str) -> str:
    normalized = (value or "").lower()
    normalized = re.sub(r"(?<=\w)['’]s\b", "", normalized)
    normalized = unicodedata.normalize("NFKD", normalized).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^\w\s-]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def maybe_extract_pdf_text(content: bytes) -> str:
    if not content:
        return ""
    attempts = [
        _extract_pdf_text_with_pymupdf,
        _extract_pdf_text_with_pdfplumber,
        _extract_pdf_text_with_pypdf,
        _extract_pdf_text_with_pypdf2,
        _extract_pdf_text_with_pdftotext,
    ]
    best_text = ""
    for extractor in attempts:
        try:
            extracted = extractor(content).strip()
        except Exception:
            extracted = ""
        if _extracted_text_length(extracted) >= MIN_EXTRACTED_TEXT_CHARS:
            return extracted
        if len(extracted) > len(best_text):
            best_text = extracted

    ocr_text = _ocr_pdf_with_openai(content).strip()
    if len(ocr_text) > len(best_text):
        return ocr_text
    return best_text


def _download_schoolmessenger_attachment(
    client: httpx.Client,
    response: httpx.Response,
    final_url: str,
) -> Optional[DownloadedAttachment]:
    parsed = urlparse(final_url)
    params = parse_qs(parsed.query)
    message_code = params.get("s", [None])[0]
    attachment_code = params.get("mal", [None])[0]
    html = response.text if "text/html" in (response.headers.get("content-type") or "") else ""
    if not message_code:
        match = re.search(r'id="message-link-code"[^>]*value="([^"]+)"', html)
        if match:
            message_code = match.group(1)
    if not attachment_code:
        match = re.search(r'id="attachment-link-code"[^>]*value="([^"]+)"', html)
        if match:
            attachment_code = match.group(1)
    if not message_code or not attachment_code:
        return None

    filename_match = re.search(r'id="filename">([^<]+)</', html)
    filename = filename_match.group(1).strip() if filename_match else "schoolmessenger.pdf"
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rsplit('/', 1)[0]}/"
    download = client.post(
        f"{base_url}requestdocument.php",
        data={"s": message_code, "mal": attachment_code, "p": None},
    )
    content_type = (download.headers.get("content-type") or "").split(";")[0].strip().lower()
    if download.status_code >= 400 or "pdf" not in content_type:
        return None
    extracted_text = maybe_extract_pdf_text(download.content)
    return DownloadedAttachment(
        filename=filename,
        content_type=content_type or "application/octet-stream",
        content=download.content,
        source_url=final_url,
        status_reason="downloaded_via_schoolmessenger_directlink",
        extracted_text=extracted_text,
    )


def _clean_body_text(body_text: str) -> str:
    text = (body_text or "").replace("\r", "")
    forwarded_context = _extract_forwarded_context(text)
    text = re.sub(
        r"^-+\s*Forwarded message\s*-+\n(?:.*\n){1,8}?\n",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\n-+\n.*SchoolMessenger.*", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"\nToronto District School Board would like to continue connecting with you.*", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"https?://[^\s<>()]+", "", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    cleaned = text.strip()
    if forwarded_context:
        cleaned = f"{forwarded_context}\n\n{cleaned}" if cleaned else forwarded_context
    return cleaned.strip()


def _iter_analysis_sources(body_text: str, attachments: Iterable[DownloadedAttachment]) -> list[tuple[str, str]]:
    sources: list[tuple[str, str]] = []
    cleaned_body = _clean_body_text(body_text)
    if cleaned_body:
        sources.append(("email_body", cleaned_body))
    seen = {_normalize_for_compare(cleaned_body)} if cleaned_body else set()
    for attachment in attachments:
        extracted_text = (attachment.extracted_text or "").strip()
        normalized = _normalize_for_compare(extracted_text)
        if not extracted_text or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        sources.append(("attachment_text", extracted_text))
    return sources


def _classify_section_kind(text: str) -> str:
    upper = text.upper()
    if "UPCOMING DATES" in upper:
        return "schedule"
    if re.search(r"(^|\n)[\u2022*-]\s", text):
        return "bullet_block"
    if re.search(r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\b", text, re.I):
        return "heading_block"
    if len(text.splitlines()) <= 3 and text.isupper():
        return "heading_block"
    return "narrative"


def _priority_for_section(text: str, section_kind: str) -> int:
    lowered = text.lower()
    if "unsubscribe" in lowered or "schoolmessenger is a notification service" in lowered:
        return 0
    if "toronto district school board would like to continue connecting with you" in lowered:
        return 0
    score = 10
    if section_kind == "schedule":
        score += 80
    elif section_kind == "bullet_block":
        score += 40
    elif section_kind == "heading_block":
        score += 30

    high_signal_terms = [
        "upcoming dates",
        "pizza lunch",
        "pa day",
        "school closed",
        "school closure",
        "swim",
        "open house",
        "book fair",
        "school council",
        "boo bash",
        "cross country",
        "photo day",
        "thanksgiving",
    ]
    medium_signal_terms = [
        "assembly",
        "concert",
        "meeting",
        "tournament",
        "schedule",
        "event",
        "webinar",
        "drop-in",
    ]
    score += sum(25 for term in high_signal_terms if term in lowered)
    score += sum(10 for term in medium_signal_terms if term in lowered)
    if re.search(r"\b\d{1,2}:\d{2}\b", text):
        score += 20
    if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec|january|february|march|april|june|july|august|september|october|november|december)\b", lowered):
        score += 15
    if re.search(r"(^|\n)(?:[\u2022*-]\s|[A-Z][A-Z ]{4,}$)", text, re.M):
        score += 10

    low_signal_terms = [
        "we hope",
        "thank you",
        "awareness month",
        "well-being",
        "mental health",
        "we are committed",
        "please join me in celebrating",
    ]
    if any(term in lowered for term in low_signal_terms):
        score -= 25
    return score


def _section_label(text: str, section_kind: str) -> str:
    first_line = text.splitlines()[0].strip()
    if section_kind == "schedule":
        return "UPCOMING DATES"
    return first_line[:80] or section_kind


def _sections_to_chunks(sections: Iterable[AnalysisSection], max_chars: int) -> list[AnalysisChunk]:
    chunks: list[AnalysisChunk] = []
    current_sections: list[AnalysisSection] = []
    current_text = ""

    def flush() -> None:
        nonlocal current_sections, current_text
        if not current_sections:
            return
        chunks.append(
            AnalysisChunk(
                index=len(chunks) + 1,
                source_kind=current_sections[0].source_kind,
                label=current_sections[0].label,
                priority_score=max(section.priority_score for section in current_sections),
                section_labels=[section.label for section in current_sections],
                text=current_text,
            )
        )
        current_sections = []
        current_text = ""

    for section in sections:
        candidate = section.text if not current_text else f"{current_text}\n\n{section.text}"
        same_tier = not current_sections or abs(current_sections[0].priority_score - section.priority_score) <= 20
        if current_text and (len(candidate) > max_chars or not same_tier):
            flush()
        if len(section.text) > max_chars:
            lines = [line.strip() for line in section.text.splitlines() if line.strip()]
            part = ""
            for line in lines:
                line_candidate = line if not part else f"{part}\n{line}"
                if len(line_candidate) <= max_chars:
                    part = line_candidate
                else:
                    if part:
                        chunks.append(
                            AnalysisChunk(
                                index=len(chunks) + 1,
                                source_kind=section.source_kind,
                                label=section.label,
                                priority_score=section.priority_score,
                                section_labels=[section.label],
                                text=part,
                            )
                        )
                    part = line
            if part:
                chunks.append(
                    AnalysisChunk(
                        index=len(chunks) + 1,
                        source_kind=section.source_kind,
                        label=section.label,
                        priority_score=section.priority_score,
                        section_labels=[section.label],
                        text=part,
                    )
                )
            continue
        current_sections.append(section)
        current_text = section.text if not current_text else f"{current_text}\n\n{section.text}"
    flush()
    return chunks


def _normalize_for_compare(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def _event_score(event: ExtractedEvent) -> tuple[int, int, int, int]:
    return (
        1 if event.start_at else 0,
        1 if event.end_at else 0,
        len(event.target_grades or []),
        len(event.mentioned_schools or []),
    )


def _extract_forwarded_context(text: str) -> str:
    match = re.search(
        r"^-+\s*Forwarded message\s*-+\n(?P<header>(?:.*\n){1,8}?)\n",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return ""

    header = match.group("header")
    preserved: list[str] = []
    for source_label, target_label in (
        ("From", "Original email from"),
        ("Date", "Original email date"),
        ("Subject", "Original email subject"),
        ("To", "Original email to"),
    ):
        line_match = re.search(rf"^{source_label}:\s*(.+)$", header, flags=re.MULTILINE)
        if line_match:
            preserved.append(f"{target_label}: {line_match.group(1).strip()}")
    return "\n".join(preserved).strip()


def _extract_pdf_text_with_pymupdf(content: bytes) -> str:
    try:
        import fitz
    except ImportError:
        return ""

    pages: list[str] = []
    with fitz.open(stream=content, filetype="pdf") as document:
        for page_index in range(document.page_count):
            page = document.load_page(page_index)
            page_text = (page.get_text("text") or "").strip()
            if page_text:
                pages.append(f"Page {page_index + 1}:\n{page_text}")
    return "\n\n".join(pages).strip()


def _extract_pdf_text_with_pdfplumber(content: bytes) -> str:
    try:
        import pdfplumber
    except ImportError:
        return ""

    pages: list[str] = []
    with pdfplumber.open(BytesIO(content)) as document:
        for page_index, page in enumerate(document.pages, start=1):
            page_text = (page.extract_text() or "").strip()
            if page_text:
                pages.append(f"Page {page_index}:\n{page_text}")
    return "\n\n".join(pages).strip()


def _extract_pdf_text_with_pypdf(content: bytes) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        return ""

    reader = PdfReader(BytesIO(content))
    pages: list[str] = []
    for page_index, page in enumerate(reader.pages, start=1):
        page_text = (page.extract_text() or "").strip()
        if page_text:
            pages.append(f"Page {page_index}:\n{page_text}")
    return "\n\n".join(pages).strip()


def _extract_pdf_text_with_pypdf2(content: bytes) -> str:
    try:
        from PyPDF2 import PdfReader
    except ImportError:
        return ""

    reader = PdfReader(BytesIO(content))
    pages: list[str] = []
    for page_index, page in enumerate(reader.pages, start=1):
        page_text = (page.extract_text() or "").strip()
        if page_text:
            pages.append(f"Page {page_index}:\n{page_text}")
    return "\n\n".join(pages).strip()


def _extract_pdf_text_with_pdftotext(content: bytes) -> str:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    try:
        result = subprocess.run(
            ["pdftotext", tmp_path, "-"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return ""
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _ocr_pdf_with_openai(content: bytes) -> str:
    try:
        import fitz
    except ImportError:
        return ""

    from app.config import settings

    if not settings.openai_api_key:
        return ""

    headers = {
        "Authorization": f"Bearer {settings.openai_api_key}",
        "Content-Type": "application/json",
    }
    pages: list[str] = []
    with fitz.open(stream=content, filetype="pdf") as document:
        for page_index in range(document.page_count):
            page = document.load_page(page_index)
            pixmap = page.get_pixmap(matrix=fitz.Matrix(300 / 72, 300 / 72))
            data_url = "data:image/png;base64," + base64.b64encode(pixmap.tobytes("png")).decode("ascii")
            payload = {
                "model": settings.openai_model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": OCR_PAGE_TEXT_PROMPT},
                            {"type": "image_url", "image_url": {"url": data_url, "detail": "high"}},
                        ],
                    }
                ],
            }
            if not settings.openai_model.startswith("gpt-5"):
                payload["temperature"] = 0
            with httpx.Client(timeout=settings.openai_timeout_sec) as client:
                response = client.post(
                    f"{settings.openai_base_url.rstrip('/')}/chat/completions",
                    headers=headers,
                    json=payload,
                )
            response.raise_for_status()
            page_text = response.json()["choices"][0]["message"]["content"].strip()
            if page_text:
                pages.append(f"Page {page_index + 1}:\n{page_text}")
    return "\n\n".join(pages).strip()


def _extracted_text_length(text: str) -> int:
    return len(re.sub(r"\s+", "", text or ""))
