import httpx

from app.services.content_analysis import (
    build_prioritized_chunks,
    build_analysis_text,
    dedupe_extracted_events,
    extract_analysis_sections,
    extract_candidate_links,
    resolve_and_download_links,
    segment_analysis_text,
)
from app.services.llm import ExtractedEvent


def test_extract_candidate_links_finds_schoolmessenger_links():
    body = (
        "Hello\n"
        "https://track.spe.schoolmessenger.com/f/a/example\n"
        "https://example.com/other\n"
    )
    links = extract_candidate_links(body)
    assert "https://track.spe.schoolmessenger.com/f/a/example" in links
    assert "https://example.com/other" in links


def test_resolve_and_download_links_downloads_schoolmessenger_pdf(monkeypatch):
    def fake_pdf_text(_content: bytes) -> str:
        return "Open House on 2025-10-07"

    monkeypatch.setattr("app.services.content_analysis.maybe_extract_pdf_text", fake_pdf_text)

    html = """
    <html>
      <span id="filename">Frankland Newsletter.pdf</span>
      <input type="hidden" id="message-link-code" value="abc123">
      <input type="hidden" id="attachment-link-code" value="def456">
    </html>
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "track.spe.schoolmessenger.com":
            return httpx.Response(
                302,
                headers={"Location": "https://msg.schoolmessenger.ca/m/?s=abc123&mal=def456"},
            )
        if request.url.host == "msg.schoolmessenger.ca" and request.method == "GET":
            return httpx.Response(200, text=html, headers={"Content-Type": "text/html; charset=UTF-8"})
        if request.url.host == "msg.schoolmessenger.ca" and request.method == "POST":
            return httpx.Response(200, content=b"%PDF-1.4 test", headers={"Content-Type": "application/pdf"})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True)
    report = resolve_and_download_links(
        ["https://track.spe.schoolmessenger.com/f/a/example"],
        client=client,
    )
    assert len(report.attachments) == 1
    assert report.attachments[0].filename == "Frankland Newsletter.pdf"
    assert report.attachments[0].status_reason == "downloaded_via_schoolmessenger_directlink"
    assert report.attachments[0].extracted_text == "Open House on 2025-10-07"
    assert report.attempts[0].outcome == "downloaded"


def test_build_analysis_text_strips_footer_and_dedupes_attachment_text():
    body = (
        "---------- Forwarded message ---------\n"
        "From: School <donotreply@example.com>\n"
        "Date: Sun, Oct 5, 2025 at 6:07 PM\n"
        "Subject: Sunday, October 5/25 Frankland Newsletter\n"
        "To: parent@example.com\n\n"
        "Upcoming Dates\nOpen House on October 7.\n\n"
        "Toronto District School Board would like to continue connecting with you\n"
    )
    text = build_analysis_text(body, [])
    assert "Forwarded message" not in text
    assert "Original email date: Sun, Oct 5, 2025 at 6:07 PM" in text
    assert "Original email subject: Sunday, October 5/25 Frankland Newsletter" in text
    assert "Toronto District School Board would like to continue connecting with you" not in text
    assert "Open House on October 7." in text


def test_segment_analysis_text_splits_long_newsletter():
    text = "\n\n".join([f"Section {idx}: " + ("event " * 400) for idx in range(1, 5)])
    chunks = segment_analysis_text(text, max_chars=1200)
    assert len(chunks) >= 2
    assert all(len(chunk.text) <= 1200 for chunk in chunks)


def test_extract_analysis_sections_prioritizes_upcoming_dates():
    body = (
        "Hello Frankland Families.\n\n"
        "UPCOMING DATES\n"
        "PIZZA LUNCH - October 1, 15, 29\n"
        "October 10 - PA Day (no school for students)\n"
        "October 13 - Thanksgiving (Holiday -- school closed)\n\n"
        "We hope you had a lovely weekend."
    )
    sections = extract_analysis_sections(body, [])
    assert sections
    assert sections[0].label == "UPCOMING DATES"
    assert sections[0].priority_score >= 80
    assert any("PA Day" in section.text for section in sections)


def test_build_prioritized_chunks_prefers_schedule_sections_first():
    body = (
        "Hello Frankland Families.\n\n"
        "UPCOMING DATES\n"
        "PIZZA LUNCH - October 1, 15, 29\n"
        "October 10 - PA Day (no school for students)\n"
        "October 13 - Thanksgiving (Holiday -- school closed)\n\n"
        + ("We hope you had a lovely weekend. " * 300)
    )
    sections, chunks = build_prioritized_chunks(body, [], max_chars=1000)
    assert sections
    assert chunks
    assert chunks[0].priority_score >= chunks[-1].priority_score
    assert "UPCOMING DATES" in chunks[0].section_labels


def test_dedupe_extracted_events_normalizes_possessive_title_variants():
    event_one = ExtractedEvent(
        title="Frankland’s Spring Swap",
        start_at=None,
        end_at=None,
        category="school",
        confidence=0.9,
        model_reason="explicit",
    )
    event_two = ExtractedEvent(
        title="Frankland Spring Swap",
        start_at=None,
        end_at=None,
        category="fundraiser",
        confidence=0.95,
        model_reason="explicit",
    )

    deduped = dedupe_extracted_events([event_one, event_two])
    assert len(deduped) == 1
    assert deduped[0].title == "Frankland’s Spring Swap" or deduped[0].title == "Frankland Spring Swap"
