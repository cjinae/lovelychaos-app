from datetime import datetime, timedelta, timezone

from app.models import FollowupContext, SourceMessage, ThreadDocument
from app.services.followups import FollowupMatch, assess_more_info_context, retrieve_more_info_source_snippets


def test_assess_more_info_context_marks_document_only_match_as_weak(db_session):
    source = SourceMessage(
        provider="mock-email",
        provider_message_id="source-doc-only",
        source_channel="email",
        sender="admin@example.com",
        household_id=1,
        subject="Frankland Update",
        body_text="Please see the attached newsletter for details.",
        internet_message_id="source-doc-only",
        thread_key="source-doc-only",
    )
    db_session.add(source)
    db_session.flush()
    context = FollowupContext(
        household_id=1,
        source_message_id=source.id,
        origin_channel="email",
        response_channel="sms",
        thread_or_conversation_key="source-doc-only",
        summary_title="Frankland Update",
        summary_items_shown=[],
        all_extracted_items=[],
        section_snippets=[
            {
                "label": "assistant_summary",
                "text": "This looks like a club update, but the saved follow-up context is thin.",
                "meta": "document_understanding",
            },
            {
                "label": "club",
                "text": "Chess club mention in the newsletter.",
                "meta": "document_topic",
            },
        ],
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
    )

    assessment = assess_more_info_context(
        context,
        FollowupMatch(
            item={
                "item_id": "brick-labs",
                "title": "BRICK LABS INC. VIB Chess Club",
                "display_text": "BRICK LABS INC. VIB Chess Club",
                "assistant_detail": "Chess club mention in the newsletter.",
                "source_refs": ["document_understanding:informational_topics:1"],
            },
            from_summary=False,
            score=10,
        ),
    )

    assert assessment.weak is True
    assert assessment.reason == "matched_topic_only_has_document_understanding_context"
    assert assessment.stored_source_snippets == []


def test_retrieve_more_info_source_snippets_reads_thread_documents(db_session):
    source = SourceMessage(
        provider="mock-email",
        provider_message_id="source-thread-doc",
        source_channel="email",
        sender="admin@example.com",
        household_id=1,
        subject="Frankland Update",
        body_text="Please see the attached newsletter for details.",
        internet_message_id="source-thread-doc",
        thread_key="source-thread-doc",
    )
    db_session.add(source)
    db_session.flush()
    db_session.add(
        ThreadDocument(
            household_id=1,
            source_message_id=source.id,
            thread_key=source.thread_key,
            filename="newsletter.pdf",
            content_type="application/pdf",
            extracted_text=(
                "BRICK LABS INC. VIB Chess Club runs on Wednesdays after school from 3:30 PM to 4:30 PM in Room 204. "
                "Registration closes on April 3."
            ),
        )
    )
    context = FollowupContext(
        household_id=1,
        source_message_id=source.id,
        origin_channel="email",
        response_channel="sms",
        thread_or_conversation_key="source-thread-doc",
        summary_title="Frankland Update",
        summary_items_shown=[],
        all_extracted_items=[],
        section_snippets=[],
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
    )
    db_session.commit()

    snippets = retrieve_more_info_source_snippets(
        db_session,
        context=context,
        match=FollowupMatch(
            item={
                "item_id": "brick-labs",
                "title": "BRICK LABS INC. VIB Chess Club",
                "display_text": "BRICK LABS INC. VIB Chess Club",
                "aliases": ["Brick Labs chess club"],
            },
            from_summary=False,
            score=10,
        ),
        query_text="tell me more about BRICK LABS INC. VIB Chess Club",
    )

    assert snippets
    assert any("Room 204" in snippet for snippet in snippets)
    assert any("April 3" in snippet for snippet in snippets)
