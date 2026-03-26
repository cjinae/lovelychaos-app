import asyncio

from sqlalchemy.orm import Session

from app.models import SourceMessage
from app.services.agent_threads import (
    DbBackedAgentSession,
    build_email_reply_headers,
    build_text_session_item,
    load_thread_documents,
    persist_thread_documents,
    resolve_email_thread_key,
)
from app.models import AgentSessionItem
from app.services.content_analysis import DownloadedAttachment


def test_resolve_email_thread_key_prefers_existing_message_thread(db_session):
    source = SourceMessage(
        provider="resend",
        provider_message_id="provider-root",
        source_channel="email",
        sender="admin@example.com",
        household_id=1,
        subject="Root thread",
        body_text="Root",
        internet_message_id="<root@example.com>",
        thread_key="<root@example.com>",
    )
    db_session.add(source)
    db_session.commit()

    thread_key = resolve_email_thread_key(
        db_session,
        household_id=1,
        internet_message_id="<reply@example.com>",
        in_reply_to_message_id="<root@example.com>",
        references_header="<root@example.com>",
        fallback_key="fallback-key",
    )

    assert thread_key == "<root@example.com>"


def test_build_email_reply_headers_includes_references_chain():
    source = SourceMessage(
        provider="resend",
        provider_message_id="provider-child",
        source_channel="email",
        sender="admin@example.com",
        household_id=1,
        subject="Child thread",
        body_text="Child",
        internet_message_id="<child@example.com>",
        references_header="<root@example.com> <mid@example.com>",
    )

    headers = build_email_reply_headers(source)

    assert headers["In-Reply-To"] == "<child@example.com>"
    assert headers["References"] == "<root@example.com> <mid@example.com> <child@example.com>"


def test_thread_documents_round_trip(db_session, monkeypatch):
    source = SourceMessage(
        provider="resend",
        provider_message_id="provider-doc",
        source_channel="email",
        sender="admin@example.com",
        household_id=1,
        subject="Doc thread",
        body_text="Doc",
        thread_key="<doc@example.com>",
    )
    db_session.add(source)
    db_session.flush()
    monkeypatch.setattr("app.services.agent_threads._upload_openai_file", lambda **kwargs: None)

    persist_thread_documents(
        db_session,
        household_id=1,
        source_message_id=source.id,
        thread_key=source.thread_key,
        attachments=[
            DownloadedAttachment(
                filename="newsletter.pdf",
                content_type="application/pdf",
                content=b"%PDF-1.4 test",
                source_url="https://example.com/newsletter.pdf",
                status_reason="downloaded",
                extracted_text="Page 1:\nNewsletter details",
            )
        ],
    )
    db_session.commit()

    documents = load_thread_documents(db_session, household_id=1, thread_key=source.thread_key)

    assert len(documents) == 1
    assert documents[0].filename == "newsletter.pdf"
    assert "Newsletter details" in documents[0].extracted_text


def test_build_text_session_item_uses_output_text_for_assistant():
    item = build_text_session_item(role="assistant", text="Hello")

    assert item["content"] == [{"type": "output_text", "text": "Hello"}]


def test_db_backed_session_normalizes_legacy_assistant_input_text(db_session):
    db_session.add(
        AgentSessionItem(
            session_id="email:1:test-thread",
            payload={"role": "assistant", "content": [{"type": "input_text", "text": "Legacy reply"}]},
        )
    )
    db_session.commit()

    session = DbBackedAgentSession(
        "email:1:test-thread",
        db_session_factory=lambda: Session(bind=db_session.bind),
    )
    items = asyncio.run(session.get_items())

    assert items == [{"role": "assistant", "content": [{"type": "output_text", "text": "Legacy reply"}]}]
