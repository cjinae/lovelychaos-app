"""P1: Verify SMS flow can access email-originated ThreadDocuments."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models import ThreadDocument, SourceMessage
from app.services.agent_threads import load_recent_household_documents


def test_sms_sees_recent_household_documents(db_session, sms_sim, seed_thread_document):
    """SMS flow should have access to documents created via email ingestion."""
    doc = seed_thread_document(
        filename="march_newsletter.pdf",
        extracted_text="Chess Club meets Wednesdays 3:30-4:30 in Room 204.",
    )

    docs = load_recent_household_documents(db_session, household_id=1, limit=3)
    assert len(docs) >= 1
    assert any("Chess Club" in d.extracted_text for d in docs)


def test_recent_documents_limited_to_7_days(db_session, seed_thread_document):
    """Documents older than 7 days should not appear in recent household docs."""
    # Create an old document by directly manipulating created_at
    old_doc = seed_thread_document(
        filename="old_newsletter.pdf",
        extracted_text="Old content that should not appear.",
        thread_key="msg-old-doc",
    )
    old_doc.created_at = datetime.now(timezone.utc) - timedelta(days=10)
    db_session.commit()

    # Create a recent document
    seed_thread_document(
        filename="recent_newsletter.pdf",
        extracted_text="Recent content that should appear.",
        thread_key="msg-recent-doc",
    )

    docs = load_recent_household_documents(db_session, household_id=1, limit=10)
    texts = [d.extracted_text for d in docs]
    assert any("Recent content" in t for t in texts)
    assert not any("Old content" in t for t in texts)


def test_sms_conversation_scope_receives_documents(client, db_session, sms_sim, seed_thread_document, monkeypatch):
    """The SMS inbound flow should pass recent household documents into
    ``_conversation_runtime_scope`` instead of an empty list."""
    import app.main as main_module

    seed_thread_document(
        filename="spring_newsletter.pdf",
        extracted_text="Spring Fair on May 1 from 10am to 2pm.",
    )

    captured_docs: list = []
    original_scope = main_module._conversation_runtime_scope

    from contextlib import contextmanager

    @contextmanager
    def capturing_scope(*, source, thread_documents, household_context=None):
        captured_docs.extend(thread_documents)
        with original_scope(source=source, thread_documents=thread_documents, household_context=household_context):
            yield

    monkeypatch.setattr(main_module, "_conversation_runtime_scope", capturing_scope)

    sms_sim.send_sms("tell me more about Spring Fair")

    assert len(captured_docs) > 0
    assert any("Spring Fair" in d.extracted_text for d in captured_docs)
