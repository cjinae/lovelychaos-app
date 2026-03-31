from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.models import FollowupContext, SourceMessage, ThreadDocument
from tests.cross_channel.sms_simulator import SmsSimulator


# ------------------------------------------------------------------
# Mock-mode simulator (always available)
# ------------------------------------------------------------------

@pytest.fixture()
def sms_sim(client):
    """An :class:`SmsSimulator` in mock mode, bound to the test client."""
    return SmsSimulator(client, mode="mock")


# ------------------------------------------------------------------
# LLM-mode simulator (opt-in via env var)
# ------------------------------------------------------------------

@pytest.fixture()
def sms_sim_llm(client, monkeypatch):
    """An :class:`SmsSimulator` backed by a real LLM.

    Skipped unless ``LOVELYCHAOS_E2E_LLM=1`` is set.  Also replaces
    ``engine_llm`` with a real :class:`OpenAIDecisionEngine` so that
    the app itself uses the live model.
    """
    if not os.getenv("LOVELYCHAOS_E2E_LLM"):
        pytest.skip("LOVELYCHAOS_E2E_LLM not set")

    from openai import OpenAI

    from app.config import settings
    from app.services.llm import OpenAIDecisionEngine
    import app.main as main_module

    real_engine = OpenAIDecisionEngine(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        reasoning_effort=settings.openai_reasoning_effort,
        timeout_sec=settings.openai_timeout_sec,
        base_url=settings.openai_base_url,
        store_responses=settings.openai_store_responses,
    )
    monkeypatch.setattr(main_module, "engine_llm", real_engine)

    llm_client = OpenAI(api_key=settings.openai_api_key)
    return SmsSimulator(client, mode="llm", llm_client=llm_client)


# ------------------------------------------------------------------
# Seed helpers
# ------------------------------------------------------------------

@pytest.fixture()
def seed_email_with_followup(db_session):
    """Factory fixture: creates a SourceMessage + FollowupContext from an email.

    Returns a callable ``(summary_items, actionable_items, **kw) -> FollowupContext``.
    """
    _counter = [0]

    def _factory(
        summary_items_shown: list[dict],
        actionable_items: list[dict],
        *,
        body_text: str = "School newsletter content.",
        subject: str = "Frankland Newsletter",
        response_channel: str = "email",
        thread_key: str | None = None,
    ) -> FollowupContext:
        _counter[0] += 1
        tk = thread_key or f"msg-xc-followup-{_counter[0]}"
        source = SourceMessage(
            provider="mock-email",
            provider_message_id=tk,
            source_channel="email",
            sender="admin@example.com",
            household_id=1,
            subject=subject,
            body_text=body_text,
            internet_message_id=tk,
            thread_key=tk,
        )
        db_session.add(source)
        db_session.flush()
        context = FollowupContext(
            household_id=1,
            source_message_id=source.id,
            origin_channel="email",
            response_channel=response_channel,
            thread_or_conversation_key=tk,
            summary_title=subject,
            summary_items_shown=summary_items_shown,
            all_extracted_items=actionable_items,
            section_snippets=[{"label": "newsletter", "text": body_text}],
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        )
        db_session.add(context)
        db_session.commit()
        return context

    return _factory


@pytest.fixture()
def seed_thread_document(db_session):
    """Factory fixture: creates a ThreadDocument linked to an email.

    Returns a callable ``(filename, extracted_text, **kw) -> ThreadDocument``.
    """
    _counter = [0]

    def _factory(
        filename: str = "newsletter.pdf",
        extracted_text: str = "Extracted document content.",
        *,
        content_type: str = "application/pdf",
        thread_key: str | None = None,
    ) -> ThreadDocument:
        _counter[0] += 1
        tk = thread_key or f"msg-xc-doc-{_counter[0]}"
        source = db_session.scalar(
            select(SourceMessage).where(SourceMessage.thread_key == tk)
        )
        if source is None:
            source = SourceMessage(
                provider="mock-email",
                provider_message_id=tk,
                source_channel="email",
                sender="admin@example.com",
                household_id=1,
                subject="Doc source",
                body_text="See attached.",
                internet_message_id=tk,
                thread_key=tk,
            )
            db_session.add(source)
            db_session.flush()
        doc = ThreadDocument(
            household_id=1,
            source_message_id=source.id,
            thread_key=tk,
            filename=filename,
            content_type=content_type,
            source_url="",
            extracted_text=extracted_text,
        )
        db_session.add(doc)
        db_session.commit()
        return doc

    return _factory
