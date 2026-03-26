import json

from sqlalchemy import select

from app.models import DecisionAudit
from app.services.attribution import AttributionResult
from app.services.llm import MockDecisionEngine


class _KnowledgeAwareEngine(MockDecisionEngine):
    def __init__(self):
        super().__init__()
        self.summary_context = None

    def extract_events(
        self,
        body_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
    ):
        return {"events": [], "email_level_notes": None}

    def extract_summary_candidates(self, summary_context: dict) -> dict:
        self.summary_context = summary_context
        return super().extract_summary_candidates(summary_context)


def test_webhook_summary_includes_school_knowledge_context_and_audit(client, session_factory, monkeypatch):
    import app.main as main_module

    engine = _KnowledgeAwareEngine()
    monkeypatch.setattr(main_module, "engine_llm", engine)
    with session_factory() as db:
        seeded_user = db.scalar(select(main_module.User).where(main_module.User.email == "admin@example.com"))
    monkeypatch.setattr(main_module, "resolve_admin_sender", lambda db, sender: AttributionResult("ok", seeded_user))

    payload = {
        "provider": "mock-email",
        "provider_event_id": "evt-movie-night",
        "provider_message_id": "msg-movie-night",
        "sender": "admin@example.com",
        "recipient_alias": "schedule@example.com",
        "subject": "Family Movie Night",
        "body_text": (
            "UPCOMING DATES\n"
            "Thursday January 15 - Family Movie Night in the school gym.\n"
            "Doors open at 5:45 PM and the movie starts at 6:00 PM."
        ),
    }

    response = client.post(
        "/webhooks/email/inbound",
        json=payload,
        headers={"x-signature": main_module.settings.webhook_secret},
    )

    assert response.status_code == 200
    assert engine.summary_context is not None
    assert "parent engagement" in engine.summary_context["domain_taxonomy_hints"]
    assert engine.summary_context["retrieved_examples"][0]["entry_id"] == "movie-night"

    with session_factory() as db:
        audit = db.scalar(select(DecisionAudit).order_by(DecisionAudit.id.desc()))

    assert audit is not None
    knowledge = audit.model_output["summary"]["knowledge_retrieval"]
    assert knowledge["matches"][0]["entry_id"] == "movie-night"
    assert "retrieved_examples" not in knowledge
    assert "snippet" not in json.dumps(knowledge)
