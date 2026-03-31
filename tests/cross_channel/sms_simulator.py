from __future__ import annotations

from typing import Any, Callable, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.testclient import TestClient


class SmsSimulator:
    """Generates realistic parent SMS/email interactions for cross-channel testing.

    Two modes:
    - ``mock``: deterministic templates keyed by intent (no external calls)
    - ``llm``: calls OpenAI to produce natural parent text from conversation history
    """

    def __init__(
        self,
        client: "TestClient",
        *,
        sender_phone: str = "+15550000001",
        sender_email: str = "admin@example.com",
        recipient_phone: str = "+15551112222",
        mode: Literal["mock", "llm"] = "mock",
        llm_client: Any | None = None,
        llm_model: str = "gpt-4o-mini",
    ) -> None:
        self.client = client
        self.sender_phone = sender_phone
        self.sender_email = sender_email
        self.recipient_phone = recipient_phone
        self.mode = mode
        self.llm_client = llm_client
        self.llm_model = llm_model
        self._turn_counter = 0
        self._conversation_history: list[dict[str, str]] = []

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def send_sms(self, body_text: str) -> dict[str, Any]:
        """Post an SMS to the inbound webhook and return the JSON response."""
        self._turn_counter += 1
        payload = {
            "provider": "mock-sms",
            "provider_event_id": f"sim-sms-evt-{self._turn_counter}",
            "provider_message_id": f"sim-sms-msg-{self._turn_counter}",
            "sender_phone": self.sender_phone,
            "recipient_phone": self.recipient_phone,
            "body_text": body_text,
        }
        response = self.client.post(
            "/webhooks/sms/inbound",
            json=payload,
            headers={"x-signature": "local-dev-secret"},
        )
        result: dict[str, Any] = response.json()
        self._conversation_history.append({"role": "user", "channel": "sms", "text": body_text})
        self._conversation_history.append({"role": "assistant", "channel": "sms", "text": result.get("message", "")})
        return result

    def send_email(self, subject: str, body_text: str, **overrides: Any) -> dict[str, Any]:
        """Post an email to the inbound webhook and return the JSON response."""
        self._turn_counter += 1
        payload: dict[str, Any] = {
            "provider": "mock-email",
            "provider_event_id": f"sim-email-evt-{self._turn_counter}",
            "provider_message_id": f"sim-email-msg-{self._turn_counter}",
            "sender": self.sender_email,
            "recipient_alias": "schedule@example.com",
            "subject": subject,
            "body_text": body_text,
            **overrides,
        }
        response = self.client.post(
            "/webhooks/email/inbound",
            json=payload,
            headers={"x-signature": "local-dev-secret"},
        )
        result: dict[str, Any] = response.json()
        self._conversation_history.append({"role": "user", "channel": "email", "text": body_text})
        self._conversation_history.append({"role": "assistant", "channel": "email", "text": result.get("message", "")})
        return result

    def generate_followup_sms(self, intent: str, context_hint: str = "") -> str:
        """Generate a parent SMS message for the given intent.

        Mock mode returns deterministic templates.
        LLM mode calls OpenAI to produce a natural parent message.
        """
        if self.mode == "mock":
            return self._mock_generate(intent, context_hint)
        return self._llm_generate(intent, context_hint)

    def run_scenario(self, steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Execute a multi-step cross-channel scenario.

        Each step dict may contain:
        - ``channel``: ``"sms"`` or ``"email"``
        - ``body_text``: explicit message text
        - ``intent`` + ``context_hint``: generate text via :meth:`generate_followup_sms`
        - ``subject``: email subject (email only)
        - ``assertions``: optional list of ``callable(result)``
        """
        results: list[dict[str, Any]] = []
        for step in steps:
            if step["channel"] == "email":
                result = self.send_email(step.get("subject", ""), step["body_text"])
            elif "body_text" in step:
                result = self.send_sms(step["body_text"])
            else:
                text = self.generate_followup_sms(step["intent"], step.get("context_hint", ""))
                result = self.send_sms(text)
            results.append(result)
            for assertion in step.get("assertions", []):
                assertion(result)
        return results

    @property
    def history(self) -> list[dict[str, str]]:
        return list(self._conversation_history)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    _MOCK_TEMPLATES: dict[str, str] = {
        "more_info": "tell me more about {hint}",
        "add_event": "add {hint} to the calendar",
        "confirm": "yes",
        "deny": "no thanks",
        "ask_when": "when is {hint}",
        "set_preference": "always add {hint}",
        "delete": "delete {hint}",
        "remind": "remind me about {hint} 30m sms",
    }

    def _mock_generate(self, intent: str, context_hint: str) -> str:
        template = self._MOCK_TEMPLATES.get(intent, "{hint}")
        return template.format(hint=context_hint or "that")

    def _llm_generate(self, intent: str, context_hint: str) -> str:
        if self.llm_client is None:
            raise RuntimeError("SmsSimulator in llm mode requires an llm_client")
        system_prompt = (
            "You are simulating a busy parent who uses SMS to interact with a school "
            "calendar assistant called LovelyChaos. Generate a short, natural SMS message "
            "that a real parent would type on their phone. Keep it under 160 characters. "
            "Be casual, use abbreviations naturally, and don't be overly polished."
        )
        recent = self._conversation_history[-6:]
        conversation_context = "\n".join(
            f"[{turn['channel']}] {turn['role']}: {turn['text']}" for turn in recent
        )
        user_prompt = (
            f"Intent: {intent}\n"
            f"Topic: {context_hint}\n"
            f"Recent conversation:\n{conversation_context}\n\n"
            f"Generate the parent's next SMS message:"
        )
        response = self.llm_client.chat.completions.create(
            model=self.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=100,
            temperature=0.7,
        )
        return str(response.choices[0].message.content or "").strip()
