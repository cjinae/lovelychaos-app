from dataclasses import dataclass
import os

from dotenv import load_dotenv


# Load local secrets/config from .env when present.
load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./lovelychaos.db")
    webhook_secret: str = os.getenv("WEBHOOK_SECRET", "local-dev-secret")
    default_timezone: str = "UTC"
    google_calendar_mode: str = os.getenv("GOOGLE_CALENDAR_MODE", "mock")
    google_calendar_timeout_sec: int = int(os.getenv("GOOGLE_CALENDAR_TIMEOUT_SEC", "10"))
    google_client_id: str = os.getenv("GOOGLE_CLIENT_ID", "")
    google_client_secret: str = os.getenv("GOOGLE_CLIENT_SECRET", "")
    google_oauth_redirect_uri: str = os.getenv(
        "GOOGLE_OAUTH_REDIRECT_URI",
        "http://localhost:8000/oauth/google/callback",
    )
    notification_mode: str = os.getenv("NOTIFICATION_MODE", "mock")
    resend_api_key: str = os.getenv("RESEND_API_KEY", "")
    resend_from_email: str = os.getenv("RESEND_FROM_EMAIL", "")
    resend_webhook_secret: str = os.getenv("RESEND_WEBHOOK_SECRET", "")
    twilio_account_sid: str = os.getenv("TWILIO_ACCOUNT_SID", "")
    twilio_auth_token: str = os.getenv("TWILIO_AUTH_TOKEN", "")
    twilio_messaging_service_sid: str = os.getenv("TWILIO_MESSAGING_SERVICE_SID", "")
    twilio_phone_number: str = os.getenv("TWILIO_PHONE_NUMBER", "")
    llm_mode: str = os.getenv("LLM_MODE", "mock")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
    openai_reasoning_effort: str = os.getenv("OPENAI_REASONING_EFFORT", "medium")
    openai_tracing_enabled: bool = os.getenv("OPENAI_TRACING_ENABLED", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    openai_store_responses: bool = os.getenv("OPENAI_STORE_RESPONSES", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    openai_timeout_sec: int = int(os.getenv("OPENAI_TIMEOUT_SEC", "20"))
    openai_base_url: str = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    local_test_response_channel_override: str = os.getenv("LOCAL_TEST_RESPONSE_CHANNEL_OVERRIDE", "").strip().lower()
    local_test_admin_email: str = os.getenv("LOCAL_TEST_ADMIN_EMAIL", "").strip().lower()
    admin_api_key: str = os.getenv("ADMIN_API_KEY", "")


settings = Settings()
