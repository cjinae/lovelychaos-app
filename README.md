# LovelyChaos

LovelyChaos is a FastAPI app for handling school communication on behalf of a family. It is meant to behave like a helpful assistant, not just a date or event operator: it ingests inbound email and SMS, explains what matters in plain language, turns clear schedule items into calendar actions when appropriate, stores informational updates for digesting, and keeps enough conversational context to support follow-up replies like "tell me more", "add this", "delete 42", or "please keep adding pizza lunches".

The current product is no longer a "Phase 1" stub. The codebase now includes live provider integrations, a real onboarding/admin surface, preference-aware summarization, teacher-contact matching, PDF/attachment processing, Google Calendar OAuth, and SMS follow-up flows.

## Current product surface

- Inbound channels:
  - Email via `POST /webhooks/email/inbound`
  - Resend inbound email via `POST /webhooks/resend/inbound`
  - SMS via `POST /webhooks/sms/inbound`
  - Twilio inbound SMS via `POST /webhooks/twilio/sms`
- Fail-closed attribution:
  - verified admin email is required for email commands and ingestion
  - verified primary admin phone is required for SMS
  - ambiguous senders are rejected
- School-update processing:
  - extracts structured events and informational items from school emails
  - handles forwarded teacher emails and preserves original sender/date context
  - downloads supported SchoolMessenger PDF attachments and extracts text
  - can fall back to OpenAI-powered OCR for weak PDF text extraction
- Decisioning and summarization:
  - combines deterministic policy logic with LLM extraction/parsing
  - builds concise recaps with important dates, important items, and other topics
  - uses a local redacted school-communication corpus to improve summary context
  - respects household priority defaults, explicit preference notes, and command-written preference rules
- Calendar behavior:
  - auto-adds clear, relevant events such as closures/breaks and household-specific preference events
  - holds ambiguous or incomplete items for follow-up instead of guessing
  - supports add, delete, and reminder workflows
  - supports mock and live Google Calendar providers
- Conversational follow-up:
  - email and SMS can request `add`, `more_info`, `delete`, `remind`, and `set_preference`
  - `more_info` replies should feel assistant-like and grounded, paraphrasing the school update instead of copying source text back to the parent
  - SMS keeps short-lived numbered disambiguation state when a reply could refer to multiple recent items
  - follow-up context is persisted so later replies can resolve "this", topic names, or numbered selections
- Operator surfaces:
  - onboarding flow for family profile, children, schools, priority profile, and calendar connection
  - admin console for settings, children, teacher contacts, preferences, calendar binding, digests, notifications, and inbound activity
  - design-language and architecture-diagram pages for internal review

## Stack

- FastAPI
- SQLAlchemy
- SQLite for local development, Postgres via `DATABASE_URL` in deployment
- Jinja templates for onboarding/admin UI
- OpenAI Responses API for live extraction, command parsing, preference parsing, summary compression, and PDF OCR
- Google Calendar API for live calendar mutations
- Resend for inbound email normalization and live outbound email
- Twilio for inbound and outbound SMS

## Run locally

```bash
python3 -m pip install -e '.[dev]'
make db-upgrade
python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

Useful local pages:

- `http://127.0.0.1:8000/onboarding`
- `http://127.0.0.1:8000/admin`
- `http://127.0.0.1:8000/architecture-diagrams`
- `http://127.0.0.1:8000/architecture-diagrams-agentsdk`

## Environment variables

Copy `.env.example` to `.env`. The app auto-loads `.env` at startup.

Core:

- `DATABASE_URL`
- `WEBHOOK_SECRET`
- `ADMIN_API_KEY` to require `X-Admin-Key` on `/internal/*`

LLM:

- `LLM_MODE=mock|openai`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `OPENAI_REASONING_EFFORT`
- `OPENAI_TRACING_ENABLED`
- `OPENAI_STORE_RESPONSES`
- `OPENAI_TIMEOUT_SEC`
- `OPENAI_BASE_URL`

Calendar:

- `GOOGLE_CALENDAR_MODE=mock|live`
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_OAUTH_REDIRECT_URI`

Notifications and provider webhooks:

- `NOTIFICATION_MODE=mock|live`
- `RESEND_API_KEY`
- `RESEND_FROM_EMAIL`
- `RESEND_WEBHOOK_SECRET`
- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`
- `TWILIO_MESSAGING_SERVICE_SID` or `TWILIO_PHONE_NUMBER`

Testing override:

- `LOCAL_TEST_RESPONSE_CHANNEL_OVERRIDE`

## Migrations

```bash
make db-upgrade
make db-downgrade
make db-revision m="describe change"
```

Recent schema changes in the current product include SMS conversation state, teacher contacts, and removal of older batch/pending routing concepts.

## Main routes

Inbound and command processing:

- `POST /webhooks/email/inbound`
- `POST /webhooks/resend/inbound`
- `POST /webhooks/sms/inbound`
- `POST /webhooks/twilio/sms`
- `GET /operations/{operation_id}`
- `POST /internal/operations/{operation_id}/run`

Jobs and platform ops:

- `POST /internal/jobs/retention`
- `POST /internal/jobs/daily-summary`
- `POST /internal/jobs/weekly-digest`
- `POST /internal/google/refresh`
- `GET /health`
- `GET /ready`

Google Calendar OAuth:

- `GET /auth/google/start`
- `GET /oauth/google/callback`

Admin JSON APIs:

- `GET/PUT /admin/profile`
- `GET/PUT /admin/settings`
- `GET/POST /admin/children`
- `GET /admin/schools/search`
- `GET /admin/schools/resolve`
- `GET/PUT /admin/preferences`
- `GET/PUT /admin/calendar-binding`
- `GET/POST /admin/reminders`
- `GET /admin/notifications`
- `GET /admin/digests`
- `GET /admin/inbound-activity`

HTML surfaces:

- `GET /`
- `GET /admin`
- `GET /admin/activity`
- `GET /onboarding`
- `GET /onboarding/design-gallery`
- `GET /onboarding/v1`
- `GET /onboarding/v2`
- `GET /onboarding/v3`
- `GET /design-language`
- `GET /architecture-diagrams`
- `GET /architecture-diagrams-agentsdk`

## Supported follow-up commands

Examples the current parser handles:

- `add this to the calendar`
- `please add Family Field Trip to the calendar for May 9, 2026`
- `tell me more about Space Pirates musical`
- `delete 42`
- `remind 42 30m sms`
- `remind 42 45m calendar`
- `please keep adding pizza lunches`
- `I don't care about school council events`

Supported actions are:

- `add`
- `more_info`
- `delete`
- `remind`
- `set_preference`

Older "confirm" style commands are not part of the current command model.

## Integration notes

LLM modes:

- `LLM_MODE=mock` uses deterministic local logic for extraction and command parsing.
- `LLM_MODE=openai` uses the OpenAI Responses API with strict JSON-schema outputs.

Google Calendar modes:

- `GOOGLE_CALENDAR_MODE=mock` avoids live Google API calls.
- `GOOGLE_CALENDAR_MODE=live` performs real calendar mutations and refreshes stored tokens when possible.

Notification modes:

- `NOTIFICATION_MODE=mock` records deterministic outbound deliveries.
- `NOTIFICATION_MODE=live` sends email through Resend and SMS through Twilio.

Resend inbound webhook:

- Configure Resend to call `POST /webhooks/resend/inbound`.
- The handler normalizes common inbound payload shapes.
- If `RESEND_WEBHOOK_SECRET` is set, Svix headers are verified. Local fallback also supports `X-Resend-Signature`.

Twilio inbound SMS webhook:

- Configure Twilio to call `POST /webhooks/twilio/sms`.
- Twilio form fields are normalized into the shared SMS command pipeline.
- If `TWILIO_AUTH_TOKEN` is set, `X-Twilio-Signature` is required.

## Testing

```bash
make ci
```

See `TESTING.md` for suite definitions and merge policy.
