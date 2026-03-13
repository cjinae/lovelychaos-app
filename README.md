# LovelyChaos Phase 1

Phase 1 implementation for:
- Email-first ingestion and command handling
- Deterministic attribution fail-closed gates
- LLM decision interface + deterministic validation/routing
- Pending decisions and operation tracking
- Minimal Admin Console
- Mandatory automated testing and CI gates

## Stack
- FastAPI
- SQLAlchemy
- SQLite (local/dev), Postgres via `DATABASE_URL` in deployment

## Run locally

```bash
python3 -m pip install -e '.[dev]'
make db-upgrade
uvicorn app.main:app --reload
```

## Environment variables (.env)
- Copy `.env.example` to `.env` and fill in your secrets.
- The app now auto-loads `.env` at startup.
- `.env` is gitignored.
- Set `ADMIN_API_KEY` to require `X-Admin-Key` on all `/internal/*` endpoints.
- Set `RESEND_WEBHOOK_SECRET` to verify Resend webhook signatures on `POST /webhooks/resend/inbound`.
- LLM mode defaults to `mock`; set `LLM_MODE=openai` with `OPENAI_API_KEY` to use live OpenAI prompts.

## Database migrations

```bash
make db-upgrade
make db-downgrade
make db-revision m="describe change"
```

## Core endpoints
- `POST /webhooks/email/inbound`
- `POST /webhooks/resend/inbound`
- `POST /webhooks/sms/inbound`
- `POST /webhooks/twilio/sms`
- `GET /operations/{operation_id}`
- `POST /internal/operations/{operation_id}/run`
- `POST /internal/jobs/expire`
- `POST /internal/jobs/retention`
- `POST /internal/jobs/daily-summary`
- `POST /internal/jobs/weekly-digest`
- `POST /internal/google/refresh`
- `GET /health`
- `GET /ready`
- `GET /admin`
- `GET /admin/activity`
- `GET /admin/inbound-activity`
- `GET /onboarding`
- `GET/PUT /admin/profile`
- `GET/PUT /admin/settings`
- `GET/POST /admin/children`
- `GET/PUT /admin/preferences`
- `GET/PUT /admin/calendar-binding`
- `GET /admin/pending-events`
- `GET /admin/reminders`
- `POST /admin/reminders`
- `GET /admin/notifications`
- `GET /admin/digests`
- `POST /admin/pending-events/{id}/confirm`
- `POST /admin/pending-events/{id}/reject`

## Test and CI gates

```bash
make ci
```

See `TESTING.md` for suite definitions and merge policy.

## Notes
- Async command operations use bounded completion-notification retries (max 3 attempts).
- `GET /operations/{operation_id}` remains queryable after retry exhaustion.
- Phase 2 adds Google Calendar mutation support (create/delete) with strict commit-time tenant gating.
- Phase 2 now includes SMS command channel with ambiguous-phone fail-closed routing and spouse receive-only enforcement.
- Reminder commands are supported via email and SMS (`remind <event_id> <minutes>m sms|calendar`).
- Daily summary and weekly digest jobs are available and respect household toggle settings.

## Google Calendar modes
- `GOOGLE_CALENDAR_MODE=mock` (default): deterministic local testing, no Google API call.
- `GOOGLE_CALENDAR_MODE=live`: calls Google Calendar REST API using stored access token and calendar binding.
- Access tokens are auto-refreshed before calendar mutations when `token_expiry` is near/past expiry and a `refresh_token` is available.

## Notification modes
- `NOTIFICATION_MODE=mock` (default): deterministic outbound notification simulation with delivery logs.
- `NOTIFICATION_MODE=live`: uses Resend for admin email delivery and Twilio for SMS delivery.
- Twilio env vars for live SMS:
  - `TWILIO_ACCOUNT_SID`
  - `TWILIO_AUTH_TOKEN`
  - `TWILIO_MESSAGING_SERVICE_SID` or `TWILIO_PHONE_NUMBER`
- Household mutation notifications now fan out to the admin by email and SMS when an admin phone is configured.

## LLM modes
- `LLM_MODE=mock` (default): deterministic local parser/classifier.
- `LLM_MODE=openai`: uses OpenAI strict JSON prompts for extraction and command parsing.
- Recommended OpenAI vars:
  - `OPENAI_API_KEY`
  - `OPENAI_MODEL=gpt-4.1-mini`
  - `OPENAI_TIMEOUT_SEC=20`
  - `OPENAI_BASE_URL=https://api.openai.com/v1`

Prompt transparency:
- Extraction and command system prompts are defined in [app/services/llm.py](/Users/jinaelee/projects/lovelychaos/app/services/llm.py) as constants (`EXTRACTION_SYSTEM_PROMPT`, `COMMAND_SYSTEM_PROMPT`).

## Resend inbound webhook
- Configure Resend inbound webhook target to `POST /webhooks/resend/inbound`.
- Supported input fields are normalized from common Resend webhook payload shapes (`type`, `data.from`, `data.to`, `data.subject`, `data.text`).
- In production, the endpoint verifies Resend's Svix signature headers (`svix-id`, `svix-timestamp`, `svix-signature`) using `RESEND_WEBHOOK_SECRET`.
- Local/dev fallback supports `X-Resend-Signature` equal to `RESEND_WEBHOOK_SECRET`.

## Twilio inbound SMS webhook
- Configure your Twilio Messaging webhook target to `POST /webhooks/twilio/sms`.
- The endpoint normalizes Twilio form fields (`MessageSid`, `From`, `To`, `Body`) into the existing SMS command pipeline.
- If `TWILIO_AUTH_TOKEN` is set, inbound requests require a valid `X-Twilio-Signature`.

## Internal endpoints auth
- If `ADMIN_API_KEY` is unset/empty, `/internal/*` endpoints are open for local development.
- If `ADMIN_API_KEY` is set, include `X-Admin-Key: <your-key>` on every `/internal/*` request.
