# Repository Guidelines

## Project Leadership
- Jina Lee is the CTO of this project.
- For project background, strategy, or source-of-truth context, refer to [`lovelychaos.docx`](/Users/jinaelee/projects/lovelychaos/lovelychaos.docx).

## Local Gmail Test Account
- A local Gmail test connection is configured for `cjinae@gmail.com`.
- This connection is for local testing workflows only, especially end-to-end email tests against the app and Resend inbound flows.
- Do not add Gmail credentials, tokens, or account-specific values to tracked app code, tests, fixtures, or docs outside this file and `CLAUDE.md`.
- Do not implement or use delete operations for this mailbox from local tooling.

## Local Gmail Tools
- Hidden local-only tooling lives in [`.local/gmail_tools/`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/).
- OAuth client secret path: [`.local/gmail_tools/client_secret.json`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/client_secret.json)
- OAuth token cache path: [`.local/gmail_tools/state/token.json`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/state/token.json)
- Wrapper command: [`.local/gmail_tools/gmail`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/gmail)
- Implementation: [`.local/gmail_tools/gmail_cli.py`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/gmail_cli.py)
- Setup notes: [`.local/gmail_tools/README.md`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/README.md)

## Gmail Commands
- Authenticate: `.local/gmail_tools/gmail auth`
- Read recent email: `.local/gmail_tools/gmail read --max-results 5`
- Read a specific message: `.local/gmail_tools/gmail read --id <gmail_message_id>`
- Create a draft: `.local/gmail_tools/gmail create-draft --to you@example.com --subject "Draft test" --body "Hello"`
- Send a message: `.local/gmail_tools/gmail send --to you@example.com --subject "Send test" --body "Hello"`
- Send an existing draft: `.local/gmail_tools/gmail send --draft-id <gmail_draft_id>`

## Usage Rules
- Prefer the local Gmail wrapper command instead of writing new ad hoc email scripts.
- If the token is missing or expired, run `.local/gmail_tools/gmail auth` and complete browser consent locally.
- When reporting mailbox contents back to the user, summarize the minimum needed content and avoid dumping large email bodies unless requested.
- Treat mailbox content and token files as sensitive local data.

## Project Context
- This repo is the LovelyChaos app workspace.
- LovelyChaos should behave like a helpful assistant for school communication, not just a date/event operator.
- It should explain what matters in plain language, support conversational follow-ups, and only act like a calendar operator when the user is clearly asking for scheduling help.
- The Gmail tooling is intentionally isolated from app runtime code and dependencies so local mailbox testing does not alter product configuration.

## OpenAI API Rules
- Prefer the OpenAI Responses API for all new and updated model integrations in this repo.
- Do not add new Chat Completions integrations when the same behavior is available through Responses API.
- When touching OpenAI code, verify the current official OpenAI docs first and keep payloads aligned with the latest Responses API and tracing guidance.
- The command execution layer uses the `openai_agents` SDK runtime with a SQLAlchemy-backed session store (`DbBackedAgentSession` in `app/services/agent_threads.py`). Session IDs are scoped per email thread key or per SMS household.
- Per-request tracing is configured via `app/services/openai_tracing.py`. Enable with `OPENAI_TRACING_ENABLED=true`.

## Key Service Modules

| Service | Purpose |
| --- | --- |
| `app/services/llm.py` | Core LLM decision engine — extraction, command parsing, summary, preference parse |
| `app/services/agent_threads.py` | Multi-turn session state, thread key resolution, thread document persistence |
| `app/services/add_requests.py` | Calendar add candidate resolution pipeline (explicit, context-based, or full extraction) |
| `app/services/school_knowledge.py` | Local school communication corpus for extraction context; advisory only |
| `app/services/openai_tracing.py` | Per-request and per-workflow tracing wrappers for the OpenAI agents SDK |
| `app/services/followups.py` | Followup context storage and SMS numbered-selection conversation state |
| `app/services/content_analysis.py` | Attachment/PDF download, extraction, and chunking |
| `app/services/relevancy.py` | Event relevance scoring against household children (name, grade, teacher) |
| `app/services/priorities.py` | Household preference rules and priority topic matching |
| `app/services/brief_summary.py` | Summary generation from extracted items |
| `app/services/calendar.py` | Calendar mutation (mock and live Google Calendar) |
| `app/services/notifications.py` | Outbound email/SMS dispatch (mock and live) |
