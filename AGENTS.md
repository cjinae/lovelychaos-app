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
- The Gmail tooling is intentionally isolated from app runtime code and dependencies so local mailbox testing does not alter product configuration.
