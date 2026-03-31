# Claude Workspace Notes

## Project Leadership
- Jinae Lee is the CTO of this project.
- For project background, strategy, or source-of-truth context, refer to [`lovelychaos.docx`](/Users/jinaelee/projects/lovelychaos/lovelychaos.docx).

## Product Intent
- LovelyChaos should behave like a helpful assistant for school communication, not just a date/event operator.
- It should explain what matters in plain language, support conversational follow-ups, and only switch into calendar/operator behavior when the user is clearly asking for scheduling help.
- The app uses a "one brain" architecture: SMS and email share a unified household session, followup context, and document access. The experience should be like talking to one personal assistant who remembers everything regardless of channel.

## Gmail Connection
- This workspace has a local Gmail test connection for `cjinae@gmail.com`.
- Use it for local read, draft, and send workflows when testing email loops for LovelyChaos and Resend inbox flows.
- Do not create delete capabilities or remove mailbox content.

## Tool Entry Points
- Primary command: `.local/gmail_tools/gmail`
- Auth flow: `.local/gmail_tools/gmail auth`
- Read inbox: `.local/gmail_tools/gmail read --max-results 5`
- Read one message: `.local/gmail_tools/gmail read --id <gmail_message_id>`
- Create draft: `.local/gmail_tools/gmail create-draft --to <email> --subject "<subject>" --body "<text>"`
- Send email: `.local/gmail_tools/gmail send --to <email> --subject "<subject>" --body "<text>"`
- Send draft: `.local/gmail_tools/gmail send --draft-id <gmail_draft_id>`

## Local Files
- Tool directory: [`.local/gmail_tools/`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/)
- Secret file: [`.local/gmail_tools/client_secret.json`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/client_secret.json)
- Token file: [`.local/gmail_tools/state/token.json`](/Users/jinaelee/projects/lovelychaos/.local/gmail_tools/state/token.json)

## Guardrails
- Keep all Gmail credentials and token state in `.local/`.
- Do not move Gmail secrets into tracked source files, env examples, or tests.
- Use the existing local tool instead of re-implementing Gmail API access unless the user asks for feature changes.
- Keep email-output summaries concise unless the user asks for full bodies or raw metadata.
