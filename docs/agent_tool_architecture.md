# Agent Tool Architecture

## Execution Matrix

| Inbound mode | Primary model task | Primary tools | Fallback path |
| --- | --- | --- | --- |
| User-initiated SMS/email command without attached context | Parse intent, use session context, execute safe tool call when supported | Preference tools, calendar search/update/delete/reminder/create tools | Existing deterministic command handlers |
| User-initiated SMS/email command with attached or forwarded context | Parse intent, use thread docs/session context, execute tools when the target is already clear | Preference tools, calendar read/update/delete/reminder tools | Existing deterministic add/extraction flow |
| Forwarded email with no user command | Extract/summarize school content | No mutation tools by default | Existing ingestion and summary pipeline |
| Conversational follow-up in same thread/session | Resolve references from session history, followup context, and thread docs | Calendar search/create/update/delete/reminder, preference tools | Existing followup matching logic |

## Source Of Truth

1. Household/user context: onboarding/admin profile, children, grades, schools, teachers, and current preferences.
2. First-party thread material: raw inbound message, forwarded content, extracted attachment/PDF text, and later relevant thread conversation.
3. Derived structured layers: extracted events, summary candidates, followup context items, stored events.
4. Advisory context only: school knowledge hints and retrieved examples.

## Tool Boundary

### Preference tools
- `read_preferences_tool`
- `update_preferences_tool`

These tools read and mutate household preference state. Persistence rules stay in application code.

### Calendar tools
- `search_calendar_tool`
- `create_calendar_event_tool`
- `update_calendar_event_tool`
- `delete_calendar_event_tool`
- `set_calendar_reminder_tool`

These tools use application code for target resolution, tenant checks, Google Calendar binding lookup, token refresh, and database persistence.

## Deterministic Paths Preserved

- Complex forwarded/context-heavy add flows still use the existing extraction-based add pipeline as fallback.
- Summary extraction/compression remains a separate structured-output pipeline.
- Existing followup-context resolution remains the canonical source for ambiguous `"this"`/`"that one"` add flows.

## Session & Thread State

- Agent sessions are scoped per email thread key (`thread_key` from message-id/references headers) or per SMS household (`sms:{household_id}`).
- Session items are persisted in `AgentSessionItem` via `DbBackedAgentSession` in `app/services/agent_threads.py`.
- PDF and attachment text is persisted as `ThreadDocument` rows so follow-up messages in the same thread can reference attachment content without re-downloading.
- Tracing is managed per-request via `app/services/openai_tracing.py` using `OPENAI_TRACING_ENABLED`.

## Notes

- The OpenAI command parser supports `update` in addition to add/read/delete/reminder/preference actions.
- The command execution agent uses function tools for stateful operations and keeps mutation authority in app code.
- Complex forwarded/context-heavy add flows use the `add_requests.py` resolution pipeline, which handles candidate extraction, disambiguation, and validation before handing off to calendar tools.
