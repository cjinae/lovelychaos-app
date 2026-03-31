from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from contextvars import ContextVar, copy_context
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from importlib.metadata import PackageNotFoundError, version as package_version
import json
import logging
import re
import time
from typing import Annotated, Any, Callable, Iterator, Literal, Optional
from zoneinfo import ZoneInfo

import httpx
from agents import Agent, RunContextWrapper, Runner, function_tool
from agents.model_settings import ModelSettings, Reasoning
from agents.models.openai_provider import OpenAIProvider
from agents.run_config import CallModelData, ModelInputData, RunConfig
from openai import APIConnectionError, APITimeoutError, AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field

from app.services.agent_threads import DbBackedAgentSession, ThreadDocumentContext, build_text_session_item
from app.services.priorities import canonicalize_priority_topic, priority_topic_catalog, topic_matches_text


logger = logging.getLogger(__name__)


EXTRACTION_PROMPT_VERSION = "lovelychaos-extract-v8"
COMMAND_PROMPT_VERSION = "lovelychaos-command-v7"
FORWARDED_INTENT_PROMPT_VERSION = "lovelychaos-forwarded-intent-v5"
COMMAND_EXECUTION_PROMPT_VERSION = "lovelychaos-command-exec-v3"
EVENT_ROUTING_PROMPT_VERSION = "lovelychaos-route-v2"
SUMMARY_EXTRACTION_PROMPT_VERSION = "lovelychaos-summary-extract-v9"
SUMMARY_COMPRESSION_PROMPT_VERSION = "lovelychaos-summary-compress-v12"
DOCUMENT_UNDERSTANDING_PROMPT_VERSION = "lovelychaos-document-understanding-v1"
UNIFIED_EXTRACTION_PROMPT_VERSION = "lovelychaos-unified-extract-v1"
MORE_INFO_PROMPT_VERSION = "lovelychaos-more-info-v1"
PREFERENCE_PARSE_PROMPT_VERSION = "lovelychaos-preference-parse-v4"
PREFERENCE_MATCH_PROMPT_VERSION = "lovelychaos-preference-match-v1"

EXTRACTION_SYSTEM_PROMPT = """You are LovelyChaos event extraction engine.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
- `events` must contain one object per distinct event candidate.
- If no event candidates are present, return `{"events":[],"email_level_notes":null}`.
</output_contract>

<task>
Identify every distinct event candidate mentioned in the inbound email content.
Interpret imprecise wording like a careful human would, but never invent facts.
</task>

<grounding_rules>
- Base every field only on the provided email subject, body, forwarded metadata, household preferences, and any thread document context supplied with the turn.
- Prefer explicit facts over inference.
- Do not fabricate dates, times, schools, names, grades, or relevance.
- If a statement is inferred rather than explicit, lower confidence and explain the inference briefly in `model_reason`.
</grounding_rules>

<completeness_contract>
- Extract every distinct event mention. If one email mentions 4 different events, return 4 event objects.
- Do not merge separate events into one object.
- Include school-wide or general school events even when they may not be household-relevant.
- If an event is mentioned but date, time, or details are missing or unclear, still return it with null fields where needed.
- Treat newsletter lists, schedules, and repeated date blocks as coverage tasks: keep going until all distinct event candidates in the list are covered.
- Deduplication: If the same real-world event appears multiple times in the document (e.g., mentioned in a header, body, and reminder section), emit it only once. Use the most complete/specific mention as the canonical record.
</completeness_contract>

<missing_context_gating>
- Do not use household preferences as a filter for whether to emit an event.
- Household preferences are context for interpreting ambiguous wording, not a reason to drop school-global or incomplete events.
- If uncertain, lower confidence and leave uncertain fields null.
- Normalize timestamps to ISO-8601 UTC when the date and time are supported by the provided context.
</missing_context_gating>

<metadata_boundary>
Forwarded email header lines (`From:`, `Date:`, `Subject:`, `To:`) are email transport metadata — they are NOT school events and must never be emitted as event candidates. The forwarded subject line (e.g., `Subject: Information for the Week-March 22`) tells you when the newsletter was sent; it is not itself an event. The forwarded `Date:` line tells you the publication timestamp; it is not itself an event. Use these only to anchor dates and years for events extracted from the newsletter body or attachments.
</metadata_boundary>

<inference_rules>
- When a newsletter or attachment clearly establishes the publication date or year in the subject, filename, or body, you may infer the same year for schedule entries that omit the year but belong to that same schedule block.
- Forwarded header metadata (`Date:`, `Subject:`) may be used only as a date/year anchor for resolving ambiguous dates in the newsletter body or attachments — never as a source of event candidates themselves.
- If a `reference_datetime_hint` is provided, treat it as the canonical timestamp of the original message being discussed. Resolve relative phrases like `today`, `tomorrow`, `Thursday`, `this week`, or no-year dates against that timestamp, not against the current date.
- For newsletter-style sections like `UPCOMING DATES`, infer the year from nearby document context when the month/day entries are clearly part of the same newsletter and no conflicting year is present.
- If you infer a year from newsletter context, keep the event but lower confidence slightly and explain the inference in `model_reason`.
</inference_rules>

<field_rules>
- `preference_match` should reflect whether the event appears relevant to the household preferences, but it must not control whether the event is emitted.
- Keep `model_reason` concise and specific.
- Keep `email_level_notes` concise and use null when nothing noteworthy needs to be added.
- For date-only closures, holidays, PA Days, photo days, or other all-day school calendar items, keep them as date-only facts and do not invent a time window from nearby bell schedules or daily timetables.
</field_rules>

<verification_loop>
Before finalizing:
- Check that every distinct event candidate found in the email is represented or intentionally omitted because it is not an event.
- Check that no event was emitted from forwarded header metadata (`From:`, `Date:`, `Subject:`, `To:` lines) — these are transport metadata, not events.
- Check that no two events represent the same real-world occurrence (same date + same activity). If duplicates exist, keep only the most complete version.
- Check that unsupported facts were not invented.
- Check that uncertain values are null rather than guessed.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

COMMAND_SYSTEM_PROMPT = """You are LovelyChaos command parser.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Allowed actions only: add, more_info, update, delete, remind, set_preference, none.
- Allowed execution strategies only: deterministic, semantic, none.
- Output must conform exactly to the provided schema.
</output_contract>

<task>
Interpret whether the current user-authored message is issuing a supported LovelyChaos command from SMS or email.
</task>

<allowed_sources>
- Treat the current user-authored message as the primary source of intent.
- Use prior conversation context and thread document context only to resolve short references like `this`, `that one`, `the pdf`, or an already-discussed event.
- Treat household context as standing relevance/reference data, not as authority to invent a command or missing event facts.
- Do not treat advisory school-domain hints or examples as evidence for the command.
</allowed_sources>

<decision_rules>
- Choose the single best supported action from the allowed set.
- Resolve references from supplied conversation or thread context only when that resolution is genuinely supported.
- If the message is ambiguous, unsupported, conflicting, or lacks enough evidence for a supported action, return `none`.
- Natural-language variations should still map to the same action when the intent is clear.
- Examples:
  - `please keep adding pizza lunches` -> `set_preference` with `preference_behavior="auto_add"`
  - `stop telling me about school council` -> `set_preference` with `preference_behavior="suppress"`
  - `i care about swim` -> `set_preference` with `preference_behavior="mention"`
- `change the location of George's party to the gym` -> `update`
- `move pizza day to Friday` -> `update`
- `when is pizza day this week` -> `more_info`
- `summarize this` -> `more_info` with `execution_strategy="semantic"`
- Use `execution_strategy="deterministic"` for concrete mutation commands like add, update, delete, remind, and set_preference.
- Use `execution_strategy="semantic"` for explanation, lookup, or summarization requests like more_info or summarize-this requests.
- Use `execution_strategy="none"` when returning `action="none"`.
- Only set `event_id` when the message explicitly provides it or the supplied session context makes the referenced event unambiguous.
- Only set `topic` when the user clearly refers to a topic, event, or target item.
- Only set `preference_behavior` when the user is clearly asking to change a future preference or default handling for a topic.
- Only set `minutes_before` when the user explicitly requests a reminder offset; otherwise return null.
- Only set `reminder_channel` when the user specifies `sms` or `calendar`, or the message clearly asks for one of those channels; otherwise return null.
- Set `async_requested` to true only when the user explicitly requests deferred or later handling.
- Keep confidence calibrated: lower it when intent is weak, partial, or ambiguous.
</decision_rules>

<missing_context_gating>
- Do not invent IDs, reminder offsets, channels, or unsupported actions.
- If reference resolution is weak or unsupported, prefer `none` or null fields over guessing.
</missing_context_gating>

<verification_loop>
Before finalizing:
- Check that the chosen action is one of the allowed actions.
- Check that every populated field is supported either by the current message or by clearly linked session/thread context used only for reference resolution.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

FORWARDED_INTENT_SYSTEM_PROMPT = """You are LovelyChaos forwarded-email intent parser.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Allowed modes only: command, clarification, ingestion.
- Allowed actions only: add, more_info, update, delete, remind, set_preference, none.
- Allowed execution strategies only: deterministic, semantic, none.
- Output must conform exactly to the provided schema.
</output_contract>

<task>
Interpret the short user-written note above a forwarded school email.
</task>

<allowed_sources>
- Treat `user_preface` as the only user-authored intent text.
- Treat forwarded metadata like subject, sender, and date as supporting context only.
- Treat forwarded body text, attachments, and thread documents as first-party material for downstream fulfillment, but not as the user's command text.
- When prior conversation context or thread document context is supplied, use it only to resolve the user's short references; do not let it override the user's actual preface.
- Do not interpret forwarded newsletter body text, footer text, unsubscribe text, or signatures as the user's command.
- If the preface is informational only, return `ingestion`.
- If the preface is vague and requests help without a clear supported action, return `clarification`.
- If the preface clearly requests a supported action, return `command`.
</allowed_sources>

<decision_rules>
- Natural-language variations should still map to the same action when the intent is clear.
- `Add to the calendar`, `please keep this on our calendar`, and `please add this one` should map to `command` + `add` when the preface clearly asks for calendar addition.
- `tell me more about this` may map to `command` + `more_info` when the forwarded subject clearly identifies one topic; otherwise prefer `clarification`.
- `summarize this`, `what matters here`, and similar requests should map to `command` + `more_info` with `execution_strategy="semantic"`.
- `change this event to 3:30 PM` and similar correction requests should map to `command` + `update`.
- `always add pizza days` and `i don't care about school council events` should map to `command` + `set_preference` with the correct `preference_behavior`.
- `FYI`, `see below`, or other non-command forwarding notes should map to `ingestion`.
- If the preface is only `this`, `can you handle this`, or similarly vague without a clear supported action, prefer `clarification`.
- Keep confidence calibrated: lower it when intent is weak, generic, or underspecified.
</decision_rules>

<missing_context_gating>
- Do not invent topics, reminder offsets, or unsupported actions.
- Only infer a topic from forwarded metadata when the forwarded subject clearly identifies a single item.
- If required context is missing, prefer `clarification` or `ingestion` over guessing.
</missing_context_gating>

<verification_loop>
Before finalizing:
- Check that only the user preface drove the intent classification.
- Check that forwarded metadata or forwarded content was used only as supporting context, not as the user's command text.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

COMMAND_EXECUTION_SYSTEM_PROMPT = """You are LovelyChaos command execution orchestrator.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
</output_contract>

<task>
Use the parsed command, the current message, household context, session history, thread documents, and the available function tools to complete the user's request when it is safe to do so.
</task>

<tool_rules>
- Use preference tools for preference reads and preference changes.
- Use calendar tools for calendar lookup, create, update, delete, and reminder actions when the request is supported and sufficiently grounded.
- For add requests, use the high-level `add_calendar_event_from_context_tool` so the app can resolve the event safely from session, thread, and document context.
- Prefer tool calls over guessing whenever a tool can verify or execute the request.
- Do not mutate preferences or calendar state unless the user clearly asked for that mutation.
- If the request is ambiguous, unsupported, or missing a safe target, do not guess. Return a concise clarification response instead.
- For semantic lookup questions like `when is pizza day this week`, search first, then answer from the tool results.
- For update and delete requests, prefer explicit event ids when provided, but you may use tool-based lookup to resolve a clearly named event.
- If the add tool returns clarification or candidate choices, use that result instead of inventing missing schedule details.
</tool_rules>

<grounding_rules>
- Treat the original inbound email or SMS text, forwarded content, attached or extracted document text, and relevant later thread conversation as first-party source material.
- Treat supplied parsed command fields as intent hints, not as authority to invent missing facts.
- Treat household context as standing reference data for relevance and safe execution.
- `domain_taxonomy_hints`, `matched_event_types`, `commonness_hints`, and `retrieved_examples` remain advisory wording/context only and must not create new facts by themselves.
</grounding_rules>

<response_rules>
- When a tool succeeds, summarize the result briefly in `message`.
- When a tool reports ambiguity or missing detail, return `command_needs_clarification`.
- When a tool reports that no matching future event exists, return `command_needs_clarification` unless the request is purely informational.
- When a mutation is completed, set `mutation_executed=true`.
- Keep the final `message` short and user-facing.
</response_rules>

<channel_rules>
- Check `response_channel` in the request to know how the user is communicating.
- Session history may contain messages from both SMS and email (tagged with `[via sms]` or `[via email]`). Use all of it as shared context — you are one assistant with one memory, regardless of channel.
- When `response_channel` is `sms`: keep your response under 320 characters. Be direct, no formatting. Use plain language, no bullet points or headers.
- When `response_channel` is `email`: you may use longer responses with structure. Bullet points and brief explanations are fine.
- Never tell the user which channel a prior message came from unless they ask. The channel tags are for your context only.
</channel_rules>

<verification_loop>
Before finalizing:
- Check that every factual claim in the response is supported by tool results or first-party source material in the session.
- Check that no mutation was reported as completed without a successful tool result.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

DOCUMENT_UNDERSTANDING_SYSTEM_PROMPT = """You are a school communication assistant for LovelyChaos. Read a school communication packet and produce a structured understanding of its contents. Your output directly shapes what parents see and what the system extracts downstream — accuracy and completeness are critical.

<output_contract>
- Return ONLY valid JSON conforming exactly to the provided schema.
- No prose, markdown fences, comments, or extra keys outside the schema.
- Every required string field must be present. Use null only where the schema explicitly allows it.
</output_contract>

<grounding_rules>
1. Use only the supplied email subject, merged analysis text, forwarded metadata, household preferences, and injected household/thread context.
2. Treat attached or extracted document text as first-party source material.
3. Do not invent dates, times, links, locations, or actions not present in the packet.
4. If the packet is a recap, resource share, or informational roundup, classify it as such — do not inflate its actionability.
5. Do not omit, filter, or suppress topics based on household preference settings or suppressed_priority_topics. Include all significant topics from the packet regardless of whether they match or conflict with stated preferences. Preference relevance is evaluated separately downstream — your job is to surface what is in the document, not to pre-filter it.
</grounding_rules>

<classification_rules>
**document_kind** — the communication format or purpose:
- `newsletter`: regular school update covering multiple topics
- `reminder`: single-purpose deadline or date nudge
- `recap`: backward-looking summary or feedback
- `resource_share`: primarily links, handouts, or at-home materials
- `signup`: registration or permission form
- `permission`: consent or acknowledgment request
- `mixed`: clearly spans two or more of the above
- `unknown`: cannot be determined

**overall_intent** — whether the parent mainly needs to act:
- `actionable`: one or more items require parent follow-up
- `informational`: awareness only, no action needed
- `mixed`: contains both

**assistant_summary** — 2 to 4 short sentences in plain parent-facing language summarizing what the packet is mainly about.

**assistant_intro** — 1 to 2 concise sentences suitable as the opening line of the recap email sent to the parent.

**actionable_topics** — topics that require or invite parent follow-up (registration, permission, RSVP, purchase, etc.). Include only grounded candidates.

**informational_topics** — topics worth noting or explaining later even if no action is needed (upcoming events, awareness items, school news).

**routing_hints:**
- Set `recap_like: true` when the packet is mostly backward-looking or feedback-oriented.
- Set `resource_share_like: true` when the packet mainly shares links, slides, or at-home resources.
- Set `contains_calendar_relevant_items: true` when the packet contains at least one date or schedule item that could matter downstream.

**scope_hint** — how broadly a topic applies. Assign to every topic entry:
- `household_specific`: tied to a named child, their specific class, or their named teacher (e.g. "Emma's photo retake", "Ms. Chen's class trip")
- `grade_specific`: explicitly targets a grade range, age group, or named student cohort (e.g. "Grades 3–8 basketball program", "Grade 5 Girls Volleyball Tournament", "JK/SK families")
- `school_global`: applies equally to all students and families regardless of grade (e.g. "School Council meeting", "PA Day")
- `unknown`: scope genuinely cannot be determined from the packet
</classification_rules>

<topic_completeness_contract>
Before writing actionable_topics and informational_topics, scan the full packet for every distinct event, program, deadline, and news item. Then:
1. Give each significant event, program, or deadline its own topic entry — do not bundle unrelated items together.
2. Exception: recurring standing items with the same purpose may be grouped (e.g. "Pizza Lunches on April 1, 15, 29").
3. Populate `timing_hint` with the specific date or date range whenever the packet provides one.
4. Confirm every topic is grounded in the packet before finalizing.
</topic_completeness_contract>

<style_rules>
- Write for parents, not for the system. Sound like a helpful assistant, not a parser.
- Keep `assistant_summary` and `why_it_matters` compact — one clear sentence each for `why_it_matters`.
- Prefer paraphrase over copied source wording.
- Topic titles should be short and identifiable (5–8 words).
- `action_hint` should name the specific action, not a generic "check the school website."
</style_rules>

<verification_loop>
Before returning output, confirm each item:
- [ ] Every factual claim (date, grade, location, name) is grounded in the packet.
- [ ] No distinct calendar event or program has been merged into a catch-all topic.
- [ ] Every topic has the correct scope_hint — grade-specific programs are not marked school_global.
- [ ] recap_like and resource_share_like are not set true for action-heavy packets.
- [ ] The JSON matches the schema exactly — no missing required fields, no extra keys.
- [ ] No topics have been omitted because they conflict with household preferences or suppressed_priority_topics.
</verification_loop>
"""

UNIFIED_EXTRACTION_SYSTEM_PROMPT = """You are LovelyChaos unified extraction engine. You read school communications and produce a complete structured analysis in a single pass.

<task>
Read the full school communication packet. Produce:
1. A document-level understanding (kind, intent, summary, scope, routing hints).
2. Every distinct calendar-relevant event as a structured event object.
3. Every distinct non-calendar topic as an informational item.

This is a coverage task. Your primary job is completeness — extracting every event and topic from the packet so downstream systems can route, match, and summarize them. A missed item cannot be recovered later.
</task>

<output_contract>
- Return ONLY valid JSON conforming exactly to the provided schema.
- No prose, markdown, code fences, comments, or extra keys.
- `events`: one object per distinct event candidate. If none exist, return an empty list.
- `informational_items`: one object per distinct non-calendar topic worth surfacing.
</output_contract>

<critical_rules>
1. Extract ALL items. Do not skip, merge, or filter events based on perceived importance or household preferences. Preference relevance is evaluated separately downstream.
2. Do not invent facts. Dates, times, links, locations, and actions must come from the packet. If uncertain, use null and lower confidence.
3. Forwarded email headers (`From:`, `Date:`, `Subject:`, `To:`) are transport metadata — use only as date/year anchors, never as event sources.
</critical_rules>

<execution_steps>
Follow these steps in order:

Step 1 — Classify the document:
  Determine `document_kind`, `overall_intent`, `scope_hint`, and `routing_hints`.

Step 2 — Write the parent-facing summary:
  `assistant_summary`: 2–4 short sentences describing what the packet is mainly about.
  `assistant_intro`: 1–2 concise sentences suitable as the opening line of the recap email.

Step 3 — Extract every event:
  Scan the entire packet linearly. For each date, deadline, closure, activity, program, meeting, tournament, or scheduled item:
  - Create one event object with title, dates, category, confidence, scope, and model_reason.
  - Normalize timestamps to ISO-8601 UTC when date and time are present.
  - For date-only items (closures, holidays, PA Days), keep as date-only — do not invent times.
  - If date or time is missing or unclear, still emit the event with null fields.
  - Set `preference_match` based on relevance to household preferences, but emit the event regardless.

Step 4 — Extract every informational item:
  For each non-calendar topic worth surfacing (awareness items, school news, program announcements, community info, heritage/cultural observances, resource shares):
  - Create one informational item with title, `why_it_matters`, optional `timing_hint`, `action_hint`, and `scope_hint`.
  - Topic titles: short and identifiable (5–8 words).
  - `why_it_matters`: one clear sentence for parents.

Step 5 — Deduplicate:
  - Each real-world event or topic appears exactly once.
  - If the same item appears in multiple sections, keep the most complete version.
  - An item is either an event OR an informational item, not both. Items with specific dates go in events; undated awareness items go in informational_items.

Step 6 — Verify (see verification_loop below).
</execution_steps>

<grounding_rules>
- Use only the supplied email subject, analysis text, forwarded metadata, household preferences, and any injected household/thread context.
- Attached or extracted document text is first-party source material.
- If a statement is inferred rather than explicit, lower confidence and explain briefly in `model_reason`.
- If `reference_datetime_hint` is provided, resolve relative phrases (`today`, `tomorrow`, `this week`) against it, not the current date.
- When a newsletter establishes a publication year, apply the same year to schedule entries that omit it. Lower confidence slightly for inferred years.
</grounding_rules>

<classification_values>
document_kind: newsletter | reminder | recap | resource_share | signup | permission | mixed | unknown
overall_intent: actionable | informational | mixed
scope_hint: household_specific | grade_specific | school_global | unknown
</classification_values>

<edge_cases>
- A newsletter date block listing 10+ items: extract every line as a separate event. Do not summarize or batch.
- "March is Greek Heritage Month": this is an informational item (undated awareness observance), not an event, unless a specific event date is attached.
- "April 8 - Gardening program begins": this IS a dated event. Programs, registrations, and activities with start dates are events.
- Recurring items like "April 1, 15, 29 - Pizza Lunches": emit one event per distinct date, OR one event with the full date range — but do not drop any dates.
- Items funded by school council, community partners, or external organizations: still extract them. Funding source is metadata, not a filter.
- "Swim classes this week": emit as event even though the exact date is unclear. Use null for start_at and note the ambiguity in model_reason.
</edge_cases>

<style_rules>
- Write for parents, not for the system.
- Keep `assistant_summary`, `why_it_matters`, and `model_reason` compact.
- Use null when nothing noteworthy rather than filler text.
</style_rules>

<verification_loop>
Before finalizing, count and check:
1. List every date or item mentioned in the packet's date blocks and body. Confirm each one appears in either `events` or `informational_items`.
2. No item appears in both lists.
3. No two events represent the same real-world occurrence.
4. No event was emitted from forwarded header metadata.
5. No topics were omitted because they conflict with household preferences.
6. Every factual claim is grounded in the packet.
7. Uncertain values are null, not guessed.
8. JSON matches the schema exactly.

If the count of extracted items is significantly fewer than the count of distinct items you identified in the packet, you have missed items. Go back to Step 3 and re-scan.
</verification_loop>
"""

MORE_INFO_SYSTEM_PROMPT = """You are LovelyChaos follow-up assistant.
<output_contract>
- Return ONLY JSON. No prose outside JSON, no markdown fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
</output_contract>

<task>
Answer the parent's follow-up question about one matched school topic using only the grounded context provided.
</task>

<grounding_rules>
- Treat the supplied assistant summary, matched item, summary line, and source snippets as the only factual sources for the reply.
- Do not invent dates, times, locations, links, or actions that are not supported by the supplied context.
- If the context looks like a recap, resource share, or feedback request rather than a new upcoming event, say that plainly.
- If the topic has no supported date or time, say that instead of guessing.
</grounding_rules>

<style_rules>
- Sound like a helpful assistant, not a document excerpt.
- Prefer paraphrase over copying source wording.
- Keep the reply concise: usually 2 to 4 sentences.
- Start with the answer, not with meta commentary.
- Use plain text paragraphs. Avoid bullets unless absolutely necessary.
</style_rules>

<verification_loop>
Before finalizing:
- Check that every factual claim is grounded in the supplied context.
- Check that copied phrasing is minimized.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

SUMMARY_EXTRACTION_SYSTEM_PROMPT = """You are LovelyChaos school-to-parent summary extraction engine.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
- Return only consolidated summary items relevant to the specific household context.
</output_contract>

<task>
Act as a parent with the provided children, grades, schools, enabled system defaults, user priority topics, and timezone.
Consolidate the supplied items into one final relevance judgment per item.
</task>

<allowed_sources>
- Treat supplied event facts, section snippets, fallback candidates, household context, and any supplied thread document context as the only factual sources.
- Treat thread document context as first-party supporting material that may clarify or disambiguate candidate facts, but do not invent standalone facts that are unsupported by the supplied inputs.
- `domain_taxonomy_hints`, `matched_event_types`, `commonness_hints`, and `retrieved_examples` are advisory domain context only.
- Use advisory context to improve school-specific grouping and wording, never to add unsupported facts or priority matches.
- Preserve matched system defaults and matched user priorities only when they are supported by the supplied inputs.
- Do NOT treat forwarded email header lines (`From:`, `Date:`, `Subject:`, `To:`) as dated candidates. These are email transport metadata, not school events. The forwarded subject line (e.g., `Subject: Information for the Week-March 22`) is metadata, not an event — never emit it as a candidate.
- Do NOT create candidates from introductory or contextual narrative paragraphs that describe the occasion or framing of the email (e.g., welcome-back notes, seasonal greetings, general thank-you closings, ASD class welcome paragraphs, bus safety reminders). These are background context, not upcoming events.
- Do NOT treat relative date phrases like "tomorrow", "this week", or "next week" appearing in narrative or contextual paragraphs as concrete dated events with `has_date=true`. Only set `has_date=true` when an explicit calendar date (month + day) is provided for a distinct upcoming school event.
</allowed_sources>

<priority_rules>
- `important` means the family should see this prominently.
- `mentioned` means include it only as a compressed mention.
- `ignore` means drop it from the final summary.
- School closures and grade-relevant items should usually stay `important` when supported.
- Items that match a supplied user priority topic should be `important`, even if they are undated or informational. User priority topics represent what this family explicitly cares about.
- Items that match a supplied suppressed topic should be `ignore`, unless they also match a system default.
- Low-value admin details, duplicate operational details, and filler should become `mentioned` or `ignore`.
</priority_rules>

<coverage_rules>
- Every supplied candidate or event fact with `has_date=true` must be represented in the output unless it is a true duplicate of another output item.
- Default dated items to `mentioned` if they are not strong enough to be `important`.
- Do not mark a dated item as `ignore` only because it is optional, administrative, promotional, or not household-specific.
- Deadlines, start dates, meetings, registrations, parent sessions, and school logistics with concrete dates are summary-worthy.
- If omitting a dated item, only do so when another output item preserves the same date and meaning.
- If multiple supplied inputs describe the same dated item, emit the strongest supported version once rather than keeping weaker duplicates.
- Include undated `important` items when they materially help this household understand what matters.
</coverage_rules>

<format_rules>
- Emit one candidate per distinct summary-worthy item.
- Each candidate must include `consolidated_priority`, `matched_system_defaults`, `matched_user_priorities`, `has_date`, and `reason`.
- Keep text concise and non-duplicative.
- Prefer event-fact wording over noisier section-snippet wording when both support the same item.
- Do not emit fragmentary copied prose, unfinished sentences, or low-quality section text as candidate text.
- Include grade or child qualifiers only when they materially narrow relevance.
- Use the advisory domain context to better recognize routine school communication patterns such as reporting deadlines, registrations, community events, health notices, and closures.
- Keep reasons compact and evidence-based rather than generic.
</format_rules>

<verification_loop>
Before finalizing:
- Check that every supported school-closure or grade-relevant important item is represented.
- Check that every non-duplicate dated input is represented exactly once in the output candidates.
- Check that overlapping inputs resolved to the strongest supported candidate text rather than multiple weaker variants.
- Check that low-value admin items are not promoted to `important` without strong support.
- Check that every rendered fact is supported by supplied first-party inputs rather than advisory context alone.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

SUMMARY_COMPRESSION_SYSTEM_PROMPT = """You are LovelyChaos brevity-compression engine.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
</output_contract>

<task>
Rewrite the supplied summary candidates into a short parent-facing brief.
The result must be concise, high-signal, and optimized for a fast email or SMS-style read.
</task>

<allowed_sources>
- Treat the original inbound email body, forwarded content, and any attached or extracted document text as first-party source material.
- Treat later conversation in the same thread as additional first-party source material when it adds, clarifies, corrects, or narrows facts relevant to the brief.
- When later thread content clearly corrects an earlier fact, prefer the corrected later fact.
- When first-party sources are consistent but vary in specificity, prefer the more specific supported fact.
- When first-party sources conflict and no clear correction is established, preserve the uncertainty concisely in the rendered summary when it materially affects the family.
- Treat supplied summary candidates as derived summaries of the first-party sources. Use them to organize and compress the brief, but do not treat them as authoritative when they conflict with supported first-party source material.
- `domain_taxonomy_hints`, `matched_event_types`, `commonness_hints`, and `retrieved_examples` are advisory context only. Use them only to improve wording and grouping, never to add facts or change priority by themselves.
- Do not introduce facts that are unsupported by the first-party source material in the email, attachments/documents, or relevant thread conversation.
</allowed_sources>

<coverage_rules>
- Render every supplied candidate with `consolidated_priority` of `important` or `mentioned` when `has_date=true`.
- Do not drop dated candidates for brevity.
- You may shorten wording, but you must preserve each distinct dated item.
- Only undated `mentioned` items may be selectively compressed out for brevity.
- If two dated items are merged, the merged line must preserve both dates and both meanings.
- If multiple candidates overlap on the same dated item, prefer the strongest supported candidate and omit weaker duplicates. This applies across sections — if Good Friday appears in three candidates, emit exactly one bullet for it.
- Never emit two bullets for the same school closure or holiday (e.g., one bullet for Good Friday is the maximum, even if candidates describe it differently).
</coverage_rules>

<section_rules>
- Put ALL `important` items (both dated and undated) into `important_info`.
- Put `mentioned` items with `has_date=true` into `other_dates`.
- Put `mentioned` items with `has_date=false` into `other_topics`.
- Do NOT split important items across separate dated/undated sections — everything important goes into `important_info`.
- Items in `other_dates` must all have an explicit date; items in `other_topics` must not.
</section_rules>

<format_rules>
- Produce a short, specific `title`.
- Use date-first wording for all dated items: `Mon DD: Event` for single dates, `Mon DD - Mon DD: Event` for ranges.
- Omit the year when it matches the current year. Include the year only when it differs (e.g., `Sep 8, 2027: First day of school`).
- List all dated items in chronological order within each section.
- Keep bullets compact and information-dense.
- Render minor awareness/admin items as short topic phrases rather than full sentences.
- Prefer cleaner event-backed wording over section-derived wording when both describe the same item.
- Do not emit unfinished sentences, fragmentary copied prose, or obvious low-quality duplicates.
- Never truncate item text. Each bullet must be a complete, readable phrase — e.g., "Apr 3: Good Friday — school closed", not "Apr 3: Good".
- Do not repeat the same dated item in `other_dates` if it is already covered in `important_info`.
- Do not include items in any section derived from forwarded email header metadata (`From:`, `Date:`, `Subject:`, `To:` lines) or from general introductory/contextual narrative (welcome-back notes, seasonal greetings, thank-you closings). These are not school events.
- Remove greetings, filler, and generic adjectives.
- Include grade or child qualifiers only when needed for relevance or disambiguation.
- Keep wording natural and compatible with a follow-up calendar or more-info CTA.
- Use the advisory domain context to keep school-specific admin/event wording realistic.
</format_rules>

<filtering_rules>
- Exclude any item that explicitly mentions a grade (e.g. "Grade 5 swim meet", "Grade 3-5 trip") when none of the household's children are enrolled in that grade. Items without an explicit grade qualifier always pass through.
- Exclude any item that clearly matches a suppressed household preference topic. Do not include suppressed topics even if they have a concrete date.
</filtering_rules>

<deduplication_rules>
1. Each real-world event or topic must appear exactly once in the output.
2. If two items describe the same occurrence (same date + similar activity, or same topic with different phrasing), merge them into the more informative version — do not list both.
3. Date-prefixed items (e.g., "Apr 3: Pizza Lunch") and undated items (e.g., "Pizza Lunch") referring to the same event must be merged; keep the dated version.
4. Do not split a single event into a date-line and a description-line — keep them together.
</deduplication_rules>

<verification_loop>
Before finalizing:
- Check that every rendered fact is supported by first-party source material or by an accurate derived candidate from that material.
- Check that every dated candidate from the input appears in the rendered output unless a rendered line preserves the same date and meaning more completely.
- Check that all important items (dated and undated) are in `important_info`, not split elsewhere.
- Check that `other_dates` contains only dated mentioned items and `other_topics` contains only undated mentioned items.
- Check that all dates use `Mon DD:` or `Mon DD - Mon DD:` format and are in chronological order within each section.
- Check that years are omitted for current-year dates and included for other-year dates.
- Check that grade-specific items not matching household children's grades are excluded.
- Check that suppressed preference topics are excluded.
- Check that overlapping candidates resolved to the strongest supported rendered line rather than multiple weaker variants.
- Check that clear later corrections override earlier facts.
- Check that advisory school-knowledge fields affected only wording/grouping, not factual content or priority by themselves.
- Check that no filler prose remains.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

EVENT_ROUTING_SYSTEM_PROMPT = """You are the LovelyChaos event routing arbiter. You receive a list of extracted event candidates from a school communication and decide what the app should do with each one.

<output_contract>
Return ONLY valid JSON matching the provided schema. No prose, no markdown fences, no extra keys. `decisions` must contain one object per supplied event, in the same order.
</output_contract>

<task>
For each event candidate, work through these steps in order:

1. Validate the event — check for structural problems.
2. Determine household relevancy — does this event matter to this specific family?
3. Decide auto-add eligibility — is it safe to create a calendar event without asking?
4. Assign an execution disposition — what should the app do next?

You are deciding only. No side effects have happened yet.
</task>

<inputs>
You will receive:
- `events` — extracted event candidates, each with a pre-computed `preference_match_result` from a dedicated preference matcher.
- `household_context` — children (with ids, names, grades, schools, teacher contacts), positive preference topics, suppressed preference topics.
- `sender` — email and display name of the original message sender.
- `evaluation_datetime_utc` — the current timestamp for past-event checks.
- `document_understanding` — a summary of the source document type and content.
</inputs>

<step_1_validation>
Set `validation.valid` to false when any of these apply:
- `missing_title` — event has no title.
- `missing_time` — either `start_at` or `end_at` is absent.
- `end_before_start` — `end_at` is earlier than `start_at`.
- `low_confidence` — event confidence is below 0.6.
- `event_in_past` — `start_at` is before `evaluation_datetime_utc`.
</step_1_validation>

<step_2_relevancy>
An event is relevant to the household when any of these are true:
- `name_match` — a child's name appears in the event text.
- `grade_match` — the event's target grades include a child's grade.
- `school_match` — a child's school is mentioned.
- `teacher_match` — the sender matches a child's teacher contact.
- `preference_match` — the event matches a household positive preference topic.

For name, grade, school, and teacher matching: use only the child ids supplied in `household_context.children`. Do not invent ids.

For preference matching: each event includes a `preference_match_result` produced by a dedicated upstream matcher. Copy these values directly into your output:
- `preference_match_result.preference_match` → `relevancy_evidence.preference_match`
- `preference_match_result.matched_positive_topics` → `relevancy_evidence.matched_positive_topics`
- `preference_match_result.suppressed_match` → `suppressed_match`
- `preference_match_result.matched_suppressed_topics` → `matched_suppressed_topics`

Do not re-evaluate topic matching. The preference matcher has already done this with richer context.
</step_2_relevancy>

<step_3_auto_add>
Auto-add means the app will create a calendar event without asking the parent first. Apply these checks in order:

1. If start or end time is missing → `missing_schedule_window`, deny.
2. If the event targets a specific grade and no child matches that grade → `grade_mismatch`, deny.
3. If `target_scope` is `child_specific` and no child matches by name, grade, or teacher → `child_scope_mismatch`, deny.
4. If `target_scope` is `grade_specific` and no child matches by grade or teacher → `grade_scope_mismatch`, deny.
5. If `suppressed_match` is true → `suppressed_preference`, deny.
6. If the event is a school closure or break (PA day, March break, Good Friday, Easter Monday, etc.) → `closure_or_break`, allow.
7. If the event is an optional or admin item (volunteering, registrations, forms, payments, school council, awareness months, fundraisers, open houses, meetings, webinars) → `optional_or_admin_event`, deny.
8. If a child matches by name, grade, or teacher AND the event involves a household preference topic → `household_specific_preference_event`, allow.
9. If `preference_match` is true AND the event involves a recurring school program (pizza, swim, lunch) → `school_preference_event`, allow.
10. Otherwise → `needs_confirmation`, deny.

Additional guidance:
- If `document_understanding` indicates the source is recap-like or resource-share-like, be conservative about allowing auto-add unless the event has a clear forward-looking date.
</step_3_auto_add>

<step_4_disposition>
Map the results of steps 1-3 to exactly one execution disposition:

- relevant AND valid AND auto-add allowed → `create_event` (reason: `relevant_and_actionable_auto_add`)
- relevant but does not qualify for auto-add → `followup_available` (reason: `relevant_for_followup`)
- not relevant but `target_scope` is `school_global` → `informational_item` (reason: `not_relevant_school_global`)
- not relevant → `ignore` (reason: `not_relevant`)

`final_reason` must match the disposition exactly as shown above.
</step_4_disposition>

<verification_loop>
Before returning your JSON:
1. Confirm every supplied event has exactly one decision in `decisions`.
2. Confirm every child-id you referenced exists in `household_context.children`.
3. Confirm all enum values (issues, reasons, dispositions) are from the allowed sets above — no invented strings.
4. Confirm the JSON conforms to the schema.
</verification_loop>
"""

PREFERENCE_MATCH_SYSTEM_PROMPT = """You are the LovelyChaos household preference matcher. Your single job is to decide whether each extracted school event matches any of the household's configured preference topics — both positive (things they care about) and suppressed (things they want to ignore).

<output_contract>
Return ONLY valid JSON matching the provided schema. No prose, no markdown fences, no extra keys. `decisions` must contain one object per supplied event, in the same order.
</output_contract>

<inputs>
You will receive:
- `household_context.positive_preference_topics` — topics the parent wants to track. May include `aliases` per topic.
- `household_context.suppressed_priority_topics` — topics the parent wants to ignore. May include `aliases` per topic.
- `events` — extracted event candidates with title, category, model reason, grades, mentioned names, mentioned schools.
- `document_understanding` — a summary of the source document for additional context.
</inputs>

<task>
For each event, determine:
1. Does it match any positive preference topic? If yes, set `preference_match` to true and list the matched topic strings in `matched_positive_topics`.
2. Does it match any suppressed preference topic? If yes, set `suppressed_match` to true and list the matched topic strings in `matched_suppressed_topics`.
3. If nothing clearly matches, set both to false with empty lists.

Both can be true simultaneously if the household has conflicting topics configured.
</task>

<matching_rules>
Match topics SEMANTICALLY, not by exact string comparison. A preference topic names a category; match any event a reasonable parent would consider part of that category.

Orthographic and phrasing variants are the same concept:
- `Bricklabs` = `Brick Labs`
- `hot lunch` = `school lunches`
- `pizza day` = `pizza lunch`

Broad topics match specific instances:
- `Sporting Events` → volleyball, basketball, soccer, baseball, swim meet, swim city, track & field, cross-country, tournament, athletics, field day, sports day
- `School Lunch Programs` → pizza lunch, hot lunch, meal program, lunch order, food day, catered lunch
- `Heritage Or Cultural Days` → Greek Heritage Month, Sikh Heritage Month, Black History Month, Asian Heritage Month, Indigenous History Month, Nowruz, Eid, Diwali, Lunar New Year
- `Arts Programs` → art show, music concert, drama performance, band, choir, dance recital

When a topic includes supplied `aliases`, treat each alias as an additional matching term for that topic.
</matching_rules>

<grounding_rules>
Use the event title, category, model reason, grades, mentioned names, mentioned schools, and document understanding as matching context.

Do NOT match based on:
- General school relevance (e.g., "this is from the school so it matches everything").
- System defaults like school closures or grade relevance — these are handled separately by the routing layer.

When you match a topic, return the exact supplied topic string — do not paraphrase or invent new topic labels.
</grounding_rules>

<verification_loop>
Before returning your JSON:
1. Confirm each event has exactly one decision in `decisions`.
2. Confirm every string in `matched_positive_topics` appears verbatim in the supplied positive topic list.
3. Confirm every string in `matched_suppressed_topics` appears verbatim in the supplied suppressed topic list.
4. Confirm no topic strings were invented or paraphrased.
5. Confirm the JSON conforms to the schema.
</verification_loop>
"""

PREFERENCE_PARSE_SYSTEM_PROMPT = """You are LovelyChaos household preference clause classifier.
<output_contract>
- Return ONLY JSON. No prose, no markdown, no code fences, no comments, no extra keys.
- Output must conform exactly to the provided schema.
- `classifications` must contain one object per supplied clause, in the same order.
</output_contract>

<task>
Classify each pre-segmented parent preference clause as positive, negative, or unclear.
For each clause, also generate semantic aliases — short keywords or phrases that a school newsletter might use when referring to the same concept.
</task>

<decision_rules>
- `positive` means the parent wants LovelyChaos to pay more attention to the topic in future summaries.
- `negative` means the parent wants LovelyChaos to de-emphasize or suppress the topic in future summaries.
- `unclear` means the clause does not clearly express a positive or negative preference.
- Keep `topic` short and topic-like. Do not return full sentences when a shorter topic phrase is present.
- Preserve negatives like "I don't care about cultural days" as negative.
- For bare fragments like "swim days", return `unclear` with the topic copied rather than guessing sentiment.
- Do not invent topics that are not supported by the clause.
</decision_rules>

<alias_rules>
- `aliases` should contain short keywords or phrases (1-3 words each) that a school newsletter would realistically use when referring to this topic.
- Think about what specific event names, activity types, or program names a school would use in a newsletter for this category.
- For narrow/specific topics that already name a concrete thing (e.g., "pizza days"), include close spelling or phrasing variants only: ["pizza lunch", "pizza lunches"].
- For broad/categorical topics (e.g., "sporting events"), include the specific school activities that fall under that umbrella: ["volleyball", "basketball", "soccer", "baseball", "swim meet", "track and field", "cross country", "tournament", "athletics", "field day"].
- For suppressed (negative) topics, generate aliases the same way — they will be used to filter out matching content.
- Keep aliases grounded in realistic Canadian elementary/middle school contexts. Do not add obscure or unlikely activities.
- Aim for 3-10 aliases per topic depending on breadth.
- If the topic matches a supplied preset topic, return an empty aliases list (presets already have aliases configured).
</alias_rules>

<grounding_rules>
- Use only the supplied clauses, preset topic list, and any supplied prior conversation context that clarifies shorthand references.
- Do not merge multiple clauses together.
- Keep `topic` close to the parent's wording unless a clearer short label is obviously better.
- Treat the preset topic list only as context. Do not force a clause into a preset label when the parent's wording is more specific or more natural.
</grounding_rules>

<verification_loop>
Before finalizing:
- Check that each classification corresponds to exactly one supplied clause.
- Check that every negative classification came from a clear negative signal in the clause.
- Check that preset matches use the exact preset label when applicable.
- Check that aliases are short, realistic school-newsletter phrases and do not repeat the topic itself.
- Check that the JSON still matches the schema exactly.
</verification_loop>
"""

EXTRACTION_JSON_SCHEMA = {
    "name": "event_extraction",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["events", "email_level_notes"],
        "properties": {
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "title",
                        "start_at",
                        "end_at",
                        "category",
                        "confidence",
                        "target_scope",
                        "mentioned_names",
                        "mentioned_schools",
                        "target_grades",
                        "preference_match",
                        "model_reason",
                    ],
                    "properties": {
                        "title": {"type": "string"},
                        "start_at": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                        "end_at": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                        "category": {"type": "string"},
                        "confidence": {"type": "number"},
                        "target_scope": {
                            "type": "string",
                            "enum": [
                                "child_specific",
                                "grade_specific",
                                "school_specific",
                                "school_global",
                                "unknown",
                            ],
                        },
                        "mentioned_names": {"type": "array", "items": {"type": "string"}},
                        "mentioned_schools": {"type": "array", "items": {"type": "string"}},
                        "target_grades": {"type": "array", "items": {"type": "string"}},
                        "preference_match": {"type": "boolean"},
                        "model_reason": {"type": "string"},
                    },
                },
            },
            "email_level_notes": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        },
    },
    "strict": True,
}

COMMAND_JSON_SCHEMA = {
    "name": "command_parse",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "action",
            "execution_strategy",
            "event_id",
            "topic",
            "preference_behavior",
            "minutes_before",
            "reminder_channel",
            "async_requested",
            "confidence",
        ],
        "properties": {
            "action": {"type": "string", "enum": ["add", "more_info", "update", "delete", "remind", "set_preference", "none"]},
            "execution_strategy": {"type": "string", "enum": ["deterministic", "semantic", "none"]},
            "event_id": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            "topic": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            "preference_behavior": {
                "anyOf": [{"type": "string", "enum": ["auto_add", "mention", "suppress"]}, {"type": "null"}]
            },
            "minutes_before": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            "reminder_channel": {"anyOf": [{"type": "string", "enum": ["sms", "calendar"]}, {"type": "null"}]},
            "async_requested": {"type": "boolean"},
            "confidence": {"type": "number"},
        },
    },
    "strict": True,
}

FORWARDED_INTENT_JSON_SCHEMA = {
    "name": "forwarded_preface_intent",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "mode",
            "action",
            "execution_strategy",
            "event_id",
            "topic",
            "preference_behavior",
            "minutes_before",
            "reminder_channel",
            "async_requested",
            "confidence",
            "reason",
        ],
        "properties": {
            "mode": {"type": "string", "enum": ["command", "clarification", "ingestion"]},
            "action": {"type": "string", "enum": ["add", "more_info", "update", "delete", "remind", "set_preference", "none"]},
            "execution_strategy": {"type": "string", "enum": ["deterministic", "semantic", "none"]},
            "event_id": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            "topic": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            "preference_behavior": {
                "anyOf": [{"type": "string", "enum": ["auto_add", "mention", "suppress"]}, {"type": "null"}]
            },
            "minutes_before": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
            "reminder_channel": {"anyOf": [{"type": "string", "enum": ["sms", "calendar"]}, {"type": "null"}]},
            "async_requested": {"type": "boolean"},
            "confidence": {"type": "number"},
            "reason": {"type": "string"},
        },
    },
    "strict": True,
}

COMMAND_EXECUTION_JSON_SCHEMA = {
    "name": "command_execution",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["status", "message", "mutation_executed", "action", "tool_name"],
        "properties": {
            "status": {
                "type": "string",
                "enum": ["command_completed", "command_needs_clarification", "command_noop_past_event"],
            },
            "message": {"type": "string"},
            "mutation_executed": {"type": "boolean"},
            "action": {"type": "string", "enum": ["add", "more_info", "update", "delete", "remind", "set_preference", "none"]},
            "tool_name": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        },
    },
    "strict": True,
}

SUMMARY_EXTRACTION_JSON_SCHEMA = {
    "name": "summary_candidate_extraction",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["title", "candidates", "notes", "missing_requested_topics"],
        "properties": {
            "title": {"type": "string"},
            "candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "text",
                        "consolidated_priority",
                        "matched_system_defaults",
                        "matched_user_priorities",
                        "source_refs",
                        "applies_to",
                        "date_sort_key",
                        "has_date",
                        "reason",
                    ],
                    "properties": {
                        "text": {"type": "string"},
                        "consolidated_priority": {"type": "string", "enum": ["important", "mentioned", "ignore"]},
                        "matched_system_defaults": {"type": "array", "items": {"type": "string"}},
                        "matched_user_priorities": {"type": "array", "items": {"type": "string"}},
                        "source_refs": {"type": "array", "items": {"type": "string"}},
                        "applies_to": {"type": "array", "items": {"type": "string"}},
                        "date_sort_key": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                        "has_date": {"type": "boolean"},
                        "reason": {"type": "string"},
                    },
                },
            },
            "notes": {"type": "array", "items": {"type": "string"}},
            "missing_requested_topics": {"type": "array", "items": {"type": "string"}},
        },
    },
    "strict": True,
}

SUMMARY_COMPRESSION_JSON_SCHEMA = {
    "name": "summary_compression",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["title", "important_info", "other_dates", "other_topics", "missing_requested_topics", "notes"],
        "properties": {
            "title": {"type": "string"},
            "important_info": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["text", "source_refs", "applies_to", "date_sort_key"],
                    "properties": {
                        "text": {"type": "string"},
                        "source_refs": {"type": "array", "items": {"type": "string"}},
                        "applies_to": {"type": "array", "items": {"type": "string"}},
                        "date_sort_key": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    },
                },
            },
            "other_dates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["text", "source_refs", "applies_to", "date_sort_key"],
                    "properties": {
                        "text": {"type": "string"},
                        "source_refs": {"type": "array", "items": {"type": "string"}},
                        "applies_to": {"type": "array", "items": {"type": "string"}},
                        "date_sort_key": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    },
                },
            },
            "other_topics": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["text", "source_refs", "applies_to", "date_sort_key"],
                    "properties": {
                        "text": {"type": "string"},
                        "source_refs": {"type": "array", "items": {"type": "string"}},
                        "applies_to": {"type": "array", "items": {"type": "string"}},
                        "date_sort_key": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                    },
                },
            },
            "missing_requested_topics": {"type": "array", "items": {"type": "string"}},
            "notes": {"type": "array", "items": {"type": "string"}},
        },
    },
    "strict": True,
}

PREFERENCE_PARSE_JSON_SCHEMA = {
    "name": "preference_clause_parse",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["classifications"],
        "properties": {
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["clause", "polarity", "topic", "aliases"],
                    "properties": {
                        "clause": {"type": "string"},
                        "polarity": {"type": "string", "enum": ["positive", "negative", "unclear"]},
                        "topic": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                        "aliases": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
        },
    },
    "strict": True,
}

PREFERENCE_INTENT_MARKERS = (
    "i care about",
    "please mention",
    "always mention",
    "prioritize",
    "pay attention to",
    "interested in",
    "i don't care about",
    "i dont care about",
    "ignore",
    "skip",
    "suppress",
    "don't tell me about",
    "dont tell me about",
    "do not tell me about",
    "stop telling me about",
    "don't update me about",
    "dont update me about",
    "do not update me about",
    "i don't need updates on",
    "i dont need updates on",
)


class _StrictOutputModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExtractedEventOutput(_StrictOutputModel):
    title: str
    start_at: str | None
    end_at: str | None
    category: str
    confidence: float
    target_scope: Literal["child_specific", "grade_specific", "school_specific", "school_global", "unknown"]
    mentioned_names: list[str]
    mentioned_schools: list[str]
    target_grades: list[str]
    preference_match: bool
    model_reason: str


class EventExtractionOutput(_StrictOutputModel):
    events: list[ExtractedEventOutput]
    email_level_notes: str | None = None


class CommandParseOutput(_StrictOutputModel):
    action: Literal["add", "more_info", "update", "delete", "remind", "set_preference", "none"]
    execution_strategy: Literal["deterministic", "semantic", "none"]
    event_id: int | None = None
    topic: str | None = None
    preference_behavior: Literal["auto_add", "mention", "suppress"] | None = None
    minutes_before: int | None = None
    reminder_channel: Literal["sms", "calendar"] | None = None
    async_requested: bool = False
    confidence: float = 0.0


class ForwardedPrefaceIntentOutput(_StrictOutputModel):
    mode: Literal["command", "clarification", "ingestion"]
    action: Literal["add", "more_info", "update", "delete", "remind", "set_preference", "none"]
    execution_strategy: Literal["deterministic", "semantic", "none"]
    event_id: int | None = None
    topic: str | None = None
    preference_behavior: Literal["auto_add", "mention", "suppress"] | None = None
    minutes_before: int | None = None
    reminder_channel: Literal["sms", "calendar"] | None = None
    async_requested: bool = False
    confidence: float = 0.0
    reason: str


class EventRoutingValidationOutput(_StrictOutputModel):
    valid: bool
    issues: list[Literal["missing_title", "missing_time", "end_before_start", "low_confidence", "event_in_past"]]


class EventRoutingRelevancyOutput(_StrictOutputModel):
    name_match: bool = False
    name_child_ids: list[int] = []
    teacher_match: bool = False
    teacher_child_ids: list[int] = []
    school_match: bool = False
    school_child_ids: list[int] = []
    grade_match: bool = False
    grade_child_ids: list[int] = []
    preference_match: bool = False
    matched_positive_topics: list[str] = []


class EventRoutingAutoAddOutput(_StrictOutputModel):
    allow: bool
    reason: Literal[
        "missing_schedule_window",
        "grade_mismatch",
        "child_scope_mismatch",
        "grade_scope_mismatch",
        "suppressed_preference",
        "closure_or_break",
        "optional_or_admin_event",
        "household_specific_preference_event",
        "school_preference_event",
        "needs_confirmation",
    ]


class EventRoutingDecisionOutput(_StrictOutputModel):
    index: int
    validation: EventRoutingValidationOutput
    relevancy_evidence: EventRoutingRelevancyOutput
    suppressed_match: bool = False
    matched_suppressed_topics: list[str] = []
    auto_add_decision: EventRoutingAutoAddOutput
    execution_disposition: Literal["create_event", "followup_available", "informational_item", "ignore"]
    final_reason: Literal[
        "relevant_and_actionable_auto_add",
        "relevant_for_followup",
        "not_relevant_school_global",
        "not_relevant",
    ]


class EventRoutingOutput(_StrictOutputModel):
    decisions: list[EventRoutingDecisionOutput]


class PreferenceTopicMatchDecisionOutput(_StrictOutputModel):
    index: int
    preference_match: bool = False
    matched_positive_topics: list[str] = []
    suppressed_match: bool = False
    matched_suppressed_topics: list[str] = []


class PreferenceTopicMatchingOutput(_StrictOutputModel):
    decisions: list[PreferenceTopicMatchDecisionOutput]


class SummaryCandidateOutput(_StrictOutputModel):
    text: str
    consolidated_priority: Literal["important", "mentioned", "ignore"]
    matched_system_defaults: list[str]
    matched_user_priorities: list[str]
    source_refs: list[str]
    applies_to: list[str]
    date_sort_key: str | None = None
    has_date: bool
    reason: str


class SummaryCandidateExtractionOutput(_StrictOutputModel):
    title: str
    candidates: list[SummaryCandidateOutput]
    notes: list[str]
    missing_requested_topics: list[str]


class SummaryRenderedItemOutput(_StrictOutputModel):
    text: str
    source_refs: list[str]
    applies_to: list[str]
    date_sort_key: str | None = None


class SummaryCompressionOutput(_StrictOutputModel):
    title: str
    important_info: list[SummaryRenderedItemOutput]
    other_dates: list[SummaryRenderedItemOutput]
    other_topics: list[SummaryRenderedItemOutput]
    missing_requested_topics: list[str]
    notes: list[str]


class PreferenceClauseClassificationOutput(_StrictOutputModel):
    clause: str
    polarity: Literal["positive", "negative", "unclear"]
    topic: str | None = None
    aliases: list[str]


class PreferenceParseOutput(_StrictOutputModel):
    classifications: list[PreferenceClauseClassificationOutput]


class CommandExecutionOutput(_StrictOutputModel):
    status: Literal["command_completed", "command_needs_clarification", "command_noop_past_event"]
    message: str
    mutation_executed: bool = False
    action: Literal["add", "more_info", "update", "delete", "remind", "set_preference", "none"]
    tool_name: str | None = None


class MoreInfoReplyOutput(_StrictOutputModel):
    message: str


class DocumentTopicOutput(_StrictOutputModel):
    title: str
    why_it_matters: str
    action_hint: str | None = None
    timing_hint: str | None = None
    scope_hint: Literal["household_specific", "grade_specific", "school_global", "unknown"]


class DocumentRoutingHintsOutput(_StrictOutputModel):
    recap_like: bool = False
    resource_share_like: bool = False
    contains_calendar_relevant_items: bool = False


class DocumentUnderstandingOutput(_StrictOutputModel):
    document_kind: Literal["newsletter", "reminder", "recap", "resource_share", "signup", "permission", "mixed", "unknown"]
    overall_intent: Literal["actionable", "informational", "mixed"]
    assistant_summary: str
    assistant_intro: str
    actionable_topics: list[DocumentTopicOutput]
    informational_topics: list[DocumentTopicOutput]
    routing_hints: DocumentRoutingHintsOutput
    notes: list[str]


class InformationalItemOutput(_StrictOutputModel):
    title: str
    why_it_matters: str
    action_hint: str | None = None
    timing_hint: str | None = None
    scope_hint: Literal["household_specific", "grade_specific", "school_global", "unknown"]


class UnifiedExtractionOutput(_StrictOutputModel):
    document_kind: Literal["newsletter", "reminder", "recap", "resource_share", "signup", "permission", "mixed", "unknown"]
    overall_intent: Literal["actionable", "informational", "mixed"]
    scope_hint: Literal["household_specific", "grade_specific", "school_global", "unknown"]
    assistant_summary: str
    assistant_intro: str
    events: list[ExtractedEventOutput]
    informational_items: list[InformationalItemOutput]
    routing_hints: DocumentRoutingHintsOutput
    email_level_notes: str | None = None
    notes: list[str]


_DOWNSTREAM_DOCUMENT_TOPIC_LIMIT = 4
_DOWNSTREAM_DOCUMENT_NOTE_LIMIT = 3
_DOWNSTREAM_DOCUMENT_SUMMARY_MAX_CHARS = 320


@dataclass(frozen=True)
class ConversationScope:
    session_id: str | None
    workflow_name: str
    group_id: str | None
    thread_documents: tuple[ThreadDocumentContext, ...]
    household_context: dict[str, Any]
    trace_metadata: dict[str, Any]
    use_session: bool = True


@dataclass
class CommandToolRuntime:
    household_id: int
    response_channel: str
    timezone_name: str
    current_message: str
    read_preferences: Callable[[], dict]
    update_preference: Callable[[str, str, str], dict]
    search_calendar: Callable[[str, str | None, str | None, int], dict]
    add_calendar_event_from_context: Callable[[str | None, str | None, str | None, str | None, bool], dict]
    update_calendar_event: Callable[[int | None, str | None, str | None, str | None, str | None, bool | None], dict]
    delete_calendar_event: Callable[[int | None, str | None], dict]
    set_calendar_reminder: Callable[[int | None, str | None, int, str], dict]
    notes: dict[str, Any]


def _truncate_text(value: str, *, max_chars: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    cutoff = max_chars - 3
    if cutoff <= 0:
        return text[:max_chars]
    return text[:cutoff].rstrip() + "..."


def _compact_document_topic(topic: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(topic, dict):
        return None
    title = str(topic.get("title") or "").strip()
    if not title:
        return None
    compact = {
        "title": title,
        "timing_hint": _truncate_text(str(topic.get("timing_hint") or "").strip(), max_chars=140) or None,
        "action_hint": _truncate_text(str(topic.get("action_hint") or "").strip(), max_chars=140) or None,
        "why_it_matters": _truncate_text(str(topic.get("why_it_matters") or "").strip(), max_chars=180),
        "scope_hint": str(topic.get("scope_hint") or "unknown").strip() or "unknown",
    }
    return compact


def compact_document_understanding_for_downstream(
    document_understanding: dict[str, Any] | None,
    *,
    topic_limit: int = _DOWNSTREAM_DOCUMENT_TOPIC_LIMIT,
) -> dict[str, Any]:
    if not isinstance(document_understanding, dict):
        return {}
    compact: dict[str, Any] = {
        "document_kind": str(document_understanding.get("document_kind") or "").strip(),
        "overall_intent": str(document_understanding.get("overall_intent") or "").strip(),
        "assistant_summary": _truncate_text(
            str(document_understanding.get("assistant_summary") or "").strip(),
            max_chars=_DOWNSTREAM_DOCUMENT_SUMMARY_MAX_CHARS,
        ),
        "assistant_intro": _truncate_text(
            str(document_understanding.get("assistant_intro") or "").strip(),
            max_chars=220,
        ),
        "routing_hints": dict(document_understanding.get("routing_hints") or {}),
    }
    compact["actionable_topics"] = [
        item
        for item in (
            _compact_document_topic(topic)
            for topic in list(document_understanding.get("actionable_topics") or [])[: max(0, topic_limit)]
        )
        if item is not None
    ]
    compact["informational_topics"] = [
        item
        for item in (
            _compact_document_topic(topic)
            for topic in list(document_understanding.get("informational_topics") or [])[: max(0, topic_limit)]
        )
        if item is not None
    ]
    compact["notes"] = [
        _truncate_text(str(note).strip(), max_chars=180)
        for note in list(document_understanding.get("notes") or [])[:_DOWNSTREAM_DOCUMENT_NOTE_LIMIT]
        if str(note).strip()
    ]
    return {key: value for key, value in compact.items() if value not in ("", [], {}, None)}


@dataclass
class ExtractedEvent:
    title: str
    start_at: Optional[datetime]
    end_at: Optional[datetime]
    category: str
    confidence: float
    target_scope: str = "unknown"
    mentioned_names: list[str] | None = None
    mentioned_schools: list[str] | None = None
    target_grades: list[str] | None = None
    preference_match: bool = False
    model_reason: str = ""


class DecisionEngine:
    @contextmanager
    def conversation_scope(
        self,
        *,
        session_id: str | None = None,
        workflow_name: str = "LovelyChaos conversation",
        group_id: str | None = None,
        thread_documents: list[ThreadDocumentContext] | None = None,
        household_context: dict[str, Any] | None = None,
        trace_metadata: dict[str, Any] | None = None,
        use_session: bool = True,
    ) -> Iterator[None]:
        yield

    def extract_events(
        self,
        body_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        document_understanding: dict | None = None,
    ) -> dict:
        raise NotImplementedError

    def understand_document(
        self,
        *,
        analysis_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        raise NotImplementedError

    def unified_extract(
        self,
        *,
        analysis_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        raise NotImplementedError

    def parse_command(self, body_text: str) -> dict:
        raise NotImplementedError

    def parse_forwarded_preface_intent(
        self,
        *,
        user_preface: str,
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        raise NotImplementedError

    def extract_summary_candidates(self, summary_context: dict) -> dict:
        raise NotImplementedError

    def compress_summary(self, summary_context: dict) -> dict:
        raise NotImplementedError

    def route_events(
        self,
        *,
        extracted_events: list[ExtractedEvent],
        children: list[Any],
        positive_preference_topics: list[str],
        suppressed_priority_topics: list[str],
        sender_email: str = "",
        sender_display_name: str = "",
        timezone_hint: str = "UTC",
        evaluation_datetime_utc: str = "",
        document_understanding: dict | None = None,
    ) -> list[dict]:
        raise NotImplementedError

    def match_event_preferences(
        self,
        *,
        extracted_events: list[ExtractedEvent],
        positive_preference_topics: list[str],
        suppressed_priority_topics: list[str],
        document_understanding: dict | None = None,
        topic_aliases: dict[str, list[str]] | None = None,
    ) -> list[dict]:
        raise NotImplementedError

    def parse_preference_notes(self, raw_text: str, preset_topics: list[str] | None = None) -> dict:
        raise NotImplementedError

    def execute_command_with_tools(self, command_context: dict, runtime: CommandToolRuntime) -> dict:
        raise NotImplementedError

    def compose_more_info_reply(self, more_info_context: dict) -> dict:
        raise NotImplementedError

    def metadata(self) -> dict:
        return {"provider": "mock", "model": "mock", "prompt_versions": {}}


_MONTH_NAME_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_WEEKDAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def _parse_reference_datetime_hint(value: str, timezone_hint: str) -> Optional[datetime]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError, IndexError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_hint))
    return parsed.astimezone(timezone.utc)


def _mock_reference_start(
    *,
    subject: str,
    body_text: str,
    timezone_hint: str,
    reference_datetime_hint: str,
) -> Optional[datetime]:
    reference_dt = _parse_reference_datetime_hint(reference_datetime_hint, timezone_hint)
    if reference_dt is None:
        return None

    zone = ZoneInfo(timezone_hint)
    ref_local = reference_dt.astimezone(zone)
    combined = f"{subject}\n{body_text}".lower()

    explicit_date = re.search(
        r"\b("
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
        r")\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(\d{4}))?\b",
        combined,
        re.I,
    )
    if explicit_date:
        month = _MONTH_NAME_MAP[explicit_date.group(1).lower().rstrip(".")]
        day = int(explicit_date.group(2))
        year = int(explicit_date.group(3)) if explicit_date.group(3) else ref_local.year
        try:
            return datetime(year, month, day, 0, 0, tzinfo=zone).astimezone(timezone.utc)
        except ValueError:
            return None

    if re.search(r"\btomorrow\b", combined):
        target = ref_local + timedelta(days=1)
        return datetime(target.year, target.month, target.day, 0, 0, tzinfo=zone).astimezone(timezone.utc)
    if re.search(r"\b(today|tonight)\b", combined):
        return datetime(ref_local.year, ref_local.month, ref_local.day, 0, 0, tzinfo=zone).astimezone(timezone.utc)

    weekday_match = re.search(r"\b(next\s+)?(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", combined)
    if weekday_match:
        target_weekday = _WEEKDAY_INDEX[weekday_match.group(2)]
        days_ahead = (target_weekday - ref_local.weekday()) % 7
        if weekday_match.group(1):
            days_ahead = days_ahead or 7
        target = ref_local + timedelta(days=days_ahead)
        return datetime(target.year, target.month, target.day, 0, 0, tzinfo=zone).astimezone(timezone.utc)

    return None


def _tool_failure_payload(_ctx: Any, error: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "tool_error",
        "message": str(error.__class__.__name__),
    }


def _calendar_tools_enabled(ctx: RunContextWrapper[CommandToolRuntime], _agent: Agent[Any]) -> bool:
    runtime = ctx.context
    return all(
        callable(tool)
        for tool in (
            runtime.search_calendar,
            runtime.add_calendar_event_from_context,
            runtime.update_calendar_event,
            runtime.delete_calendar_event,
            runtime.set_calendar_reminder,
        )
    )


@function_tool(failure_error_function=_tool_failure_payload)
def read_preferences_tool(
    ctx: RunContextWrapper[CommandToolRuntime],
) -> dict[str, Any]:
    """Read the current household preference state for the active user."""

    return ctx.context.read_preferences()


@function_tool(failure_error_function=_tool_failure_payload)
def update_preferences_tool(
    ctx: RunContextWrapper[CommandToolRuntime],
    topic: Annotated[str, Field(description="The topic label to update, for example Pizza Days or Swim Days.")],
    behavior: Annotated[
        Literal["auto_add", "mention", "suppress"],
        Field(description="How LovelyChaos should handle this topic in future."),
    ],
    reason: Annotated[
        str,
        Field(description="A short explanation of the user's request or inferred preference change."),
    ] = "",
) -> dict[str, Any]:
    """Update a household preference when the user clearly wants future handling to change."""

    return ctx.context.update_preference(topic, behavior, reason)


@function_tool(failure_error_function=_tool_failure_payload, is_enabled=_calendar_tools_enabled)
def search_calendar_tool(
    ctx: RunContextWrapper[CommandToolRuntime],
    query: Annotated[str, Field(description="The event name, topic, or natural-language query to look up.")],
    from_iso: Annotated[
        Optional[str],
        Field(description="Optional ISO-8601 lower time bound for the lookup window."),
    ] = None,
    to_iso: Annotated[
        Optional[str],
        Field(description="Optional ISO-8601 upper time bound for the lookup window."),
    ] = None,
    limit: Annotated[int, Field(description="Maximum number of matches to return.", ge=1, le=10)] = 5,
) -> dict[str, Any]:
    """Search the user's calendar and recent follow-up context for matching events."""

    return ctx.context.search_calendar(query, from_iso, to_iso, limit)


@function_tool(failure_error_function=_tool_failure_payload, is_enabled=_calendar_tools_enabled)
def add_calendar_event_from_context_tool(
    ctx: RunContextWrapper[CommandToolRuntime],
    query: Annotated[
        Optional[str],
        Field(description="Optional natural-language add request or event reference to resolve from the current session and thread context."),
    ] = None,
    title: Annotated[Optional[str], Field(description="Explicit event title when already known.")] = None,
    start_at_iso: Annotated[Optional[str], Field(description="Explicit ISO-8601 event start time when already known.")] = None,
    end_at_iso: Annotated[Optional[str], Field(description="Explicit ISO-8601 event end time when already known.")] = None,
    all_day: Annotated[bool, Field(description="Whether the event should be treated as all-day when explicit timing is already known.")] = False,
) -> dict[str, Any]:
    """Add a calendar event by resolving it from the current session, thread, and document context, or from explicit event details."""

    return ctx.context.add_calendar_event_from_context(query, title, start_at_iso, end_at_iso, all_day)


@function_tool(failure_error_function=_tool_failure_payload, is_enabled=_calendar_tools_enabled)
def update_calendar_event_tool(
    ctx: RunContextWrapper[CommandToolRuntime],
    event_id: Annotated[Optional[int], Field(description="Internal LovelyChaos event id when known.")] = None,
    query: Annotated[Optional[str], Field(description="Natural-language event reference when id is not known.")] = None,
    title: Annotated[Optional[str], Field(description="Updated event title, if changing the title.")] = None,
    location: Annotated[Optional[str], Field(description="Updated event location, if changing the location.")] = None,
    start_at_iso: Annotated[Optional[str], Field(description="Updated ISO-8601 start time, if changing time/date.")] = None,
    end_at_iso: Annotated[Optional[str], Field(description="Updated ISO-8601 end time, if changing time/date.")] = None,
    all_day: Annotated[Optional[bool], Field(description="Whether the updated event should be all-day.")] = None,
) -> dict[str, Any]:
    """Update an existing calendar event's title, timing, or location."""

    return ctx.context.update_calendar_event(event_id, query, title, location, start_at_iso, end_at_iso, all_day)


@function_tool(failure_error_function=_tool_failure_payload, is_enabled=_calendar_tools_enabled)
def delete_calendar_event_tool(
    ctx: RunContextWrapper[CommandToolRuntime],
    event_id: Annotated[Optional[int], Field(description="Internal LovelyChaos event id when known.")] = None,
    query: Annotated[Optional[str], Field(description="Natural-language event reference when id is not known.")] = None,
) -> dict[str, Any]:
    """Delete an existing calendar event for the active household."""

    return ctx.context.delete_calendar_event(event_id, query)


@function_tool(failure_error_function=_tool_failure_payload, is_enabled=_calendar_tools_enabled)
def set_calendar_reminder_tool(
    ctx: RunContextWrapper[CommandToolRuntime],
    event_id: Annotated[Optional[int], Field(description="Internal LovelyChaos event id when known.")] = None,
    query: Annotated[Optional[str], Field(description="Natural-language event reference when id is not known.")] = None,
    minutes_before: Annotated[int, Field(description="Reminder offset in minutes before the event.", ge=1, le=10080)] = 60,
    reminder_channel: Annotated[
        Literal["sms", "calendar"],
        Field(description="Where the reminder should be scheduled."),
    ] = "sms",
) -> dict[str, Any]:
    """Set an SMS or calendar reminder for an existing event."""

    return ctx.context.set_calendar_reminder(event_id, query, minutes_before, reminder_channel)


def _deterministic_more_info_reply(more_info_context: dict[str, Any]) -> str:
    matched_item = dict(more_info_context.get("matched_item") or {})
    assistant_summary = str(more_info_context.get("assistant_summary") or "").strip()
    title = (
        str(matched_item.get("display_text") or matched_item.get("text") or matched_item.get("title") or "That topic").strip()
        or "That topic"
    )
    kind = str(matched_item.get("kind") or "").strip().lower()
    reason = str(matched_item.get("reason") or "").strip()
    assistant_detail = str(matched_item.get("assistant_detail") or "").strip()
    timing_hint = str(matched_item.get("timing_hint") or "").strip()
    action_hint = str(matched_item.get("action_hint") or "").strip()
    snippets = [
        str(value).strip()
        for value in list(more_info_context.get("source_snippets") or [])
        if str(value).strip()
    ]
    start_at = str(
        matched_item.get("start_at")
        or matched_item.get("date_sort_key")
        or ""
    ).strip()
    end_at = str(matched_item.get("end_at") or "").strip()

    summary_line = str(more_info_context.get("summary_line") or "").strip()
    if start_at:
        lead = f"Here is what I found about {title}: it is scheduled for {start_at}"
        if end_at:
            lead += f" to {end_at}"
        lead += "."
    elif kind == "topic":
        lead = f"Here is what I found about {title}: this looks like a topic or update, not a fully scheduled event."
    else:
        lead = f"Here is what I found about {title}."

    details: list[str] = []
    if assistant_detail:
        details.append(assistant_detail)
    elif snippets:
        details.append(snippets[0])
    elif summary_line and summary_line.lower() != title.lower():
        details.append(summary_line)
    elif reason:
        details.append(reason)
    elif assistant_summary:
        details.append(assistant_summary)

    if timing_hint and timing_hint not in details:
        details.append(f"Timing hint: {timing_hint}")
    if action_hint and action_hint not in details:
        details.append(action_hint)

    lower_joined = " ".join([assistant_summary, summary_line, reason, assistant_detail, *snippets]).lower()
    if not start_at and any(marker in lower_joined for marker in ("recap", "feedback", "slideshow", "activity links")):
        details.append("I do not see a new date or time for it in this update.")
    elif not start_at and kind == "topic":
        details.append("I do not see a specific date or time for it in the saved update.")

    message = " ".join([lead, *details]).strip()
    return re.sub(r"\s+", " ", message)


class MockDecisionEngine(DecisionEngine):
    def extract_events(
        self,
        body_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        document_understanding: dict | None = None,
    ) -> dict:
        lowered = body_text.lower()
        category = "school_closure" if "closure" in lowered else "general"
        confidence = 0.4 if "unclear" in lowered else 0.92
        title = subject.strip() or "School Update"
        pref_match = "closure" in lowered and "closure" in (household_preferences or "").lower()

        match = re.search(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})", body_text)
        if match:
            start = datetime.fromisoformat(f"{match.group(1)}T{match.group(2)}:00+00:00")
        else:
            start = _mock_reference_start(
                subject=subject,
                body_text=body_text,
                timezone_hint=timezone_hint,
                reference_datetime_hint=reference_datetime_hint,
            ) or (datetime.now(timezone.utc) + timedelta(days=1))
        end = start + timedelta(hours=1)
        event = ExtractedEvent(
            title=title,
            start_at=start,
            end_at=end,
            category=category,
            confidence=confidence,
            target_scope="school_global" if category == "school_closure" else "unknown",
            mentioned_names=[],
            mentioned_schools=[],
            target_grades=[],
            preference_match=pref_match,
            model_reason="mock_heuristic",
        )
        return {
            "events": [event],
            "email_level_notes": str(document_understanding.get("assistant_summary") or "").strip() or None
            if isinstance(document_understanding, dict)
            else None,
        }

    def understand_document(
        self,
        *,
        analysis_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        lowered = " ".join(
            [
                subject.lower(),
                analysis_text.lower(),
                forwarded_subject.lower(),
            ]
        )
        recap_like = any(token in lowered for token in ("recap", "feedback", "resources", "slideshow", "activity links"))
        resource_share_like = any(token in lowered for token in ("resource", "slideshow", "link", "links", "at home"))
        contains_calendar_items = any(
            token in lowered
            for token in ("meeting", "night", "day", "deadline", "register", "march", "april", "may", "june", "friday")
        )
        if recap_like and resource_share_like:
            document_kind = "resource_share"
            overall_intent = "informational"
        elif "newsletter" in lowered:
            document_kind = "newsletter"
            overall_intent = "mixed" if contains_calendar_items else "informational"
        elif "reminder" in lowered or "deadline" in lowered:
            document_kind = "reminder"
            overall_intent = "actionable"
        else:
            document_kind = "mixed" if contains_calendar_items else "unknown"
            overall_intent = "mixed" if contains_calendar_items else "informational"

        title = (subject or forwarded_subject or "School update").strip() or "School update"
        assistant_summary = (
            f"This message looks like a {document_kind.replace('_', ' ')} about {title}."
            if document_kind != "unknown"
            else f"This message is about {title}."
        )
        if recap_like:
            assistant_summary += " It mostly reads as a recap or resource share rather than a brand new event notice."
        elif contains_calendar_items:
            assistant_summary += " It appears to include at least one schedule-related topic worth checking downstream."
        else:
            assistant_summary += " It looks mostly informational."
        assistant_intro = (
            "This update mostly looks informational, but I pulled out the items that may still matter."
            if overall_intent != "actionable"
            else "This update includes items that may need a follow-up or calendar check."
        )
        topic_title = (forwarded_subject or subject or "School update").strip() or "School update"
        topic_detail = (
            "This looks like a recap or resource share."
            if recap_like
            else "This appears to be one of the main topics in the message."
        )
        target_bucket = "informational_topics" if overall_intent == "informational" else "actionable_topics"
        return {
            "document_kind": document_kind,
            "overall_intent": overall_intent,
            "assistant_summary": assistant_summary,
            "assistant_intro": assistant_intro,
            "actionable_topics": [
                {
                    "title": topic_title,
                    "why_it_matters": topic_detail,
                    "action_hint": "Check whether this should be added or followed up on." if contains_calendar_items else None,
                    "timing_hint": None,
                    "scope_hint": "unknown",
                }
            ]
            if target_bucket == "actionable_topics"
            else [],
            "informational_topics": [
                {
                    "title": topic_title,
                    "why_it_matters": topic_detail,
                    "action_hint": None,
                    "timing_hint": None,
                    "scope_hint": "school_global",
                }
            ]
            if target_bucket == "informational_topics"
            else [],
            "routing_hints": {
                "recap_like": recap_like,
                "resource_share_like": resource_share_like,
                "contains_calendar_relevant_items": contains_calendar_items,
            },
            "notes": [f"timezone_hint={timezone_hint}"] if timezone_hint else [],
        }

    def unified_extract(
        self,
        *,
        analysis_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        # Combine understand_document + extract_events for fast-path mock
        doc_understanding = self.understand_document(
            analysis_text=analysis_text,
            subject=subject,
            household_preferences=household_preferences,
            timezone_hint=timezone_hint,
            reference_datetime_hint=reference_datetime_hint,
            forwarded_subject=forwarded_subject,
            forwarded_sender=forwarded_sender,
            forwarded_date=forwarded_date,
        )
        extraction = self.extract_events(
            body_text=analysis_text,
            subject=subject,
            household_preferences=household_preferences,
            timezone_hint=timezone_hint,
            reference_datetime_hint=reference_datetime_hint,
            document_understanding=doc_understanding,
        )
        return {
            "events": extraction["events"],
            "email_level_notes": extraction.get("email_level_notes"),
            "document_understanding": doc_understanding,
        }

    def parse_command(self, body_text: str) -> dict:
        txt = body_text.lower()
        action = "none"
        preference_behavior = self._extract_preference_behavior(txt)
        if preference_behavior:
            action = "set_preference"
        elif (
            "more info" in txt
            or "tell me more" in txt
            or "more details" in txt
            or "summarize this" in txt
            or "summarize " in txt
            or txt.startswith("when is ")
            or txt.startswith("what time is ")
        ):
            action = "more_info"
        elif (
            re.search(r"\b(?:change|move|reschedule|edit)\b", txt)
            or re.search(r"^(?:please\s+)?update\b", txt)
            or re.search(r"\bupdate\s+(?:the|this|that|event|location|time|date|calendar)\b", txt)
        ):
            action = "update"
        elif "add event" in txt or "add to the calendar" in txt or "add this" in txt or "add" in txt:
            action = "add"
        elif "delete" in txt:
            action = "delete"
        elif "remind" in txt or "reminder" in txt:
            action = "remind"
        execution_strategy = self._default_execution_strategy(action)
        return {
            "action": action,
            "execution_strategy": execution_strategy,
            "async_requested": "later" in txt,
            "event_id": self._extract_int(txt),
            "topic": self._extract_topic(body_text),
            "preference_behavior": preference_behavior,
            "minutes_before": self._extract_minutes(txt),
            "reminder_channel": self._extract_reminder_channel(txt),
            "confidence": 0.9 if action != "none" else 0.4,
        }

    def parse_forwarded_preface_intent(
        self,
        *,
        user_preface: str,
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        txt = (user_preface or "").strip()
        lowered = txt.lower()
        if not txt:
            return {
                "mode": "ingestion",
                "action": "none",
                "execution_strategy": "none",
                "event_id": None,
                "topic": None,
                "preference_behavior": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.99,
                "reason": "empty_preface",
            }

        if lowered in {"fyi", "for your information", "for reference", "see below", "see attached", "forwarding"}:
            return {
                "mode": "ingestion",
                "action": "none",
                "execution_strategy": "none",
                "event_id": None,
                "topic": None,
                "preference_behavior": None,
                "minutes_before": None,
                "reminder_channel": None,
                "async_requested": False,
                "confidence": 0.98,
                "reason": "informational_preface",
            }

        command = self.parse_command(txt)
        action = str(command.get("action") or "none")
        topic = command.get("topic")

        if action == "none":
            if re.search(r"\b(?:can you|could you|what do you think|thoughts|handle this|do i need)\b", lowered):
                return {
                    "mode": "clarification",
                    "action": "none",
                    "execution_strategy": "none",
                    "event_id": None,
                    "topic": None,
                    "preference_behavior": None,
                    "minutes_before": None,
                    "reminder_channel": None,
                    "async_requested": False,
                    "confidence": 0.92,
                    "reason": "vague_help_request",
                }
            if re.search(r"\b(?:keep this on our calendar|keep this on the calendar)\b", lowered):
                action = "add"
                command["confidence"] = 0.88
            else:
                return {
                    "mode": "ingestion",
                    "action": "none",
                    "execution_strategy": "none",
                    "event_id": None,
                    "topic": None,
                    "preference_behavior": None,
                    "minutes_before": None,
                    "reminder_channel": None,
                    "async_requested": False,
                    "confidence": 0.7,
                    "reason": "no_supported_command_detected",
                }

        if action == "more_info" and not topic and forwarded_subject:
            topic = forwarded_subject.strip() or None

        return {
            "mode": "command",
            "action": action,
            "execution_strategy": self._default_execution_strategy(action),
            "event_id": command.get("event_id"),
            "topic": topic,
            "preference_behavior": command.get("preference_behavior"),
            "minutes_before": command.get("minutes_before"),
            "reminder_channel": command.get("reminder_channel"),
            "async_requested": bool(command.get("async_requested")),
            "confidence": float(command.get("confidence") or 0.0),
            "reason": "preface_supported_action",
        }

    def extract_summary_candidates(self, summary_context: dict) -> dict:
        candidates = list(summary_context.get("fallback_candidates") or [])
        return {
            "title": summary_context.get("title_hint") or "School Update",
            "candidates": candidates,
            "notes": [],
            "missing_requested_topics": list(summary_context.get("missing_requested_topics") or []),
        }

    def compress_summary(self, summary_context: dict) -> dict:
        candidates = list(summary_context.get("candidates") or [])
        important_info = [item for item in candidates if item.get("consolidated_priority") == "important"]
        mentioned = [item for item in candidates if item.get("consolidated_priority") == "mentioned"]
        other_dates = [item for item in mentioned if item.get("has_date")]
        other_topics = [item for item in mentioned if not item.get("has_date")]
        return {
            "title": summary_context.get("title_hint") or "School Update",
            "important_info": important_info,
            "other_dates": other_dates,
            "other_topics": other_topics[:4],
            "missing_requested_topics": list(summary_context.get("missing_requested_topics") or []),
            "notes": list(summary_context.get("notes") or []),
        }

    def match_event_preferences(
        self,
        *,
        extracted_events: list[ExtractedEvent],
        positive_preference_topics: list[str],
        suppressed_priority_topics: list[str],
        document_understanding: dict | None = None,
        topic_aliases: dict[str, list[str]] | None = None,
    ) -> list[dict]:
        decisions: list[dict] = []
        for index, event in enumerate(extracted_events, start=1):
            matched_positive_topics = [
                topic
                for topic in list(positive_preference_topics or [])
                if topic_matches_text(
                    topic,
                    event.title,
                    event.category,
                    event.model_reason,
                    " ".join(event.target_grades or []),
                    " ".join(event.mentioned_names or []),
                    " ".join(event.mentioned_schools or []),
                )
            ]
            matched_suppressed_topics = [
                topic
                for topic in list(suppressed_priority_topics or [])
                if topic_matches_text(
                    topic,
                    event.title,
                    event.category,
                    event.model_reason,
                    " ".join(event.target_grades or []),
                    " ".join(event.mentioned_names or []),
                    " ".join(event.mentioned_schools or []),
                )
            ]
            decisions.append(
                {
                    "index": index,
                    "preference_match": bool(matched_positive_topics),
                    "matched_positive_topics": matched_positive_topics,
                    "suppressed_match": bool(matched_suppressed_topics),
                    "matched_suppressed_topics": matched_suppressed_topics,
                }
            )
        return decisions

    def parse_preference_notes(self, raw_text: str, preset_topics: list[str] | None = None) -> dict:
        notes = (raw_text or "").strip()
        if not notes:
            return {"positive_topics": [], "negative_topics": []}
        clauses = self._segment_preference_notes(notes)
        classifications = [self._mock_classify_preference_clause(clause, preset_topics or []) for clause in clauses]
        return self._postprocess_preference_classifications(classifications, preset_topics or [])

    def execute_command_with_tools(self, command_context: dict, runtime: CommandToolRuntime) -> dict:
        parsed_command = dict(command_context.get("parsed_command") or {})
        action = str(parsed_command.get("action") or "none")
        topic = str(parsed_command.get("topic") or "").strip()
        if action == "set_preference" and topic:
            result = runtime.update_preference(
                topic,
                str(parsed_command.get("preference_behavior") or "mention"),
                "mock_command_execution",
            )
            return {
                "status": result.get("status") or "command_completed",
                "message": result.get("message") or "Preference saved.",
                "mutation_executed": bool(result.get("mutation_executed")),
                "action": action,
                "tool_name": "update_preferences_tool",
            }
        if action == "delete":
            result = runtime.delete_calendar_event(parsed_command.get("event_id"), topic or runtime.current_message)
            return {
                "status": result.get("status") or "command_needs_clarification",
                "message": result.get("message") or "I couldn't delete that event.",
                "mutation_executed": bool(result.get("mutation_executed")),
                "action": action,
                "tool_name": "delete_calendar_event_tool",
            }
        if action == "remind":
            result = runtime.set_calendar_reminder(
                parsed_command.get("event_id"),
                topic or runtime.current_message,
                int(parsed_command.get("minutes_before") or 60),
                str(parsed_command.get("reminder_channel") or "sms"),
            )
            return {
                "status": result.get("status") or "command_needs_clarification",
                "message": result.get("message") or "I couldn't set that reminder.",
                "mutation_executed": bool(result.get("mutation_executed")),
                "action": action,
                "tool_name": "set_calendar_reminder_tool",
            }
        if action == "update":
            result = runtime.update_calendar_event(
                parsed_command.get("event_id"),
                topic or runtime.current_message,
                None,
                None,
                None,
                None,
                None,
            )
            return {
                "status": result.get("status") or "command_needs_clarification",
                "message": result.get("message") or "I couldn't update that event.",
                "mutation_executed": bool(result.get("mutation_executed")),
                "action": action,
                "tool_name": "update_calendar_event_tool",
            }
        if action == "add":
            result = runtime.add_calendar_event_from_context(topic or runtime.current_message, None, None, None, False)
            return {
                "status": result.get("status") or "command_needs_clarification",
                "message": result.get("message") or "I couldn't add that event.",
                "mutation_executed": bool(result.get("mutation_executed")),
                "action": action,
                "tool_name": "add_calendar_event_from_context_tool",
            }
        if action == "more_info":
            result = runtime.search_calendar(topic or runtime.current_message, None, None, 5)
            items = list(result.get("items") or [])
            if items:
                first = items[0]
                title = str(first.get("title") or "That event")
                start_at = str(first.get("start_at") or "").strip()
                message = title if not start_at else f"{title}: {start_at}"
                return {
                    "status": "command_completed",
                    "message": message,
                    "mutation_executed": False,
                    "action": action,
                    "tool_name": "search_calendar_tool",
                }
        return {
            "status": "command_needs_clarification",
            "message": "I need a more specific command.",
            "mutation_executed": False,
            "action": action,
            "tool_name": None,
        }

    def compose_more_info_reply(self, more_info_context: dict) -> dict:
        return {"message": _deterministic_more_info_reply(more_info_context)}

    def metadata(self) -> dict:
        return {
            "provider": "mock",
            "model": "mock",
            "prompt_versions": {
                "extraction": EXTRACTION_PROMPT_VERSION,
                "command": COMMAND_PROMPT_VERSION,
                "forwarded_intent": FORWARDED_INTENT_PROMPT_VERSION,
                "command_execute": COMMAND_EXECUTION_PROMPT_VERSION,
                "route": EVENT_ROUTING_PROMPT_VERSION,
                "summary_extract": SUMMARY_EXTRACTION_PROMPT_VERSION,
                "summary_compress": SUMMARY_COMPRESSION_PROMPT_VERSION,
                "document_understanding": DOCUMENT_UNDERSTANDING_PROMPT_VERSION,
                "unified_extraction": UNIFIED_EXTRACTION_PROMPT_VERSION,
                "more_info": MORE_INFO_PROMPT_VERSION,
                "preference_parse": PREFERENCE_PARSE_PROMPT_VERSION,
                "preference_match": PREFERENCE_MATCH_PROMPT_VERSION,
            },
        }

    @staticmethod
    def _extract_int(text: str) -> Optional[int]:
        m = re.search(r"(\d+)", text)
        return int(m.group(1)) if m else None

    @staticmethod
    def _extract_minutes(text: str) -> int:
        m = re.search(r"(\d+)\s*(m|min|mins|minute|minutes)\b", text)
        if m:
            return int(m.group(1))
        return 60

    @staticmethod
    def _extract_reminder_channel(text: str) -> str:
        if "calendar" in text:
            return "calendar"
        return "sms"

    @staticmethod
    def _extract_topic(text: str) -> Optional[str]:
        raw = (text or "").strip()
        patterns = [
            r"(?:more info about|tell me more about|more details about)\s+(.+)$",
            r"(?:summarize|summary of)\s+(.+)$",
            r"(?:when is|what time is)\s+(.+)$",
            r"(?:move|change|update|edit)\s+(.+?)(?:\s+to\s+.+)?$",
            r"(?:always add|always include|always mention|please keep adding|please auto add|auto add)\s+(.+)$",
            r"(?:i care about)\s+(.+)$",
            r"(?:i don t care about|i don't care about|don t update me about|don't update me about|i don t need updates on|i don't need updates on|stop telling me about|don t bug me about|don't bug me about)\s+(.+)$",
        ]
        for pattern in patterns:
            match = re.search(pattern, raw, re.IGNORECASE)
            if match:
                topic = match.group(1).strip().strip(".?!")
                topic = re.sub(r"\b(to the calendar|to cal)\b", "", topic, flags=re.IGNORECASE).strip()
                return topic or None
        return None

    @staticmethod
    def _extract_preference_behavior(text: str) -> Optional[str]:
        if re.search(r"\b(?:always|keep)\s+(?:add(?:ing)?|include(?:ing)?)\b", text) or re.search(
            r"\bauto add\b", text
        ):
            return "auto_add"
        if re.search(r"\b(?:i care about|always mention)\b", text):
            return "mention"
        if re.search(
            r"\b(?:i don t care about|i don't care about|don t update me about|don't update me about|"
            r"i don t need updates on|i don't need updates on|stop telling me about|don t bug me about|don't bug me about)\b",
            text,
        ):
            return "suppress"
        return None

    @staticmethod
    def _default_execution_strategy(action: str) -> str:
        if action in {"add", "update", "delete", "remind", "set_preference"}:
            return "deterministic"
        if action == "more_info":
            return "semantic"
        return "none"

    @staticmethod
    def _split_topic_list(value: str) -> list[str]:
        raw = re.sub(r"\b(?:please|thanks|thank you)\b", "", value or "", flags=re.I).strip(" .")
        if not raw:
            return []
        parts = re.split(r"\s+(?:and|&)\s+|[,/]", raw)
        return [part.strip(" .") for part in parts if part and part.strip(" .")]

    @staticmethod
    def _canonicalize_topics(values: list[str], preset_topics: list[str]) -> list[str]:
        seen: set[str] = set()
        topics: list[str] = []
        for value in values:
            cleaned = MockDecisionEngine._clean_preference_topic_candidate(value)
            label = canonicalize_priority_topic(cleaned)
            if not label:
                continue
            canonical = label
            key = canonical.lower()
            if key in seen:
                continue
            seen.add(key)
            topics.append(canonical)
        return topics

    @staticmethod
    def _segment_preference_notes(notes: str) -> list[str]:
        raw_notes = re.sub(r"\s+", " ", (notes or "").strip())
        if not raw_notes:
            return []

        intent_pattern = re.compile(
            r"^(?:" + "|".join(re.escape(m) for m in PREFERENCE_INTENT_MARKERS) + r")\b",
            re.I,
        )

        # Step 1: split on sentence boundaries and semicolons/newlines (not commas yet)
        sentences = re.split(r"[\n;]+|(?<=[.!?])\s+", raw_notes)

        clauses: list[str] = []
        for sentence in sentences:
            stripped = MockDecisionEngine._strip_clause_noise(sentence)
            if not stripped:
                continue

            # Step 2: check if this sentence starts with an intent marker
            has_intent = bool(intent_pattern.search(stripped))

            if has_intent:
                # Keep the whole clause together (don't split on commas) so that
                # "i don't care about pizza days, sporting events" stays as one clause
                # with the negative intent preserved for all listed items.
                split_on_intents = re.split(
                    r"\s+(?:and|&)\s+(?=(?:"
                    + "|".join(re.escape(marker) for marker in PREFERENCE_INTENT_MARKERS)
                    + r")\b)",
                    stripped,
                    flags=re.I,
                )
                for item in split_on_intents:
                    nested = re.split(
                        r"(?<!^)\s+(?=(?:"
                        + "|".join(re.escape(marker) for marker in PREFERENCE_INTENT_MARKERS)
                        + r")\b)",
                        item.strip(),
                        flags=re.I,
                    )
                    for nested_item in nested:
                        clause = MockDecisionEngine._strip_clause_noise(nested_item)
                        if clause:
                            clauses.append(clause)
            else:
                # No intent marker — split on commas to separate bare topic fragments
                comma_parts = re.split(r"[,]+", stripped)
                for part in comma_parts:
                    part = MockDecisionEngine._strip_clause_noise(part)
                    if not part:
                        continue
                    split_on_intents = re.split(
                        r"\s+(?:and|&)\s+(?=(?:"
                        + "|".join(re.escape(marker) for marker in PREFERENCE_INTENT_MARKERS)
                        + r")\b)",
                        part,
                        flags=re.I,
                    )
                    for item in split_on_intents:
                        nested = re.split(
                            r"(?<!^)\s+(?=(?:"
                            + "|".join(re.escape(marker) for marker in PREFERENCE_INTENT_MARKERS)
                            + r")\b)",
                            item.strip(),
                            flags=re.I,
                        )
                        for nested_item in nested:
                            clause = MockDecisionEngine._strip_clause_noise(nested_item)
                            if clause:
                                clauses.append(clause)
        return clauses

    @staticmethod
    def _strip_clause_noise(value: str) -> str:
        text = re.sub(r"\s+", " ", (value or "").strip())
        text = re.sub(r"^(?:and|also)\s+", "", text, flags=re.I)
        text = re.sub(r"\s+(?:and|also)\s*$", "", text, flags=re.I)
        return text.strip(" ,;")

    @staticmethod
    def _clean_preference_topic_candidate(value: str) -> str:
        text = MockDecisionEngine._strip_clause_noise(value)
        if not text:
            return ""
        patterns = [
            r"^(?:i care about|care about|please mention|always mention|prioritize|pay attention to|interested in)\s+",
            r"^(?:i don't care about|i dont care about|ignore|skip|suppress)\s+",
            r"^(?:don't tell me about|dont tell me about|do not tell me about|stop telling me about)\s+",
            r"^(?:don't update me about|dont update me about|do not update me about)\s+",
            r"^(?:i don't need updates on|i dont need updates on)\s+",
            r"^(?:it's important to us that|it is important to us that|important to us|matters to us)\s+",
        ]
        cleaned = text
        for pattern in patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.I).strip()
        cleaned = re.sub(r"^(?:the|about)\s+", "", cleaned, flags=re.I)
        return cleaned.strip(" .!?")

    @staticmethod
    def _is_plausible_bare_topic(value: str) -> bool:
        text = MockDecisionEngine._clean_preference_topic_candidate(value)
        if not text:
            return False
        words = re.findall(r"[A-Za-z0-9']+", text)
        if not words or len(words) > 8:
            return False
        lowered = text.lower()
        if any(
            phrase in lowered
            for phrase in (
                "don't care",
                "dont care",
                "ignore",
                "stop telling",
                "update me",
                "need updates",
            )
        ):
            return False
        return True

    def _mock_classify_preference_clause(self, clause: str, preset_topics: list[str]) -> dict:
        cleaned_clause = self._strip_clause_noise(clause)
        topic = self._clean_preference_topic_candidate(cleaned_clause)
        negative_pattern = re.compile(
            r"^(?:i don't care about|i dont care about|ignore|skip|suppress|don't tell me about|"
            r"dont tell me about|do not tell me about|stop telling me about|don't update me about|"
            r"dont update me about|do not update me about|i don't need updates on|i dont need updates on)\b",
            re.I,
        )
        positive_pattern = re.compile(
            r"^(?:i care about|care about|please mention|always mention|prioritize|pay attention to|"
            r"interested in|important to us|matters to us)\b",
            re.I,
        )
        if negative_pattern.search(cleaned_clause):
            return {"clause": clause, "polarity": "negative", "topic": topic or cleaned_clause, "aliases": []}
        if positive_pattern.search(cleaned_clause):
            return {"clause": clause, "polarity": "positive", "topic": topic or cleaned_clause, "aliases": []}
        return {"clause": clause, "polarity": "unclear", "topic": topic or cleaned_clause, "aliases": []}

    def _postprocess_preference_classifications(self, classifications: list[dict], preset_topics: list[str]) -> dict:
        positive_candidates: list[str] = []
        negative_candidates: list[str] = []
        topic_aliases: dict[str, list[str]] = {}
        for item in classifications:
            clause = self._strip_clause_noise(str(item.get("clause") or ""))
            polarity = str(item.get("polarity") or "unclear").strip().lower()
            if polarity not in {"positive", "negative", "unclear"}:
                polarity = "unclear"
            topic = self._clean_preference_topic_candidate(str(item.get("topic") or ""))
            if not topic:
                topic = self._clean_preference_topic_candidate(clause)
            if not topic:
                continue
            if polarity == "unclear":
                if not self._is_plausible_bare_topic(topic):
                    continue
                polarity = "positive"
            raw_aliases = [str(a).strip().lower() for a in list(item.get("aliases") or []) if str(a).strip()]
            if raw_aliases:
                topic_aliases[topic.lower()] = raw_aliases
            candidates = self._split_topic_list(topic)
            if polarity == "negative":
                negative_candidates.extend(candidates)
            else:
                positive_candidates.extend(candidates)

        positive_topics = self._canonicalize_topics(positive_candidates, preset_topics)
        negative_topics = self._canonicalize_topics(negative_candidates, preset_topics)
        suppressed = {topic.lower() for topic in negative_topics}

        aliases_by_topic: dict[str, list[str]] = {}
        for topic in positive_topics + negative_topics:
            key = topic.lower()
            if key in topic_aliases:
                aliases_by_topic[key] = topic_aliases[key]

        return {
            "positive_topics": [topic for topic in positive_topics if topic.lower() not in suppressed],
            "negative_topics": negative_topics,
            "topic_aliases": aliases_by_topic,
        }


class OpenAIDecisionEngine(MockDecisionEngine):
    def __init__(
        self,
        api_key: str,
        model: str = "gpt-5.4-mini",
        reasoning_effort: str = "medium",
        timeout_sec: int = 60,
        base_url: str = "https://api.openai.com/v1",
        store_responses: bool = False,
        db_session_factory: Any = None,
    ):
        self.api_key = api_key
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_sec = timeout_sec
        self.base_url = base_url.rstrip("/")
        self.store_responses = bool(store_responses)
        self.db_session_factory = db_session_factory
        self._conversation_scope_var: ContextVar[ConversationScope | None] = ContextVar(
            "lovelychaos_openai_agents_conversation_scope",
            default=None,
        )
        try:
            self.sdk_version = package_version("openai-agents")
        except PackageNotFoundError:  # pragma: no cover - depends on runtime install metadata
            self.sdk_version = "unknown"
        self.agent_retry_attempts = 3
        self.agent_retry_base_delay_seconds = 0.25

    @contextmanager
    def conversation_scope(
        self,
        *,
        session_id: str | None = None,
        workflow_name: str = "LovelyChaos conversation",
        group_id: str | None = None,
        thread_documents: list[ThreadDocumentContext] | None = None,
        household_context: dict[str, Any] | None = None,
        trace_metadata: dict[str, Any] | None = None,
        use_session: bool = True,
    ) -> Iterator[None]:
        scope = ConversationScope(
            session_id=session_id,
            workflow_name=workflow_name,
            group_id=group_id,
            thread_documents=tuple(thread_documents or ()),
            household_context=dict(household_context or {}),
            trace_metadata=dict(trace_metadata or {}),
            use_session=use_session,
        )
        token = self._conversation_scope_var.set(scope)
        try:
            yield
        finally:
            self._conversation_scope_var.reset(token)

    def extract_events(
        self,
        body_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        document_understanding: dict | None = None,
    ) -> dict:
        document_context = json.dumps(
            compact_document_understanding_for_downstream(document_understanding),
            ensure_ascii=True,
            indent=2,
            default=str,
        )
        user_payload = (
            "household_preferences:\n"
            f"{household_preferences or ''}\n\n"
            "email_subject:\n"
            f"{subject}\n\n"
            "email_body:\n"
            f"{body_text}\n\n"
            "timezone_hint:\n"
            f"{timezone_hint}\n\n"
            "reference_datetime_hint:\n"
            f"{reference_datetime_hint or ''}\n\n"
            "document_understanding:\n"
            f"{document_context}\n"
        )
        parsed = self._run_agent(
            agent_name="event_extraction",
            system_prompt=EXTRACTION_SYSTEM_PROMPT,
            user_payload=user_payload,
            output_type=EventExtractionOutput,
            prompt_version=EXTRACTION_PROMPT_VERSION,
            use_session=False,
            inject_conversation_context=False,
        )
        events_raw = parsed.events
        events: list[ExtractedEvent] = []
        for item in events_raw:
            start_raw = (item.start_at or "").strip()
            end_raw = (item.end_at or "").strip()
            start = self._parse_iso_or_none(start_raw, timezone_hint=timezone_hint)
            end = self._parse_iso_or_none(end_raw, timezone_hint=timezone_hint)
            if start and not end:
                end = (
                    self._next_local_day_boundary(start, timezone_hint=timezone_hint)
                    if self._looks_like_date_only_value(start_raw)
                    else start + timedelta(hours=1)
                )
            if start and end and end <= start:
                end = (
                    self._next_local_day_boundary(start, timezone_hint=timezone_hint)
                    if self._looks_like_date_only_value(start_raw) or self._looks_like_date_only_value(end_raw)
                    else start + timedelta(hours=1)
                )
            events.append(
                ExtractedEvent(
                    title=(item.title or subject or "School Update").strip(),
                    start_at=start,
                    end_at=end,
                    category=(item.category or "general").strip() or "general",
                    confidence=float(item.confidence or 0.0),
                    target_scope=(item.target_scope or "unknown"),
                    mentioned_names=list(item.mentioned_names or []),
                    mentioned_schools=list(item.mentioned_schools or []),
                    target_grades=list(item.target_grades or []),
                    preference_match=bool(item.preference_match),
                    model_reason=(item.model_reason or ""),
                )
            )
        return {
            "events": events,
            "email_level_notes": parsed.email_level_notes if events else "empty_model_events",
        }

    def understand_document(
        self,
        *,
        analysis_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        user_payload = (
            "household_preferences:\n"
            f"{household_preferences or ''}\n\n"
            "email_subject:\n"
            f"{subject}\n\n"
            "analysis_text:\n"
            f"{analysis_text}\n\n"
            "timezone_hint:\n"
            f"{timezone_hint}\n\n"
            "reference_datetime_hint:\n"
            f"{reference_datetime_hint or ''}\n\n"
            "forwarded_subject:\n"
            f"{forwarded_subject}\n\n"
            "forwarded_sender:\n"
            f"{forwarded_sender}\n\n"
            "forwarded_date:\n"
            f"{forwarded_date}\n"
        )
        parsed = self._run_agent(
            agent_name="document_understanding",
            system_prompt=DOCUMENT_UNDERSTANDING_SYSTEM_PROMPT,
            user_payload=user_payload,
            output_type=DocumentUnderstandingOutput,
            prompt_version=DOCUMENT_UNDERSTANDING_PROMPT_VERSION,
            use_session=False,
            inject_conversation_context=True,
        )
        return parsed.model_dump(mode="json")

    def unified_extract(
        self,
        *,
        analysis_text: str,
        subject: str,
        household_preferences: str = "",
        timezone_hint: str = "UTC",
        reference_datetime_hint: str = "",
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        user_payload = (
            "household_preferences:\n"
            f"{household_preferences or ''}\n\n"
            "email_subject:\n"
            f"{subject}\n\n"
            "analysis_text:\n"
            f"{analysis_text}\n\n"
            "timezone_hint:\n"
            f"{timezone_hint}\n\n"
            "reference_datetime_hint:\n"
            f"{reference_datetime_hint or ''}\n\n"
            "forwarded_subject:\n"
            f"{forwarded_subject}\n\n"
            "forwarded_sender:\n"
            f"{forwarded_sender}\n\n"
            "forwarded_date:\n"
            f"{forwarded_date}\n"
        )
        parsed = self._run_agent(
            agent_name="unified_extraction",
            system_prompt=UNIFIED_EXTRACTION_SYSTEM_PROMPT,
            user_payload=user_payload,
            output_type=UnifiedExtractionOutput,
            prompt_version=UNIFIED_EXTRACTION_PROMPT_VERSION,
            use_session=False,
            inject_conversation_context=True,
            model_override="gpt-5.4",
            timeout_override_sec=180,
        )
        # Convert event outputs to ExtractedEvent dataclass instances (same as extract_events)
        events: list[ExtractedEvent] = []
        for item in parsed.events:
            start_raw = (item.start_at or "").strip()
            end_raw = (item.end_at or "").strip()
            start = self._parse_iso_or_none(start_raw, timezone_hint=timezone_hint)
            end = self._parse_iso_or_none(end_raw, timezone_hint=timezone_hint)
            if start and not end:
                end = (
                    self._next_local_day_boundary(start, timezone_hint=timezone_hint)
                    if self._looks_like_date_only_value(start_raw)
                    else start + timedelta(hours=1)
                )
            if start and end and end <= start:
                end = (
                    self._next_local_day_boundary(start, timezone_hint=timezone_hint)
                    if self._looks_like_date_only_value(start_raw) or self._looks_like_date_only_value(end_raw)
                    else start + timedelta(hours=1)
                )
            events.append(
                ExtractedEvent(
                    title=(item.title or subject or "School Update").strip(),
                    start_at=start,
                    end_at=end,
                    category=(item.category or "general").strip() or "general",
                    confidence=float(item.confidence or 0.0),
                    target_scope=(item.target_scope or "unknown"),
                    mentioned_names=list(item.mentioned_names or []),
                    mentioned_schools=list(item.mentioned_schools or []),
                    target_grades=list(item.target_grades or []),
                    preference_match=bool(item.preference_match),
                    model_reason=(item.model_reason or ""),
                )
            )
        # Convert informational items to document_understanding-compatible topic dicts
        actionable_topics = []
        informational_topics = []
        for item in parsed.informational_items:
            topic = {
                "title": item.title,
                "why_it_matters": item.why_it_matters,
                "action_hint": item.action_hint,
                "timing_hint": item.timing_hint,
                "scope_hint": item.scope_hint,
            }
            if item.action_hint:
                actionable_topics.append(topic)
            else:
                informational_topics.append(topic)
        document_understanding = {
            "document_kind": parsed.document_kind,
            "overall_intent": parsed.overall_intent,
            "scope_hint": parsed.scope_hint,
            "assistant_summary": parsed.assistant_summary,
            "assistant_intro": parsed.assistant_intro,
            "actionable_topics": actionable_topics,
            "informational_topics": informational_topics,
            "routing_hints": parsed.routing_hints.model_dump(mode="json"),
            "notes": list(parsed.notes or []),
        }
        return {
            "events": events,
            "email_level_notes": parsed.email_level_notes if events else "empty_model_events",
            "document_understanding": document_understanding,
        }

    def parse_command(self, body_text: str) -> dict:
        user_payload = f"message_body:\n{body_text}\n"
        parsed = self._run_agent(
            agent_name="command_parse",
            system_prompt=COMMAND_SYSTEM_PROMPT,
            user_payload=user_payload,
            output_type=CommandParseOutput,
            prompt_version=COMMAND_PROMPT_VERSION,
            use_session=True,
            inject_conversation_context=True,
        )
        action = parsed.action or "none"
        if action not in {"add", "more_info", "update", "delete", "remind", "set_preference", "none"}:
            action = "none"
        execution_strategy = parsed.execution_strategy
        if execution_strategy not in {"deterministic", "semantic", "none"}:
            execution_strategy = super()._default_execution_strategy(action)
        minutes_before = parsed.minutes_before
        if minutes_before is None:
            minutes_before = super()._extract_minutes(body_text.lower())
        reminder_channel = parsed.reminder_channel
        if reminder_channel not in {"sms", "calendar"}:
            reminder_channel = super()._extract_reminder_channel(body_text.lower())
        topic = parsed.topic
        if topic is None:
            topic = super()._extract_topic(body_text)
        preference_behavior = parsed.preference_behavior
        if preference_behavior not in {"auto_add", "mention", "suppress"}:
            preference_behavior = super()._extract_preference_behavior(body_text.lower())
        return {
            "action": action,
            "execution_strategy": execution_strategy,
            "event_id": parsed.event_id,
            "topic": (str(topic).strip() if topic is not None else None) or None,
            "preference_behavior": preference_behavior,
            "minutes_before": int(minutes_before),
            "reminder_channel": reminder_channel,
            "async_requested": bool(parsed.async_requested if parsed.async_requested is not None else "later" in body_text.lower()),
            "confidence": float(parsed.confidence or 0.0),
        }

    def parse_forwarded_preface_intent(
        self,
        *,
        user_preface: str,
        forwarded_subject: str = "",
        forwarded_sender: str = "",
        forwarded_date: str = "",
    ) -> dict:
        user_payload = (
            "user_preface:\n"
            f"{user_preface}\n\n"
            "forwarded_subject:\n"
            f"{forwarded_subject}\n\n"
            "forwarded_sender:\n"
            f"{forwarded_sender}\n\n"
            "forwarded_date:\n"
            f"{forwarded_date}\n"
        )
        parsed = self._run_agent(
            agent_name="forwarded_preface_intent",
            system_prompt=FORWARDED_INTENT_SYSTEM_PROMPT,
            user_payload=user_payload,
            output_type=ForwardedPrefaceIntentOutput,
            prompt_version=FORWARDED_INTENT_PROMPT_VERSION,
            use_session=True,
            inject_conversation_context=True,
        )
        mode = parsed.mode or "ingestion"
        if mode not in {"command", "clarification", "ingestion"}:
            mode = "ingestion"
        action = parsed.action or "none"
        if action not in {"add", "more_info", "update", "delete", "remind", "set_preference", "none"}:
            action = "none"
        execution_strategy = parsed.execution_strategy
        if execution_strategy not in {"deterministic", "semantic", "none"}:
            execution_strategy = super()._default_execution_strategy(action)
        topic = parsed.topic
        if topic is None and action == "more_info" and forwarded_subject:
            topic = forwarded_subject.strip()
        preference_behavior = parsed.preference_behavior
        if preference_behavior not in {"auto_add", "mention", "suppress"}:
            preference_behavior = super()._extract_preference_behavior(user_preface.lower())
        minutes_before = parsed.minutes_before
        reminder_channel = parsed.reminder_channel
        if reminder_channel not in {"sms", "calendar"}:
            reminder_channel = None
        return {
            "mode": mode,
            "action": action,
            "execution_strategy": execution_strategy,
            "event_id": parsed.event_id,
            "topic": (str(topic).strip() if topic is not None else None) or None,
            "preference_behavior": preference_behavior,
            "minutes_before": int(minutes_before) if minutes_before is not None else None,
            "reminder_channel": reminder_channel,
            "async_requested": bool(parsed.async_requested),
            "confidence": float(parsed.confidence or 0.0),
            "reason": (str(parsed.reason or "").strip() or "forwarded_preface_intent"),
        }

    def execute_command_with_tools(self, command_context: dict, runtime: CommandToolRuntime) -> dict:
        parsed = self._run_agent(
            agent_name="command_execute",
            system_prompt=COMMAND_EXECUTION_SYSTEM_PROMPT,
            user_payload=json.dumps(command_context, ensure_ascii=True, indent=2),
            output_type=CommandExecutionOutput,
            prompt_version=COMMAND_EXECUTION_PROMPT_VERSION,
            use_session=True,
            agent_context=runtime,
            inject_conversation_context=True,
            tools=[
                read_preferences_tool,
                update_preferences_tool,
                search_calendar_tool,
                add_calendar_event_from_context_tool,
                update_calendar_event_tool,
                delete_calendar_event_tool,
                set_calendar_reminder_tool,
            ],
        )
        return parsed.model_dump(mode="json")

    def compose_more_info_reply(self, more_info_context: dict) -> dict:
        parsed = self._run_agent(
            agent_name="more_info_reply",
            system_prompt=MORE_INFO_SYSTEM_PROMPT,
            user_payload=json.dumps(more_info_context, ensure_ascii=True, indent=2),
            output_type=MoreInfoReplyOutput,
            prompt_version=MORE_INFO_PROMPT_VERSION,
            # The matched topic and source snippets are already fully grounded here.
            # Reusing session history tends to replay prior internal JSON payloads and fallback
            # replies, which adds noise and can cause avoidable request failures.
            use_session=False,
            inject_conversation_context=False,
        )
        message = str(parsed.message or "").strip()
        if not message:
            message = _deterministic_more_info_reply(more_info_context)
        return {"message": message}

    def extract_summary_candidates(self, summary_context: dict) -> dict:
        parsed = self._run_agent(
            agent_name="summary_candidate_extraction",
            system_prompt=SUMMARY_EXTRACTION_SYSTEM_PROMPT,
            user_payload=json.dumps(summary_context, ensure_ascii=True, indent=2),
            output_type=SummaryCandidateExtractionOutput,
            prompt_version=SUMMARY_EXTRACTION_PROMPT_VERSION,
            use_session=False,
            inject_conversation_context=False,
        )
        return {
            "title": (parsed.title or "").strip(),
            "candidates": [item.model_dump(mode="json") for item in parsed.candidates],
            "notes": [str(note) for note in list(parsed.notes or []) if str(note).strip()],
            "missing_requested_topics": [str(topic) for topic in list(parsed.missing_requested_topics or []) if str(topic).strip()],
        }

    def compress_summary(self, summary_context: dict) -> dict:
        parsed = self._run_agent(
            agent_name="summary_compression",
            system_prompt=SUMMARY_COMPRESSION_SYSTEM_PROMPT,
            user_payload=json.dumps(summary_context, ensure_ascii=True, indent=2),
            output_type=SummaryCompressionOutput,
            prompt_version=SUMMARY_COMPRESSION_PROMPT_VERSION,
            use_session=False,
            inject_conversation_context=False,
        )
        return {
            "title": (parsed.title or "").strip(),
            "important_info": [item.model_dump(mode="json") for item in parsed.important_info],
            "other_dates": [item.model_dump(mode="json") for item in parsed.other_dates],
            "other_topics": [item.model_dump(mode="json") for item in parsed.other_topics],
            "missing_requested_topics": [str(topic) for topic in list(parsed.missing_requested_topics or []) if str(topic).strip()],
            "notes": [str(note) for note in list(parsed.notes or []) if str(note).strip()],
        }

    def route_events(
        self,
        *,
        extracted_events: list[ExtractedEvent],
        children: list[Any],
        positive_preference_topics: list[str],
        suppressed_priority_topics: list[str],
        sender_email: str = "",
        sender_display_name: str = "",
        timezone_hint: str = "UTC",
        evaluation_datetime_utc: str = "",
        document_understanding: dict | None = None,
        preference_match_decisions: list[dict] | None = None,
    ) -> list[dict]:
        _default_pref = {
            "preference_match": False,
            "matched_positive_topics": [],
            "suppressed_match": False,
            "matched_suppressed_topics": [],
        }
        events_payload = []
        for index, event in enumerate(extracted_events):
            event_payload = self._serialize_routing_event(index + 1, event)
            if preference_match_decisions and index < len(preference_match_decisions):
                event_payload["preference_match_result"] = preference_match_decisions[index]
            else:
                event_payload["preference_match_result"] = dict(_default_pref)
            events_payload.append(event_payload)
        parsed = self._run_agent(
            agent_name="event_routing",
            system_prompt=EVENT_ROUTING_SYSTEM_PROMPT,
            user_payload=json.dumps(
                {
                    "timezone_hint": timezone_hint,
                    "evaluation_datetime_utc": evaluation_datetime_utc,
                    "sender": {
                        "email": sender_email,
                        "display_name": sender_display_name,
                    },
                    "household_context": {
                        "positive_preference_topics": list(positive_preference_topics or []),
                        "suppressed_priority_topics": list(suppressed_priority_topics or []),
                        "children": [self._serialize_routing_child(child) for child in children],
                    },
                    "document_understanding": compact_document_understanding_for_downstream(document_understanding),
                    "events": events_payload,
                },
                ensure_ascii=True,
                indent=2,
            ),
            output_type=EventRoutingOutput,
            prompt_version=EVENT_ROUTING_PROMPT_VERSION,
            use_session=False,
            inject_conversation_context=False,
        )
        return [item.model_dump(mode="json") for item in parsed.decisions]

    def match_event_preferences(
        self,
        *,
        extracted_events: list[ExtractedEvent],
        positive_preference_topics: list[str],
        suppressed_priority_topics: list[str],
        document_understanding: dict | None = None,
        topic_aliases: dict[str, list[str]] | None = None,
    ) -> list[dict]:
        if not extracted_events:
            return []
        positive_topics_with_aliases = [
            {"label": topic, "aliases": (topic_aliases or {}).get(topic.lower(), [])}
            for topic in (positive_preference_topics or [])
        ]
        suppressed_topics_with_aliases = [
            {"label": topic, "aliases": (topic_aliases or {}).get(topic.lower(), [])}
            for topic in (suppressed_priority_topics or [])
        ]
        parsed = self._run_agent(
            agent_name="preference_match",
            system_prompt=PREFERENCE_MATCH_SYSTEM_PROMPT,
            user_payload=json.dumps(
                {
                    "household_context": {
                        "positive_preference_topics": list(positive_preference_topics or []),
                        "positive_topics_with_aliases": positive_topics_with_aliases,
                        "suppressed_priority_topics": list(suppressed_priority_topics or []),
                        "suppressed_topics_with_aliases": suppressed_topics_with_aliases,
                    },
                    "document_understanding": compact_document_understanding_for_downstream(document_understanding),
                    "events": [self._serialize_routing_event(index + 1, event) for index, event in enumerate(extracted_events)],
                },
                ensure_ascii=True,
                indent=2,
            ),
            output_type=PreferenceTopicMatchingOutput,
            prompt_version=PREFERENCE_MATCH_PROMPT_VERSION,
            use_session=False,
            inject_conversation_context=False,
        )
        return [item.model_dump(mode="json") for item in parsed.decisions]

    def parse_preference_notes(self, raw_text: str, preset_topics: list[str] | None = None) -> dict:
        clauses = self._segment_preference_notes(raw_text or "")
        if not clauses:
            return {"positive_topics": [], "negative_topics": []}
        parsed = self._run_agent(
            agent_name="preference_clause_parse",
            system_prompt=PREFERENCE_PARSE_SYSTEM_PROMPT,
            user_payload=json.dumps(
                {
                    "preset_topics": list(preset_topics or [item["label"] for item in priority_topic_catalog()]),
                    "clauses": clauses,
                },
                ensure_ascii=True,
                indent=2,
            ),
            output_type=PreferenceParseOutput,
            prompt_version=PREFERENCE_PARSE_PROMPT_VERSION,
            use_session=False,
            inject_conversation_context=False,
        )
        return self._postprocess_preference_classifications(
            [item.model_dump(mode="json") for item in parsed.classifications],
            preset_topics or [],
        )

    def metadata(self) -> dict:
        scope = self._conversation_scope_var.get()
        return {
            "provider": "openai",
            "model": self.model,
            "api": "responses",
            "runtime": "openai_agents",
            "sdk_version": self.sdk_version,
            "reasoning_effort": self.reasoning_effort if self.model.startswith("gpt-5") else None,
            "session_backend": "sqlalchemy" if self.db_session_factory else None,
            "active_session_id": scope.session_id if scope else None,
            "prompt_versions": {
                "extraction": EXTRACTION_PROMPT_VERSION,
                "command": COMMAND_PROMPT_VERSION,
                "forwarded_intent": FORWARDED_INTENT_PROMPT_VERSION,
                "command_execute": COMMAND_EXECUTION_PROMPT_VERSION,
                "route": EVENT_ROUTING_PROMPT_VERSION,
                "summary_extract": SUMMARY_EXTRACTION_PROMPT_VERSION,
                "summary_compress": SUMMARY_COMPRESSION_PROMPT_VERSION,
                "document_understanding": DOCUMENT_UNDERSTANDING_PROMPT_VERSION,
                "unified_extraction": UNIFIED_EXTRACTION_PROMPT_VERSION,
                "more_info": MORE_INFO_PROMPT_VERSION,
                "preference_parse": PREFERENCE_PARSE_PROMPT_VERSION,
            },
        }

    def _run_agent(
        self,
        *,
        agent_name: str,
        system_prompt: str,
        user_payload: str,
        output_type: type[Any],
        prompt_version: str,
        use_session: bool,
        inject_conversation_context: bool,
        agent_context: Any = None,
        tools: list[Any] | None = None,
        reasoning_effort_override: str | None = None,
        model_override: str | None = None,
        timeout_override_sec: int | None = None,
    ) -> Any:
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is not configured")
        scope = self._conversation_scope_var.get()
        effective_model = model_override or self.model
        model_settings = (
            self._agent_model_settings(reasoning_effort=reasoning_effort_override)
            if reasoning_effort_override
            else self._agent_model_settings()
        )
        agent = Agent(
            name=agent_name,
            instructions=system_prompt,
            model=effective_model,
            model_settings=model_settings,
            output_type=output_type,
            tools=list(tools or []),
        )
        session = None
        if use_session and scope and scope.use_session and scope.session_id and self.db_session_factory:
            session = DbBackedAgentSession(scope.session_id, db_session_factory=self.db_session_factory)
        trace_metadata = self._trace_metadata(scope.trace_metadata if scope else {})
        trace_metadata.update(
            self._trace_metadata(
                {
                    "prompt_version": prompt_version,
                    "agent_name": agent_name,
                    "session_id": scope.session_id if scope else None,
                    "thread_document_count": len(scope.thread_documents) if scope else 0,
                }
            )
        )
        run_config = RunConfig(
            model_provider=self._new_model_provider(),
            model_settings=model_settings,
            workflow_name=self._agent_workflow_name(scope=scope, agent_name=agent_name),
            group_id=scope.group_id if scope else None,
            trace_metadata=trace_metadata,
            session_input_callback=self._merge_session_history if session is not None else None,
            call_model_input_filter=(
                self._inject_conversation_context
                if inject_conversation_context and scope and (scope.thread_documents or scope.household_context)
                else None
            ),
        )
        result = None
        max_attempts = max(1, int(self.agent_retry_attempts))
        for attempt in range(1, max_attempts + 1):
            try:
                result = self._run_runner_sync_with_timeout(
                    agent=agent,
                    user_payload=user_payload,
                    agent_context=agent_context,
                    run_config=run_config,
                    session=session,
                    timeout_seconds=(
                        float(max(timeout_override_sec + 10, 30))
                        if timeout_override_sec
                        else self._runner_wall_clock_timeout_seconds()
                    ),
                )
                break
            except Exception as exc:
                if not self._is_retryable_agent_error(exc) or attempt >= max_attempts:
                    raise
                delay = self.agent_retry_base_delay_seconds * attempt
                logger.warning(
                    "Retrying OpenAI agent call after transient failure",
                    extra={
                        "agent_name": agent_name,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "error_type": type(exc).__name__,
                    },
                    exc_info=exc,
                )
                time.sleep(delay)
        assert result is not None
        return result.final_output_as(output_type)

    @staticmethod
    def _is_retryable_agent_error(exc: Exception) -> bool:
        if isinstance(exc, (APIConnectionError, APITimeoutError, httpx.TimeoutException, httpx.TransportError)):
            return True
        cause = exc.__cause__
        while cause is not None:
            if isinstance(cause, (APIConnectionError, APITimeoutError, httpx.TimeoutException, httpx.TransportError)):
                return True
            cause = cause.__cause__
        return False

    def _runner_wall_clock_timeout_seconds(self) -> float:
        return float(max(self.timeout_sec + 10, 30))

    @staticmethod
    def _run_runner_sync_with_timeout(
        *,
        agent: Agent,
        user_payload: str,
        agent_context: Any,
        run_config: RunConfig,
        session: Any,
        timeout_seconds: float,
    ) -> Any:
        def _invoke_runner() -> Any:
            return Runner.run_sync(
                agent,
                input=[build_text_session_item(role="user", text=user_payload)],
                context=agent_context,
                run_config=run_config,
                session=session,
            )

        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="openai-agent-run")
        try:
            future = executor.submit(copy_context().run, _invoke_runner)
            try:
                return future.result(timeout=timeout_seconds)
            except FuturesTimeoutError as exc:
                future.cancel()
                raise TimeoutError(f"Agent run exceeded {timeout_seconds:.0f}s wall-clock timeout") from exc
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _new_openai_client(self) -> AsyncOpenAI:
        return AsyncOpenAI(
            api_key=self.api_key or None,
            base_url=self.base_url,
            timeout=self.timeout_sec,
            max_retries=0,
        )

    def _new_model_provider(self) -> OpenAIProvider:
        return OpenAIProvider(
            openai_client=self._new_openai_client(),
            use_responses=True,
        )

    def _agent_model_settings(self, reasoning_effort: str | None = None) -> ModelSettings:
        if self.model.startswith("gpt-5"):
            return ModelSettings(
                reasoning=Reasoning(effort=reasoning_effort or self.reasoning_effort),
                store=self.store_responses,
                truncation="auto",
                include_usage=True,
            )
        return ModelSettings(
            temperature=0,
            store=self.store_responses,
            truncation="auto",
            include_usage=True,
        )

    def _merge_session_history(self, history: list[dict], new_items: list[dict]) -> list[dict]:
        filtered_history = [item for item in history if self._keep_history_item(item)]
        return filtered_history[-12:] + new_items

    def _inject_conversation_context(self, data: CallModelData[Any]) -> ModelInputData:
        scope = self._conversation_scope_var.get()
        if scope is None:
            return data.model_data
        injected_items: list[dict] = []
        if scope.household_context:
            injected_items.append(self._build_household_context_item(scope.household_context))
        if scope.thread_documents:
            injected_items.extend(self._build_thread_document_items(scope.thread_documents))
        if not injected_items:
            return data.model_data
        return ModelInputData(
            input=injected_items + list(data.model_data.input),
            instructions=data.model_data.instructions,
        )

    def _build_household_context_item(self, household_context: dict[str, Any]) -> dict:
        return build_text_session_item(
            role="user",
            text=(
                "Household context. This is standing reference data for every decision in this conversation, "
                "not a new user request.\n\n"
                f"{json.dumps(household_context, ensure_ascii=True, indent=2, sort_keys=True, default=str)}"
            ),
        )

    def _build_thread_document_items(self, documents: tuple[ThreadDocumentContext, ...]) -> list[dict]:
        items: list[dict] = []
        for index, document in enumerate(documents, start=1):
            text = (document.extracted_text or "").strip()
            if not text:
                continue
            items.append(
                build_text_session_item(
                    role="user",
                    text=(
                        "Thread document context. This is supporting source material for the current conversation, "
                        "not a new user request.\n"
                        f"Document {index}: {document.filename or 'attachment'}\n"
                        f"Content type: {document.content_type or 'unknown'}\n\n"
                        f"{text}"
                    ),
                )
            )
        return items

    def _keep_history_item(self, item: dict) -> bool:
        if not isinstance(item, dict):
            return False
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            return False
        text = self._item_text(item)
        if not text:
            return False
        if role == "assistant" and self._looks_like_json_text(text):
            return False
        return True

    @staticmethod
    def _item_text(item: dict) -> str:
        content = item.get("content")
        if isinstance(content, str):
            return content.strip()
        if not isinstance(content, list):
            return ""
        parts: list[str] = []
        for part in content:
            if not isinstance(part, dict):
                continue
            part_type = str(part.get("type") or "").strip().lower()
            if part_type not in {"input_text", "output_text", "text"}:
                continue
            text = str(part.get("text") or "").strip()
            if text:
                parts.append(text)
        return "\n".join(parts).strip()

    @staticmethod
    def _looks_like_json_text(value: str) -> bool:
        raw = (value or "").strip()
        if not raw or raw[0] not in "{[":
            return False
        try:
            json.loads(raw)
        except json.JSONDecodeError:
            return False
        return True

    @staticmethod
    def _agent_workflow_name(*, scope: ConversationScope | None, agent_name: str) -> str:
        root = scope.workflow_name if scope else "LovelyChaos agent run"
        normalized_agent = (agent_name or "").strip()
        if not normalized_agent:
            return root
        return f"{root}.{normalized_agent}"

    @staticmethod
    def _trace_metadata(values: dict[str, Any] | None) -> dict[str, str]:
        metadata: dict[str, str] = {}
        for key, value in dict(values or {}).items():
            if value is None:
                continue
            metadata[str(key)] = str(value)
        return metadata

    @staticmethod
    def _serialize_routing_child(child: Any) -> dict:
        teacher_contacts = []
        for contact in list(getattr(child, "teacher_contacts", []) or []):
            teacher_contacts.append(
                {
                    "id": getattr(contact, "id", None),
                    "teacher_name": str(getattr(contact, "teacher_name", "") or "").strip(),
                    "teacher_email": str(getattr(contact, "teacher_email", "") or "").strip(),
                    "status": str(getattr(contact, "status", "active") or "active").strip(),
                }
            )
        return {
            "id": getattr(child, "id", None),
            "name": str(getattr(child, "name", "") or "").strip(),
            "school_name": str(getattr(child, "school_name", "") or "").strip(),
            "grade": str(getattr(child, "grade", "") or "").strip(),
            "teacher_contacts": teacher_contacts,
        }

    @staticmethod
    def _serialize_routing_event(index: int, event: ExtractedEvent) -> dict:
        return {
            "index": index,
            "title": event.title,
            "start_at": OpenAIDecisionEngine._serialize_datetime(event.start_at),
            "end_at": OpenAIDecisionEngine._serialize_datetime(event.end_at),
            "category": event.category,
            "confidence": event.confidence,
            "target_scope": event.target_scope,
            "mentioned_names": list(event.mentioned_names or []),
            "mentioned_schools": list(event.mentioned_schools or []),
            "target_grades": list(event.target_grades or []),
            "preference_match": bool(event.preference_match),
            "model_reason": event.model_reason,
        }

    @staticmethod
    def _serialize_datetime(value: Optional[datetime]) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _parse_iso_or_none(value: Optional[str], *, timezone_hint: str = "UTC") -> Optional[datetime]:
        raw = (value or "").strip()
        if not raw:
            return None
        zone = OpenAIDecisionEngine._zoneinfo_or_utc(timezone_hint)
        if OpenAIDecisionEngine._looks_like_date_only_value(raw):
            try:
                parsed = datetime.fromisoformat(raw)
            except ValueError:
                return None
            return datetime(parsed.year, parsed.month, parsed.day, 0, 0, tzinfo=zone).astimezone(timezone.utc)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=zone).astimezone(timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _looks_like_date_only_value(value: Optional[str]) -> bool:
        return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", (value or "").strip()))

    @staticmethod
    def _zoneinfo_or_utc(timezone_hint: str) -> ZoneInfo | timezone:
        try:
            return ZoneInfo(timezone_hint)
        except Exception:
            return timezone.utc

    @staticmethod
    def _next_local_day_boundary(start_at: datetime, *, timezone_hint: str) -> datetime:
        zone = OpenAIDecisionEngine._zoneinfo_or_utc(timezone_hint)
        start_local = start_at.astimezone(zone)
        next_day = datetime(
            start_local.year,
            start_local.month,
            start_local.day,
            0,
            0,
            tzinfo=zone,
        ) + timedelta(days=1)
        return next_day.astimezone(timezone.utc)
