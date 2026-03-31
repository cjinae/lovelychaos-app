# Testing and Quality Gates

## Required automated suites
- Lint/static checks (`ruff`)
- Unit tests (`tests/unit`)
- Integration tests (`tests/integration`)
- API contract tests (`tests/contracts`)
- Cross-channel tests (`tests/cross_channel`)
- UI smoke tests (`tests/ui`)
- Security regression tests (`tests/security`)

## Cross-channel testing (unified brain)

The `tests/cross_channel/` suite validates the "one brain" architecture where email and SMS share context.

**SmsSimulator** (`tests/cross_channel/sms_simulator.py`):
- Wraps the test client to send SMS and email payloads with auto-incrementing IDs
- Mock mode: deterministic templates for intents (`more_info`, `add_event`, `confirm`, etc.)
- LLM mode: calls OpenAI to generate natural parent SMS from conversation history
- `run_scenario(steps)` executes multi-step cross-channel scenarios

**Functional tests** (always run, use mock LLM):
- `test_cross_channel_context.py` — followup context crosses channel boundaries
- `test_cross_channel_documents.py` — SMS accesses email-originated ThreadDocuments
- `test_cross_channel_session.py` — email and SMS share a unified household session
- `test_cross_channel_formatting.py` — channel tags not leaked in user-facing responses
- `test_e2e_unified_brain.py` — real-content scenarios using Frankland newsletter data

**E2E tests** (opt-in, require real LLM):
- `test_e2e_conversations.py` — marked `@pytest.mark.e2e`, excluded by default
- Run with: `LOVELYCHAOS_E2E_LLM=1 python3 -m pytest -m e2e -v`

## Local execution

```bash
make db-upgrade
make ci
```

Run cross-channel tests only:

```bash
python3 -m pytest tests/cross_channel/ -v
```

## CI merge policy
- All required checks are blocking.
- No warn-only path for required suites.
- E2E tests (`@pytest.mark.e2e`) are excluded from CI by default via `addopts = "-m 'not e2e'"` in `pyproject.toml`.

## Flake policy
- Quarantine requires an explicit tracking ticket and owner.
- Quarantined tests cannot be used to cover critical security paths.
