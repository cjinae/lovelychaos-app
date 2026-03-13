# Testing and Quality Gates

## Required automated suites
- Lint/static checks (`ruff`)
- Unit tests (`tests/unit`)
- Integration tests (`tests/integration`)
- API contract tests (`tests/contracts`)
- UI smoke tests (`tests/ui`)
- Security regression tests (`tests/security`)

## Local execution

```bash
make db-upgrade
make ci
```

## CI merge policy
- All required checks are blocking.
- No warn-only path for required suites.

## Flake policy
- Quarantine requires an explicit tracking ticket and owner.
- Quarantined tests cannot be used to cover critical security paths.
