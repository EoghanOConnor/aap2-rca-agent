# AGENTS.md

These rules apply to the entire repository. Treat the flowchart in the
`README.md` **Architecture** section as the authoritative pipeline description
when implementing changes. Follow `CONTRIBUTING.md` for setup and contribution
conventions.

## Maintainability

- Keep each change focused on the requested behavior or documentation; avoid
  unrelated refactors.
- Use the smallest maintainable implementation that follows existing style and
  repository patterns.
- Trace new code through its callers and update focused tests and affected
  documentation before completing behavior changes.
- Never commit secrets, `.env` files, local Helm values, analysis output, or
  other sensitive runtime data.

## Compatibility

Before completing a change, verify that new code is compatible with the current
implementation, callers, public interfaces, configuration, documentation, and
tests. Explicitly review externally visible contracts for breaking changes:

- Command-line interfaces
- Python APIs
- Configuration
- Schemas
- Report formats
- Database interfaces
- Deployment behavior

If a change can break consumers, preserve backward compatibility or provide
the required migration and deployment handling, and explain that decision in
the change description.

Existing schemas must remain unchanged. Never alter or remove fields from an
existing schema; if a schema change appears necessary, stop and request
maintainer direction.

## Testing

Install the development environment with:

```bash
.venv/bin/pip install -e '.[dev]'
```

Run the tests applicable to the change:

```bash
# Python behavior
.venv/bin/pytest -m "not integration"

# PostgreSQL behavior
tests/run_integration_tests.sh

# Helm and deployment behavior
helm lint deploy/helm -f deploy/helm/values.example.yaml
```

Never point integration-test database settings at production.
