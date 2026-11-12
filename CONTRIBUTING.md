# Contributing

Thanks for improving the AAP RCA agent. Python source code lives in the
installable `src/rca` package; the shell batch entry point remains only as a
compatibility wrapper.

## Development

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

The PostgreSQL integration tests can be run with
`tests/run_integration_tests.sh`.

## Making changes

- Add runtime and development dependencies to `pyproject.toml`.
- Keep Python imports within `rca.*`; do not add `sys.path` or `PYTHONPATH`
  workarounds.
- Put reusable Python code in `src/rca/`. Define JSON artifact contracts in
  `rca.models` and generate API schemas with `model_json_schema()`; do not
  maintain separate JSON schema files. Load packaged prompts with
  `importlib.resources`.
- If Helm behavior changes, update `deploy/helm/values.example.yaml` and the
  deployment documentation.

## Secrets and analysis data

Never commit `.claude/`, `.env` files, Helm values containing real credentials,
or analysis output. These files can contain tokens, connection details, job
logs, or customer data. Verify `git status` before committing local test data.
