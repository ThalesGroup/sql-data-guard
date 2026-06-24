# SQL Data Guard Agent Guidelines

This guide focuses on repository-specific gotchas, commands, and architecture that an AI agent might otherwise miss or guess wrong.

## Development Setup & Quirks

### Critical: pyproject.toml Version Gotcha
- `pyproject.toml` contains `version = "UPDATED-BY-WORKFLOW"`. This is non-PEP-440 compliant.
- **Consequence**: Modern strict package managers/parsers like `uv` or `poetry` will fail with TOML parsing errors.
- **Remedy**: Use standard `venv` and `pip` for installation, or temporarily patch `version = "0.0.1"` if `uv`/`poetry` must be used.

### Setup Commands
To set up the development environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r test/test.requirements.txt
```

---

## Verifying Changes (Testing)

### Run Unit Tests
Unit tests include files ending in `_unit.py` and `test_verification_utils.py` (which is often missed because it does not end in `_unit.py`):
```bash
PYTHONPATH=src python -m pytest --color=yes test/*_unit.py test/test_verification_utils.py
```

### Run a Focused Test
```bash
PYTHONPATH=src python -m pytest test/test_verification_utils.py -k "test_split_to_expressions_matching"
```

### LLM Integration Tests
- Location: `test/test_sql_guard_llm.py`
- **Quirk**: Requires AWS Bedrock permissions configured via AWS environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, and optionally `AWS_SESSION_TOKEN`). Do not run them locally if AWS credentials are missing.
- Command:
  ```bash
  PYTHONPATH=src python -m pytest --color=yes test/test_sql_guard_llm.py
  ```

### Quality & Coverage Reporting
To evaluate and generate a consolidated report on the project's code style, test coverage, and docstring completeness, execute:
```bash
python scripts/generate_quality_report.py
```
- **How it works:** This script programmatically runs `flake8` (linting), `pytest --cov` (statement and branch coverage), and `interrogate` (docstrings).
- **Output:** It aggregates all statistics and details into a beautiful markdown report dashboard located at **`QUALITY_REPORT.md`** at the project root.

---

## Architecture & Sub-project Boundaries

### Core Library
- Located in `src/sql_data_guard/`.
- Uses the `sqlglot` library to parse, analyze, and rewrite SQL queries.

### REST API
- Located in `src/sql_data_guard/rest/sql_data_guard_rest.py`.
- Run/build using Flask and configured via `Dockerfile`.

### MCP Wrapper
- Located in `src/sql_data_guard/mcpwrapper/mcp_wrapper.py`.
- Managed/packaged via `wrapper.Dockerfile`.
- **How it works**: Intercepts standard JSON-RPC stdin/stdout streams between the MCP client and an inner MCP server container (e.g. SQLite or Postgres). It parses, verifies, and optionally rewrites the SQL queries before passing them to the inner container.

### Dify Plugin
- Located in `plugins/dify/`.
- Packaged locally using Dify CLI:
  ```bash
  dify plugin package ../dify --output_path sql_data_guard.difypkg
  ```
