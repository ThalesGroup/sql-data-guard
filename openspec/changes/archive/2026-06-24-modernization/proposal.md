## Why

The `sql-data-guard` project currently relies on older Python environments, fragmented dependency managers (split across `requirements.txt` and `test.requirements.txt`), manual linting via `flake8` without standard auto-formatting, and synchronous Flask for the REST API. Modernizing the stack to Python 3.13, unified Astral tooling (`uv` and `ruff`), and an OpenAPI-first asynchronous framework (`FastAPI`) will dramatically improve developer ergonomics, security, testing stability, API interactive exploration, and overall performance.

## What Changes

- **Upgrade to Python 3.13**: Set the minimum required Python version to `>=3.13` across the project, CI workflows, and containers.
- **Single-source Dependency Management via `uv`**: Eliminate all `requirements.txt` and `test.requirements.txt` files, consolidation into standard PEP 621 metadata with PEP 735 dependency groups inside `pyproject.toml`, and lock package trees deterministically in `uv.lock`.
- **Adopt Ruff**: Replace `flake8` with `ruff` for ultra-fast unified linting and code formatting, configuring standard rules directly in `pyproject.toml`.
- **Migrate REST API to FastAPI**: Replace Flask with FastAPI for the HTTP verification endpoint, using Pydantic models for type-safe requests and enabling automatic interactive Swagger documentation (**BREAKING** for REST starting commands and Docker run arguments).
- **OCI Containerfile Standard**: Rename `Dockerfile` and `wrapper.Dockerfile` to `Containerfile` and `wrapper.Containerfile` respectively, and optimize them with multi-stage builds powered by `uv`.
- **Pluralize Test Folder**: Move `/test/` to `/tests/` to conform to modern community conventions, updating all paths and CI actions.
- **Sphinx Documentation with GitHub Pages**: Set up comprehensive Sphinx documentation in `/docs` using MyST Parser (for Markdown files) and a robust GitHub Actions workflow to publish automatically on GitHub Pages.

## Capabilities

### New Capabilities
- `fastapi-endpoint`: FastAPI-based async REST service with interactive OpenAPI docs, request schema parsing via Pydantic, and Uvicorn server integration.
- `sphinx-documentation`: Complete Markdown-based documentation structure under `/docs` compiled into static HTML and deployed to GitHub Pages via Git actions.

### Modified Capabilities
- `quality-report-tool`: Modernize the `generate_quality_report.py` script to use Ruff (instead of Flake8), target the new `tests/` directory, and run seamlessly with `uv`.
- `test-verification-utils`: Relocate and adapt the entire unit test suite to reside inside the pluralized `/tests` folder, executing via `uv run pytest`.

## Impact

- **Affected code**: `src/sql_data_guard/rest/sql_data_guard_rest.py`, `scripts/generate_quality_report.py`.
- **APIs**: Restructures `/verify-sql` using Pydantic schemas (compatible with the old JSON schema but with explicit status codes and automatic error responses).
- **Containers**: Relocates `Dockerfile` and `wrapper.Dockerfile` to `Containerfile` and `wrapper.Containerfile`, replacing pip installs with `uv` virtual environments.
- **CI/CD**: Standardizes GitHub Actions on `astral-sh/setup-uv`, updating compatibility matrices and tests to utilize `uv run pytest`.
