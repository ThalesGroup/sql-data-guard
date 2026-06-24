## 1. Project Initialization & Dependency Restructuring (uv)

- [x] 1.1 Move package configurations and target Python version (`>=3.13`) into `pyproject.toml`
- [x] 1.2 Setup PEP 735 `[dependency-groups]` (`dev`, `test`, `docs`) replacing separate requirements files
- [x] 1.3 Initialize deterministic lockfile with `uv lock` or `uv sync`
- [x] 1.4 Safely delete legacy `requirements.txt` and `test/test.requirements.txt`

## 2. Test Suite Pluralization & Verification

- [x] 2.1 Rename the legacy `/test/` directory to `/tests/`
- [x] 2.2 Update pytest and coverage path settings in `pyproject.toml` to point to `/tests/`
- [x] 2.3 Execute unit tests using `uv run pytest` to ensure full environment and dialect compatibility

## 3. Ruff Integration

- [x] 3.1 Define `[tool.ruff]` and linter rule groups (`E`, `F`, `I`, `N`, `W`, `B`, `UP`, `C4`) inside `pyproject.toml`
- [x] 3.2 Run `uv run ruff format .` and `uv run ruff check --fix .` across the codebase to establish clean format
- [x] 3.3 Verify there are zero style or quality violations remaining

## 4. FastAPI & REST API Migration

- [x] 4.1 Re-implement `/verify-sql` endpoint in `src/sql_data_guard/rest/sql_data_guard_rest.py` using FastAPI and Uvicorn
- [x] 4.2 Construct explicit Pydantic request models (`VerifySQLRequest`) to ensure payload validation
- [x] 4.3 Update REST test suite in `tests/test_rest_api_unit.py` using FastAPI `TestClient` and `httpx` to verify payload error boundaries

## 5. Container & MCP Modernization

- [x] 5.1 Rename `Dockerfile` to `Containerfile` and re-author using highly optimized multi-stage `uv` build and run stages
- [x] 5.2 Rename `wrapper.Dockerfile` to `wrapper.Containerfile` and re-author to package MCP wrapper
- [x] 5.3 Update local docker/podman script references, testing commands, or docs targeting old Dockerfiles

## 6. Sphinx Documentation & Pages Automation

- [x] 6.1 Create `/docs/source/` structure and configure `conf.py` to enable Sphinx autodoc, MyST Parser, and RTD theme
- [x] 6.2 Draft `index.md` (overview) and `api.md` (autogenerating detailed library references from code docstrings)
- [x] 6.3 Compose `.github/workflows/docs.yml` to compile documentation and automatically publish built HTML to GitHub Pages

## 7. CI Workflow Modernization & Quality Script Updates

- [x] 7.1 Modernize `.github/workflows/test.yaml` and `.github/workflows/python-compatability-test.yaml` using `astral-sh/setup-uv`
- [x] 7.2 Update `generate_quality_report.py` to run and parse `ruff check` and `ruff format` instead of legacy `flake8`
- [x] 7.3 Run the quality report script via `uv run python scripts/generate_quality_report.py` to verify green scores
