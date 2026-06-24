## Context

The `sql-data-guard` repository currently implements a SQL verification and guardrail library. It is packaged via standard `setuptools` with a custom `pyproject.toml`. However, it relies on obsolete patterns:
- Split dependency files (`requirements.txt` and `test/test.requirements.txt`).
- No standard formatting configuration; style enforcement is done via legacy `flake8`.
- A Flask-based REST service.
- Single-stage `Dockerfile` and `wrapper.Dockerfile` using traditional `pip` inside Python 3.12.
- A non-plural `test/` directory.
- No automated documentation system.

This design outlines the migration path to modernize every aspect of the project to match modern Python 3.13 packaging, deployment, quality control, and API standards.

---

## Goals / Non-Goals

**Goals:**
- **Standardize on Python 3.13**: Set language constraints and container runtimes to Python 3.13.
- **Consolidate with `uv`**: Manage both runtime and development dependencies inside `pyproject.toml` (under modern PEP 735 `[dependency-groups]`), maintaining single-command onboarding (`uv sync`).
- **Unify Style with Ruff**: Integrate `ruff` to provide instantaneous linting and code formatting.
- **FastAPI Migration**: Upgrade the REST server to FastAPI for automatic Swagger documentation and structural type-safety.
- **Community-Standard Test Layout**: Rename `test/` to `tests/` and update all pytest runs.
- **OCI Containerfiles**: Transition Dockerfiles to modern OCI-compliant Containerfiles with multi-stage `uv` builds.
- **Self-Publishing Sphinx Docs**: Implement Markdown-friendly Sphinx documentation deployed on GitHub Pages via Actions.

**Non-Goals:**
- No changes to the core `sql_data_guard.py` engine or its verification logic.
- No changes to the underlying Dify plugin packaging or MCP Wrapper interceptor logic.

---

## Decisions

### 1. Web Framework: FastAPI & Uvicorn (replacing Flask)
* **Rationale**: FastAPI uses Pydantic for validation, which prevents manually parsing JSON objects or returning verbose `jsonify({"error": ...})` errors. FastAPI automatically generates interactive docs (`/docs` and `/redoc`), greatly simplifying developer exploration of SQL Data Guard. Uvicorn acts as a high-performance ASGI server.
* **Alternatives Considered**: 
  - *Keep Flask*: Simpler diff, but misses out on interactive API exploration, async capabilities, and native schema validation.

### 2. Dependency Management: `uv` (replacing pip & setuptools/build)
* **Rationale**: `uv` replaces `pip`, `venv`, and packaging backends. By using the `hatchling` build-backend and `uv`, we can manage dependencies with extreme speed.
* **Configuration**:
  ```toml
  [build-system]
  requires = ["hatchling"]
  build-backend = "hatchling.build"

  [project]
  name = "sql-data-guard"
  dynamic = ["version"]
  requires-python = ">=3.13"
  dependencies = ["sqlglot>=25.0.0"]

  [dependency-groups]
  dev = [
      "ruff>=0.8.0",
      "interrogate>=1.7.0",
  ]
  test = [
      "pytest>=8.0.0",
      "pytest-cov>=5.0.0",
      "duckdb>=1.0.0",
      "httpx>=0.27.0",
  ]
  docs = [
      "sphinx>=8.0.0",
      "sphinx-rtd-theme>=2.0.0",
      "myst-parser>=3.0.0",
  ]
  ```

### 3. Linting/Formatting: Ruff (replacing Flake8)
* **Rationale**: Replaces flake8, isort, and other linters with a single, highly configurable tool.
* **Configuration**:
  ```toml
  [tool.ruff]
  line-length = 88
  target-version = "py313"

  [tool.ruff.lint]
  select = ["E", "F", "I", "N", "W", "B", "UP", "C4"]
  ```

### 4. Containers: Multi-stage OCI Containerfiles (replacing Dockerfiles)
* **Rationale**: Containerfile is the OCI-standard format. Multi-stage builds using `uv` ensure that no compilation tools or heavy cache layers are included in the final image, reducing image size from ~150MB to <60MB.
* **Containerfile Paths**:
  - `Dockerfile` ──▶ `Containerfile`
  - `wrapper.Dockerfile` ──▶ `wrapper.Containerfile`

### 5. Documentation: Sphinx + MyST Parser + GitHub Pages
* **Rationale**: We want rich, automated docs without forcing developers to learn reStructuredText. `myst-parser` enables writing elegant documentation in Markdown, while Sphinx's `autodoc` extracts modules' docstrings automatically.

---

## Risks / Trade-offs

* **[Risk] Non-PEP-440 Versioning**: The current `version = "UPDATED-BY-WORKFLOW"` inside `pyproject.toml` is a placeholder updated during CI. `uv` parses this strictly and will crash.
  * *Mitigation*: Configure `dynamic = ["version"]` under `[project]` and configure Hatch to use `hatch-vcs` or fall back to reading `src/sql_data_guard/__init__.py`'s `__version__`.
* **[Risk] FastAPI Router Porting**: Endpoints might respond with different HTTP status codes or schemas if not mapped perfectly.
  * *Mitigation*: We will write high-fidelity FastAPI unit tests in `tests/test_rest_api_unit.py` using FastAPI's `TestClient` and `httpx` to guarantee 100% backward compatibility of response bodies and status codes.
