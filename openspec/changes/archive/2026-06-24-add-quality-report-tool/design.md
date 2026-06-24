## Context

Currently, the `sql-data-guard` project does not have a consolidated mechanism to monitor and showcase code quality, test coverage, or docstring coverage. Creating a script to run and aggregate these statistics will keep the code clean and well-documented.

## Goals / Non-Goals

**Goals:**
- Provide a simple, fast development script (`scripts/generate_quality_report.py`) that aggregates flake8 lint, pytest coverage, and interrogate docstring coverage.
- Write a consolidated `QUALITY_REPORT.md` markdown file in the workspace root.
- Keep execution fast and completely non-blocking for local developers.

**Non-Goals:**
- No blocking of git commits or push requests in this iteration.
- No heavy third-party dashboard or reporting services (keep it local and simple).

## Decisions

### 1. Script Location and Implementation
We will implement the reporter in `scripts/generate_quality_report.py` using Python 3 standard library `subprocess` to trigger tools, `xml.etree.ElementTree` to parse `coverage.xml`, and the `json` module to parse JSON responses from `interrogate`.

### 2. Integration with Flake8
`flake8` will be called to capture lint violations across the codebase, allowing us to easily count and list code issues.

### 3. Integration with Pytest Coverage
We will add `pytest-cov` to `test/test.requirements.txt`. The script will invoke `pytest --cov=src --cov-report=xml` and parse the output `coverage.xml` file.

### 4. Integration with Interrogate
`interrogate` will be added to `test/test.requirements.txt`. The script will run `interrogate --format json src/` to extract coverage percent and missing docstring targets.

## Risks / Trade-offs

- **Risk:** `pytest` could take a few seconds to run during generation.
  - *Mitigation:* This script is manually executed on-demand or during CI, so it does not block the daily edit-save-run cycle of unit tests.
- **Risk:** subprocess calls fail if tools are not installed.
  - *Mitigation:* Ensure robust error messages instructing the user to run `pip install -r test/test.requirements.txt` if any tool is missing.
