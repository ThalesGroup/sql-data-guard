## Why

Currently, there is no standardized way to verify the code, testing, or documentation quality of the `sql-data-guard` project. Providing a lightweight, programmatic assessment tool will allow developers and CI to easily monitor these quality metrics without introducing blockages or over-engineered gates.

## What Changes

- Add a lightweight quality reporter script (`scripts/generate_quality_report.py`) that executes existing quality tools (`flake8`, `pytest --cov`, and `interrogate`) and aggregates their results.
- Add `interrogate` and `pytest-cov` to the development requirements to support the reporting tool.
- Generate a beautiful, standardized `QUALITY_REPORT.md` dashboard at the root of the project.

## Capabilities

### New Capabilities
- `quality-report-tool`: Automatically runs code formatting/lint checks, test code coverage, and docstring coverage checks, aggregating the metrics into a Markdown report.

### Modified Capabilities
<!-- No modified capabilities -->

## Impact

- Adds a development script in `scripts/`.
- Updates `test/test.requirements.txt` to include `pytest-cov`.
- Adds `pyproject.toml` configuration for `interrogate`.
- Generates `QUALITY_REPORT.md` at the workspace root when run.
