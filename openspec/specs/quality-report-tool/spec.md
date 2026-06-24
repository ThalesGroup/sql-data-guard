# quality-report-tool Specification

## Purpose
TBD - created by synchronizing delta specs for change add-quality-report-tool. Update Purpose after archive.
## Requirements
### Requirement: Quality Report Execution
The system SHALL provide a CLI command to run the quality reporting tool under the `uv` virtual environment.

#### Scenario: Running the quality report command
- **WHEN** the user executes `uv run python scripts/generate_quality_report.py`
- **THEN** the system runs Ruff, Pytest test coverage (targeting `/tests`), and docstring coverage checks, generating a report file.

### Requirement: Code Quality Integration
The system SHALL execute the `ruff` check and format linter tools and capture any style violations.

#### Scenario: Running ruff checks
- **WHEN** the Ruff checks are executed on the codebase
- **THEN** the system captures code violations, formatting warnings, and notes them for the report.

### Requirement: Test Quality Integration
The system SHALL run `pytest` pointing to the `/tests` directory with coverage instrumentation and generate an XML report to parse the line and branch coverage.

#### Scenario: Parsing coverage xml
- **WHEN** pytest coverage is executed on the `/tests` directory and `coverage.xml` is generated
- **THEN** the system parses the file to extract overall statement and branch coverage percentages, and per-file coverage.

### Requirement: Documentation Quality Integration
The system SHALL run `interrogate` on the codebase to check docstring coverage and list any missing docstrings.

#### Scenario: Parsing interrogate output
- **WHEN** interrogate is run on `src/`
- **THEN** the system extracts overall docstring coverage percentage and the list of classes, methods, or modules lacking docstrings.

### Requirement: Markdown Quality Report Generation
The system SHALL generate a unified Markdown quality report dashboard at `QUALITY_REPORT.md` at the project's root.

#### Scenario: Generating QUALITY_REPORT.md
- **WHEN** all quality check metrics are successfully gathered
- **THEN** the system formats the aggregated metrics into a beautiful, human-readable table with pass/warning status indications and details.

