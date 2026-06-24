## ADDED Requirements

### Requirement: Quality Report Execution
The system SHALL provide a CLI command to run the quality reporting tool.

#### Scenario: Running the quality report command
- **WHEN** the user executes `python scripts/generate_quality_report.py`
- **THEN** the system runs the linter, test coverage, and docstring coverage checks, then generates the report file.

### Requirement: Code Quality Integration
The system SHALL execute the `flake8` linter check and capture any style violations.

#### Scenario: Running flake8 checks
- **WHEN** the flake8 check is executed on the `src/` codebase
- **THEN** the system captures lint errors and notes them for the report.

### Requirement: Test Quality Integration
The system SHALL run `pytest` with coverage instrumentation and generate an XML report to parse the line and branch coverage.

#### Scenario: Parsing coverage xml
- **WHEN** pytest coverage is executed and `coverage.xml` is generated
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
