## 1. Setup and Dependencies

- [x] 1.1 Add `pytest-cov` and `interrogate` to `test/test.requirements.txt`
- [x] 1.2 Install the updated requirements using pip
- [x] 1.3 Add standard config block for `interrogate` in `pyproject.toml`

## 2. Core Implementation

- [x] 2.1 Create `scripts/` directory if it does not exist
- [x] 2.2 Implement `scripts/generate_quality_report.py` with standard `subprocess` execution to run Flake8
- [x] 2.3 Implement coverage XML parsing in the script using `xml.etree.ElementTree`
- [x] 2.4 Implement interrogate JSON parsing in the script
- [x] 2.5 Aggregate the data and write a formatted `QUALITY_REPORT.md` markdown file in the root

## 3. Verification and Testing

- [x] 3.1 Execute the script and verify that `QUALITY_REPORT.md` is generated without errors
- [x] 3.2 Ensure the generated report lists correct test coverage, flake8 findings, and docstring coverage details
