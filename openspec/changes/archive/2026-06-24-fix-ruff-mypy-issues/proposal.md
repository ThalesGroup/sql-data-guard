## Why

The SQL Data Guard codebase currently contains 39 Ruff linting warnings and 270 Mypy type-checking errors across both source and test files. Resolving these issues aligns the codebase with modern Python type-safety standards, eliminates legacy paths in favor of modern `pathlib.Path` structures, and ensures that continuous integration checks can pass cleanly.

## What Changes

- **Mypy Type Annotations**:
  - Annotate all test functions and test fixtures with proper type signatures, including `-> None` return annotations.
  - Specify generic type arguments for generic structures (e.g. `dict` becomes `dict[str, Any]` or `dict[Any, Any]`, and `list` becomes `list[Any]`).
  - Resolve implicit `Optional` annotations (e.g. `Optional[None]` or missing `| None`).
  - Fix any attribute access errors (such as missing `http.client` imports and union types with `None`).
- **Ruff Linting Violations**:
  - Replace legacy `os.path` and manual file-handling calls with modern `pathlib.Path` APIs (e.g. `Path.open()`, `Path.exists()`, path `/` operator).
  - Break up overly long lines exceeding 120 characters where possible.
  - Simplify unnecessarily verbose constructs (e.g., replace verbose `if-else` blocks with ternary assignments, and remove redundant return assignments).
  - Explicitly raise exceptions from original errors in catch blocks (`raise ... from err`).

## Capabilities

### New Capabilities
- None

### Modified Capabilities
- None

## Impact

- All unit/integration tests and source files in `src/` and `tests/` are affected.
- No functional or external behavioral changes are introduced.
- Continuous integration checks (`uv run ruff check .` and `uv run mypy .`) will run and pass successfully.
