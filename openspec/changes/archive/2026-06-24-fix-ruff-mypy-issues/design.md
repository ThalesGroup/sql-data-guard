## Context

The SQL Data Guard codebase currently suffers from numerous linting errors and type-checking warnings across Python source files, tests, and utility scripts:
- **Mypy**: 270 type-safety and annotation-related errors.
- **Ruff**: 39 linter violations (concerning pathlib usages, exception chaining, line length, and syntax constructs).

This technical design document outlines the strategy for resolving these quality issues systematically, preserving complete behavioral and test suite compatibility.

## Goals / Non-Goals

**Goals:**
- Eliminate all Mypy type-checking errors under the `src/`, `tests/`, and `scripts/` directories such that `uv run mypy .` runs perfectly clean.
- Resolve all Ruff linting violations such that `uv run ruff check .` runs with zero warnings or errors.
- Ensure all existing unit/integration tests continue to run and pass.

**Non-Goals:**
- Adding new library dependencies.
- Refactoring the core query checking/rewriting logic of `sql-data-guard`.
- Modifying test behavior or functionality.

## Decisions

### 1. Test Function and Fixture Annotations
- **Decision**: Annotate all test functions (which implicitly return nothing) with `-> None`. Annotate all pytest fixtures with their specific return types or generic `Generator` types with arguments (e.g., `Generator[dict[str, Any], None, None]`).
- **Rationale**: pytest does not require typing signatures, but modern strict type checkers (like our Mypy configuration) enforce annotations for any function defined, including tests.

### 2. Standardize Generic Types
- **Decision**: Upgrade raw `dict` and `list` type annotations to `dict[str, Any]` (or similar specialized key/value pairs) and `list[Any]`.
- **Rationale**: Mypy warns against using raw generics without type arguments. We will use standard typing syntax (like `dict[str, Any]`) to comply with Python 3.9+ type styling.

### 3. Handle Union Attribute Access & Implicit Optionals
- **Decision**:
  - For attributes accessed on union types that can be `None` (e.g., `Expr | None`), introduce explicit assertions or `if obj is not None:` guard checks before calling methods like `obj.sql()`.
  - Replace implicit `Optional` parameters (e.g. `dialect: str = None`) with explicit Union typing (e.g. `dialect: str | None = None` or `Optional[str] = None`).
- **Rationale**: Prevents potential `AttributeError` run-time failures and satisfies Mypy's strict union-attr check.

### 4. Transition os.path and open to Pathlib
- **Decision**: Replace `os.path` / `open` helper patterns with `pathlib.Path` APIs in source code, scripts, and tests (e.g. `Path(path_str).exists()`, `Path(path_str).open()`).
- **Rationale**: Simplifies path manipulation and aligns with Ruff's path-related rules (`PTH100`, `PTH110`, `PTH118`, `PTH120`, `PTH123`), leading to cleaner and more idiomatic Python code.

### 5. Standard Exception Chaining and Ternary Replacements
- **Decision**:
  - Always chain exceptions in catch-raise scenarios using `raise ... from err`.
  - Use ternary operators for single-expression assignments where recommended by Ruff.
- **Rationale**: Preserves debugging traceback information (Mypy/Ruff `B904`) and simplifies code syntax (Ruff `SIM108`).

## Risks / Trade-offs

- **[Risk]**: Introducing type-assertion guards might change runtime edge-cases if `None` values are handled differently.
  - *Mitigation*: Ensure logic behaves identically by strictly guarding against `None` without changing the flow, and continuously running the test suite (`pytest`) to guarantee complete compatibility.
- **[Risk]**: Extensive test annotation refactoring might introduce syntax errors.
  - *Mitigation*: Use automated refactoring / linting tooling cautiously and manually verify syntax consistency, running tests after editing each test file.
