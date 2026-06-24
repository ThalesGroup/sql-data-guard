# 05 · Coding Guidelines & Contribution Standards

> Conventions, security mindset, testing expectations, and a PR checklist tailored to `sql-data-guard`. These build on the project's existing `.clinerules` (PEP 8 + code-quality + security) and `CONTRIBUTING.md`.

See also: [01-CODE_EXPLANATION](01-CODE_EXPLANATION.md) · [04-ARCHITECTURE](04-ARCHITECTURE.md)

---

## 1. Golden rules (project-specific)

1. **Read before you edit.** Trace execution end-to-end; security logic is subtle.
2. **Prefer AST-based validation over regex.** Regex is a last resort (only the pre-parse comment scan uses it, by necessity).
3. **Never break backward compatibility silently.** New policies must be **opt-in** and documented.
4. **Every change ships with:** rationale · security impact · tests · doc update.
5. **Fail closed.** When in doubt, block (`can_fix=False`) rather than allow.
6. **The fix must be safe.** Any SQL the guard *generates* must use escaped literals (`_format_value`).

---

## 2. Style & formatting

| Topic | Rule |
|-------|------|
| Formatter | `black` (project default); 4-space indent, ≤ 99-char lines (PEP 8). |
| Imports | stdlib → third-party → local, alphabetical within groups. |
| Naming | `snake_case` functions/vars, `PascalCase` classes, `UPPER_SNAKE_CASE` constants, `_prefix` for private. |
| Type hints | Required on all function signatures (`def verify_sql(sql: str, config: dict, dialect: str = None) -> dict`). |
| Docstrings | Required on public functions/modules; explain **why**, not just what. |
| f-strings | Preferred for interpolation. |
| Logging | Use `logging`, never `print()` in library code. |

---

## 3. Complexity & size limits (from code-quality rules)

| Metric | Limit |
|--------|-------|
| Cyclomatic / cognitive complexity | < 15 |
| Nesting depth | ≤ 3 |
| Function length | < 50 lines |
| Parameters | ≤ 4 |
| Duplication | none ≥ 3 lines |

> `verify_sql` and `_verify_col` currently push these limits — see [04-ARCHITECTURE §6](04-ARCHITECTURE.md#6-technical-debt--risk-register) (H3, M1). New code must stay within limits; touch those hotspots only with accompanying tests.

---

## 4. Security mindset (review like an attacker)

When adding or reviewing rule code, actively look for:

- **Allow-list bypasses:** can a column/table reach the output via a sub-query, CTE, lateral, `UNNEST`, or set-operation arm without being checked?
- **Restriction bypasses:** can a `WHERE` predicate *appear* to satisfy a restriction while being neutralised by `NOT`, `OR`, parentheses, or type confusion?
- **Comment / encoding tricks:** does the payload survive a `--`, `/* */`, `#`, or unusual whitespace?
- **Parser confusion:** does a dialect parse the statement differently than expected (test across dialects)?
- **Fix-as-vector:** does an injected restriction or mask use raw string concatenation? It must not.

Map every new control to an attack class (see [04-ARCHITECTURE §3.2](04-ARCHITECTURE.md#32-controls-mapped-to-attack-classes)).

### Security rules quick reference (applies here)

- Parameterised/escaped values only — never concatenate untrusted strings into SQL.
- Catch **specific** exceptions; never an empty `except:`.
- No secrets in code or logs; the optional API key comes from `SQL_GUARD_API_KEY` env var.
- Validate config **before** parsing (fail fast).

---

## 5. Testing standards

The suite is the project's safety net. Conventions observed in `test/`:

| Convention | Detail |
|------------|--------|
| Framework | `pytest`; classes group related cases (`TestStackedQueries`, `TestColumnMaskRewrite`, …). |
| Helper | `conftest.py::verify_sql_test` asserts `errors`, `risk`, and `fixed`, and (optionally) executes against a real DB connection. |
| Multi-dialect | Parametrise across `sqlite` / `mysql` / `postgres` / `trino` / `duckdb` where behaviour can differ. |
| Real execution | Many tests run the `fixed` SQL against SQLite/DuckDB to prove it's valid and returns the expected rows. |
| Risk assertions | `errors present ⇒ risk > 0`; `no errors ⇒ risk == 0`. |
| Determinism | `errors` is a list — order-insensitive comparisons via `set(...)`. |

### What every new feature/fix needs

1. **Positive test** — the legitimate case still passes (non-breaking).
2. **Negative test** — the attack/violation is caught with the right message and risk.
3. **Fix test** — if fixable, assert the exact `fixed` string *and* (ideally) execute it.
4. **Validation test** — bad config produces a graceful error, not a crash.
5. **False-positive guard** — a benign look-alike (e.g. a column named `sys`) is *not* flagged.

```bash
# Run the suite (from repo root)
pip install -r test/test.requirements.txt
PYTHONPATH=src pytest test -q
```

---

## 6. Adding a new policy — recipe

To add, say, a new restriction operator or mask policy:

1. **Validate** the config shape in `restriction_validation.py` / `column_masking.py` (fail fast).
2. **Enforce/rewrite** in the relevant module, reporting via `context.add_error(msg, can_fix, risk)`.
3. **Choose a risk weight** consistent with the [taxonomy](03-LLD.md#6-error-taxonomy--risk-weights); use a named constant.
4. **Keep it opt-in** — default behaviour unchanged.
5. **Document** in a `docs/FEATURE_Fx_*.md` and update [00-INDEX](00-INDEX.md).
6. **Test** per §5.

---

## 7. Deliverable format for findings (from `.clinerules`)

For every security/quality task, produce:

1. **Findings** — what's wrong, with evidence (run the code, don't assume).
2. **Root Cause** — why it happens.
3. **Recommended Fix** — implementable, with code.
4. **Impact** — security + backward-compatibility.
5. **Test Cases** — positive, negative, fix, validation.
6. **Documentation Updates** — which docs change.

Never stop at identifying an issue — always propose an implementable improvement.

---

## 8. Pull-request checklist

- [ ] Change is **minimal and focused**; no unrelated refactoring.
- [ ] New behaviour is **opt-in**; existing tests stay green.
- [ ] **Rationale + security impact** described in the PR.
- [ ] **Tests** added (positive / negative / fix / validation / false-positive).
- [ ] Multi-dialect tested where relevant.
- [ ] Any guard-generated SQL uses **escaped literals**.
- [ ] **Specific** exceptions caught; no empty `except`.
- [ ] Complexity/size limits respected (§3).
- [ ] Public APIs have **type hints + docstrings**.
- [ ] Risk weight chosen via **named constant**, consistent with taxonomy.
- [ ] Relevant **docs updated** (feature doc + this analysis set + README/manual if user-facing).
- [ ] `black` clean; imports sorted; no `print()` / unused imports.
