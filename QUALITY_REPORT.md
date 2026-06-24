# 🛡️ SQL Data Guard Quality Report

**Generated on:** 2026-06-24 16:10:13

## 📊 Quality Summary

| Pillar | Metric Checked | Current Score | Status |
| :--- | :--- | :---: | :---: |
| 🐍 **Code Quality** | Ruff Style Violations | **0 Issues** | 🟢 Passed |
| 🧪 **Test Quality** | Pytest Unit Coverage | **93.9%** | 🟢 Passed |
| 📝 **Doc Quality** | Public API Docstring Coverage | **13.5%** | 🟡 Needs Docs |

---

## 🐍 Code Quality Details (Ruff)

- **Lint Errors:** 0 violations found. Code style is clean.

## 🧪 Test Quality Details (Pytest-Cov)

- **Total Tests Executed:** 315 (Passed: 315, Failed: 0)
- **Branch Coverage:** 92.1%
- **Statement Coverage:** 93.9%

## 📝 Documentation Quality Details (Interrogate)

- **Total Docstring Coverage:** 13.5%

### Missing Docstrings (45)

- Module `__init__.py` missing docstring
- Module `restriction_validation.py` missing docstring
- `UnsupportedRestrictionError (L4)` in `restriction_validation.py` missing docstring
- Module `restriction_verification.py` missing docstring
- `verify_restrictions (L11)` in `restriction_verification.py` missing docstring
- `_format_value (L94)` in `restriction_verification.py` missing docstring
- `_get_restriction_values (L165)` in `restriction_verification.py` missing docstring
- Module `sql_data_guard.py` missing docstring
- `_verify_where_clause (L105)` in `sql_data_guard.py` missing docstring
- `_verify_static_expression (L118)` in `sql_data_guard.py` missing docstring
- `_has_static_expression (L133)` in `sql_data_guard.py` missing docstring
- `_get_in_scope_table_names (L160)` in `sql_data_guard.py` missing docstring
- `_verify_query_statement (L176)` in `sql_data_guard.py` missing docstring
- `_verify_from_tables (L199)` in `sql_data_guard.py` missing docstring
- `_verify_sub_queries (L213)` in `sql_data_guard.py` missing docstring
- *...and 30 more.*
