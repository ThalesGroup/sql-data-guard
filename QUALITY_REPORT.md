# 🛡️ SQL Data Guard Quality Report

**Generated on:** 2026-06-24 17:31:27

## 📊 Quality Summary

| Pillar | Metric Checked | Current Score | Status |
| :--- | :--- | :---: | :---: |
| 🐍 **Code Quality** | Ruff Style Violations | **0 Issues** | 🟢 Passed |
| 🧪 **Test Quality** | Pytest Unit Coverage | **93.6%** | 🟢 Passed |
| 📝 **Doc Quality** | Public API Docstring Coverage | **13.7%** | 🟡 Needs Docs |

---

## 🐍 Code Quality Details (Ruff)
- **Lint Errors:** 0 violations found. Code style is clean.

## 🧪 Test Quality Details (Pytest-Cov)
- **Total Tests Executed:** 310 (Passed: 310, Failed: 0)
- **Branch Coverage:** 91.6%
- **Statement Coverage:** 93.6%

## 📝 Documentation Quality Details (Interrogate)
- **Total Docstring Coverage:** 13.7%

### Missing Docstrings (44):
- Module `__init__.py` missing docstring
- Module `restriction_validation.py` missing docstring
- `UnsupportedRestrictionError (L4)` in `restriction_validation.py` missing docstring
- Module `restriction_verification.py` missing docstring
- `verify_restrictions (L11)` in `restriction_verification.py` missing docstring
- `_format_value (L94)` in `restriction_verification.py` missing docstring
- `_get_restriction_values (L165)` in `restriction_verification.py` missing docstring
- Module `sql_data_guard.py` missing docstring
- `_verify_where_clause (L85)` in `sql_data_guard.py` missing docstring
- `_verify_static_expression (L98)` in `sql_data_guard.py` missing docstring
- `_has_static_expression (L113)` in `sql_data_guard.py` missing docstring
- `_verify_query_statement (L140)` in `sql_data_guard.py` missing docstring
- `_verify_from_tables (L155)` in `sql_data_guard.py` missing docstring
- `_verify_sub_queries (L167)` in `sql_data_guard.py` missing docstring
- `_verify_select_clause (L175)` in `sql_data_guard.py` missing docstring
- *...and 29 more.*
