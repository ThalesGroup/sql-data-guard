# 🛡️ SQL Data Guard Quality Report

**Generated on:** 2026-06-24 14:15:49

## 📊 Quality Summary

| Pillar | Metric Checked | Current Score | Status |
| :--- | :--- | :---: | :---: |
| 🐍 **Code Quality** | Flake8 Style Violations | **2 Issues** | 🟡 Warnings |
| 🧪 **Test Quality** | Pytest Unit Coverage | **94.9%** | 🔴 Action Required |
| 📝 **Doc Quality** | Public API Docstring Coverage | **14.0%** | 🟡 Needs Docs |

---

## 🐍 Code Quality Details (Flake8)
- **Violations Found:** 2 issue(s):
  - `src/sql_data_guard/sql_data_guard.py:286:17: E741 ambiguous variable name 'l'`
  - `src/sql_data_guard/sql_data_guard.py:290:13: E741 ambiguous variable name 'l'`

## 🧪 Test Quality Details (Pytest-Cov)
- **Total Tests Executed:** 311 (Passed: 302, Failed: 9)
- **Branch Coverage:** 93.4%
- **Statement Coverage:** 94.9%

## 📝 Documentation Quality Details (Interrogate)
- **Total Docstring Coverage:** 14.0%

### Missing Docstrings (43):
- Module `__init__.py` missing docstring
- Module `restriction_validation.py` missing docstring
- `UnsupportedRestrictionError (L1)` in `restriction_validation.py` missing docstring
- Module `restriction_verification.py` missing docstring
- `verify_restrictions (L10)` in `restriction_verification.py` missing docstring
- `_format_value (L92)` in `restriction_verification.py` missing docstring
- `_get_restriction_values (L164)` in `restriction_verification.py` missing docstring
- Module `sql_data_guard.py` missing docstring
- `_verify_where_clause (L85)` in `sql_data_guard.py` missing docstring
- `_verify_static_expression (L98)` in `sql_data_guard.py` missing docstring
- `_has_static_expression (L113)` in `sql_data_guard.py` missing docstring
- `_verify_query_statement (L140)` in `sql_data_guard.py` missing docstring
- `_verify_from_tables (L155)` in `sql_data_guard.py` missing docstring
- `_verify_sub_queries (L167)` in `sql_data_guard.py` missing docstring
- `_verify_select_clause (L175)` in `sql_data_guard.py` missing docstring
- *...and 28 more.*
