# 🛡️ SQL Data Guard Quality Report

**Generated on:** 2026-06-24 15:51:10

## 📊 Quality Summary

| Pillar | Metric Checked | Current Score | Status |
| :--- | :--- | :---: | :---: |
| 🐍 **Code Quality** | Ruff Style Violations | **148 Issues** | 🟡 Warnings |
| 🧪 **Test Quality** | Pytest Unit Coverage | **93.8%** | 🟢 Passed |
| 📝 **Doc Quality** | Public API Docstring Coverage | **13.7%** | 🟡 Needs Docs |

---

## 🐍 Code Quality Details (Ruff)
- **Violations Found:** 148 issue(s):
  - `E501 Line too long (95 > 88)`
  - `--> src/sql_data_guard/mcpwrapper/mcp_wrapper.py:104:89`
  - `|`
  - `102 |         if not result["allowed"]:`
  - `103 |             sys.stderr.write(`
  - `104 |                 f"ID: {json_line['id']} Blocked SQL: {sql}\nErrors: {list(result['errors'])}\n"`
  - `|                                                                                         ^^^^^^^`
  - `105 |             )`
  - `106 |             if result["fixed"]:`
  - `|`
  - `UP035 `typing.Dict` is deprecated, use `dict` instead`
  - `--> src/sql_data_guard/rest/sql_data_guard_rest.py:4:1`
  - `|`
  - `2 | import os`
  - `3 | from logging.config import fileConfig`
  - `4 | from typing import Any, Dict, Optional`
  - `| ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^`
  - `5 |`
  - `6 | from fastapi import FastAPI`
  - `|`
  - `UP006 [*] Use `dict` instead of `Dict` for type annotation`
  - `--> src/sql_data_guard/rest/sql_data_guard_rest.py:20:13`
  - `|`
  - `18 | class VerifySQLRequest(BaseModel):`
  - `19 |     sql: str = Field(..., description="The SQL query to verify")`
  - `20 |     config: Dict[str, Any] = Field(`
  - `|             ^^^^`
  - `21 |         ...,`
  - `22 |         description="The verification configuration specifying allowed tables, columns, and restrictions",`
  - `|`
  - `help: Replace with `dict``
  - `E501 Line too long (106 > 88)`
  - `--> src/sql_data_guard/rest/sql_data_guard_rest.py:22:89`
  - `|`
  - `20 |     config: Dict[str, Any] = Field(`
  - `21 |         ...,`
  - `22 |         description="The verification configuration specifying allowed tables, columns, and restrictions",`
  - `|                                                                                         ^^^^^^^^^^^^^^^^^^`
  - `23 |     )`
  - `24 |     dialect: Optional[str] = Field(`
  - `|`
  - `UP045 [*] Use `X | None` for type annotations`
  - `--> src/sql_data_guard/rest/sql_data_guard_rest.py:24:14`
  - `|`
  - `22 |         description="The verification configuration specifying allowed tables, columns, and restrictions",`
  - `23 |     )`
  - `24 |     dialect: Optional[str] = Field(`
  - `|              ^^^^^^^^^^^^^`
  - `25 |         None, description="Optional SQL dialect for parsing"`
  - `26 |     )`
  - `|`
  - `help: Convert to `X | None``
  - `E501 Line too long (97 > 88)`
  - `--> src/sql_data_guard/restriction_validation.py:7:89`
  - `|`
  - `5 | def validate_restrictions(config: dict):`
  - `6 |     """`
  - `7 |     Validates the restrictions in the configuration to ensure only supported operations are used.`
  - `|                                                                                         ^^^^^^^^^`
  - `8 |`
  - `9 |     Args:`
  - `|`
  - `E501 Line too long (124 > 88)`
  - `--> src/sql_data_guard/restriction_validation.py:56:89`
  - `|`
  - `54 |                 ):`
  - `55 |                     raise ValueError(`
  - `56 |                         f"Invalid 'BETWEEN' format. Expected list of two numeric values where min < max. Received: {values}"`
  - `|                                                                                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^`
  - `57 |                     )`
  - `|`
  - `E501 Line too long (103 > 88)`
  - `--> src/sql_data_guard/restriction_validation.py:67:89`
  - `|`
  - `65 |                 ):`
  - `66 |                     raise ValueError(`
  - `67 |                         f"Invalid 'IN' format. Expected list of two numeric values. Received: {values}"`
  - `|                                                                                         ^^^^^^^^^^^^^^^`
  - `68 |                     )`
  - `|`
  - `E501 Line too long (154 > 88)`
  - `--> src/sql_data_guard/restriction_validation.py:75:89`
  - `|`
  - `73 | …`
  - `74 | …`
  - `75 | …e for column '{restriction['column']}' in table '{table['table_name']}'. Expected a numeric value."`
  - `|                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^`
  - `76 | …`
  - `|`
  - `E501 Line too long (151 > 88)`
  - `--> src/sql_data_guard/restriction_verification.py:37:89`
  - `|`
  - `36 | …`
  - `37 | …le: {c_t['table_name']} column: {t_prefix}{r['column']} value: {r.get('values', r.get('value'))}",`
  - `|                                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^`
  - `38 | …`
  - `39 | …`
  - `|`
  - `E501 Line too long (108 > 88)`
  - `--> src/sql_data_guard/restriction_verification.py:72:89`
  - `|`
  - `70 |     if restriction.get("operation") == "BETWEEN":`
  - `71 |         operator = "BETWEEN"`
  - `72 |         operand = f"{_format_value(restriction['values'][0])} AND {_format_value(restriction['values'][1])}"`
  - `|                                                                                         ^^^^^^^^^^^^^^^^^^^^`
  - `73 |     elif restriction.get("operation") == "IN":`
  - `74 |         operator = "IN"`
  - `|`
  - `E501 Line too long (99 > 88)`
  - `--> src/sql_data_guard/restriction_verification.py:105:89`
  - `|`
  - `104 |     Args:`
  - `105 |         restriction (dict): The restriction to verify, containing 'column' and 'value' or 'values'.`
  - `|                                                                                         ^^^^^^^^^^^`
  - `106 |         from_table (Table): The table reference to check the restriction against.`
  - `107 |         exp (Expression): The SQL expression to check against the restriction.`
  - `|`
  - `E501 Line too long (94 > 88)`
  - `--> src/sql_data_guard/sql_data_guard.py:21:89`
  - `|`
  - `19 |     Args:`
  - `20 |         sql (str): The SQL query to verify.`
  - `21 |         config (dict): The configuration specifying allowed tables, columns, and restrictions.`
  - `|                                                                                         ^^^^^^`
  - `22 |         dialect (str, optional): The SQL dialect to use for parsing`
  - `|`
  - `E501 Line too long (90 > 88)`
  - `--> src/sql_data_guard/sql_data_guard.py:36:89`
  - `|`
  - `34 |             "allowed": False,`
  - `35 |             "errors": [`
  - `36 |                 "Invalid configuration provided. The configuration must include 'tables'."`
  - `|                                                                                         ^^`
  - `37 |             ],`
  - `38 |             "fixed": None,`
  - `|`
  - `E501 Line too long (111 > 88)`
  - `--> src/sql_data_guard/verification_context.py:12:89`
  - `|`
  - `10 |         _fixed (Optional[str]): The fixed query if modifications were made.`
  - `11 |         _config (dict): The configuration used for verification.`
  - `12 |         _dynamic_tables (Set[str]): Set of dynamic tables found in the query, like sub select and WITH clauses.`
  - `|                                                                                         ^^^^^^^^^^^^^^^^^^^^^^^`
  - `13 |         _dialect (str): The SQL dialect to use for parsing.`
  - `14 |     """`
  - `|`
  - `Found 15 errors.`
  - `[*] 2 fixable with the `--fix` option.`

## 🧪 Test Quality Details (Pytest-Cov)
- **Total Tests Executed:** 302 (Passed: 302, Failed: 0)
- **Branch Coverage:** 92.4%
- **Statement Coverage:** 93.8%

## 📝 Documentation Quality Details (Interrogate)
- **Total Docstring Coverage:** 13.7%

### Missing Docstrings (44):
- Module `__init__.py` missing docstring
- Module `restriction_validation.py` missing docstring
- `UnsupportedRestrictionError (L1)` in `restriction_validation.py` missing docstring
- Module `restriction_verification.py` missing docstring
- `verify_restrictions (L9)` in `restriction_verification.py` missing docstring
- `_format_value (L91)` in `restriction_verification.py` missing docstring
- `_get_restriction_values (L163)` in `restriction_verification.py` missing docstring
- Module `sql_data_guard.py` missing docstring
- `_verify_where_clause (L84)` in `sql_data_guard.py` missing docstring
- `_verify_static_expression (L97)` in `sql_data_guard.py` missing docstring
- `_has_static_expression (L112)` in `sql_data_guard.py` missing docstring
- `_verify_query_statement (L139)` in `sql_data_guard.py` missing docstring
- `_verify_from_tables (L154)` in `sql_data_guard.py` missing docstring
- `_verify_sub_queries (L166)` in `sql_data_guard.py` missing docstring
- `_verify_select_clause (L174)` in `sql_data_guard.py` missing docstring
- *...and 29 more.*
