import logging
import os
from logging.config import fileConfig
from typing import Optional, List, Tuple, NamedTuple

import sqlfluff
from dill.detect import errors
from sqlfluff.api import APIParsingError

fileConfig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.conf"))

class _VerificationResult:

    def __init__(self):
        super().__init__()
        self._can_fix = True
        self._errors = []
        self._fixed = None

    @property
    def can_fix(self) -> bool:
        return self._can_fix

    def add_error(self, error: str, can_fix: bool = True):
        self._errors.append(error)
        if not can_fix:
            self._can_fix = False

    @property
    def errors(self) -> List[str]:
        return self._errors

    @property
    def fixed(self) -> Optional[str]:
        return self._fixed

    @fixed.setter
    def fixed(self, value: Optional[str]):
        self._fixed = value



def verify_sql(sql: str, config: dict, dialect: str = "ansi") -> dict:
    try:
        sql_statement = None
        parse_tree = sqlfluff.parse(sql, dialect=dialect)
        for s in parse_tree["file"] if isinstance(parse_tree["file"], list) else [parse_tree["file"]]:
            if "statement" in s and "select_statement" in s["statement"]:
                sql_statement = s["statement"]["select_statement"]
                break
    except APIParsingError as e:
        logging.error(f"SQL: {sql}\nError parsing SQL: {e}")
        return { "allowed": False, "errors": [e.msg]}
    if sql_statement is None:
        return {"allowed": False, "errors": ["Could not find a select statement"]}
    result = _verify_tables_and_columns(sql_statement, config)
    return { "allowed": len(result.errors) == 0, "errors": result.errors, "fixed": result.fixed }


def _verify_where_clause(result: _VerificationResult, config: dict, statement: list):
    where_clause = _get_clause(statement, "where")
    if where_clause is None:
        where_str = " WHERE "
        for t in config["tables"]:
            for r in config["tables"][t].get("restrictions", []):
                where_str += f"{r['column']} = {r['value']}"
                result.add_error(f"Missing restriction for table: {t} column: {r['column']} value: {r['value']}")

        for idx, e in enumerate(statement):
            if isinstance(e, dict) and "from_clause" in e:
                statement.insert(idx + 1, {"where_clause": where_str})
                break
    else:
        for t in config["tables"]:
            for idx, r in enumerate(config["tables"][t].get("restrictions", [])):
                found = False
                for k, e in _get_elements(where_clause):
                    if k == "expression":
                        if _verify_restriction(r, e):
                            found = True
                            break
                if not found:
                    result.add_error(f"Missing restriction for table: {t} column: {r['column']} value: {r['value']}")
                    where_clause[f"{t}_{idx}"] = f" AND {r['column']} = {r['value']}"

def _get_reference_value(e: dict) -> str:
    if "naked_identifier" in e:
        return e["naked_identifier"]
    elif "quoted_identifier" in e:
        return e["quoted_identifier"].strip('"')
    else:
        raise ValueError(f"Unexpected column reference: {e}")

def _created_reference_value(value: str) -> dict:
    if "-" in value or " " in value:
        return {"quoted_identifier": f'"{value}"'}
    else:
        return {"naked_identifier": value}

def _verify_restriction(restriction: dict, exp: list) -> bool:
    columns_found = False
    op_found = False
    value_found = False
    for e in exp:
        if "column_reference" in e:
            if _get_reference_value(e["column_reference"]) == restriction["column"]:
                columns_found = True
        if "comparison_operator" in e:
            if e["comparison_operator"]["raw_comparison_operator"] == "=":
                op_found = True
        if "numeric_literal" in e:
            if int(e["numeric_literal"]) == restriction["value"]:
                value_found = True
    return columns_found and op_found and value_found


def _verify_tables_and_columns(select_statement, config: dict) -> _VerificationResult:
    result = _VerificationResult()
    if isinstance(select_statement, dict):
        select_statement = [{k: select_statement[k]} for k in select_statement]
    from_clause = _get_clause(select_statement, "from")
    tables = _get_from_clause_tables(from_clause)
    for t in tables:
        if t not in config["tables"]:
            result.add_error(f"Table {t} is not allowed", False)
    if not result.can_fix:
        return result
    select_clause = _get_clause(select_statement, "select")
    updated_select_clause_elements = _verify_select_clause(result, config, select_clause)
    select_clause.clear()
    if isinstance(select_clause, list):
        select_clause.extend(updated_select_clause_elements)
    elif isinstance(select_clause, dict):
        select_clause["select_clause"] = updated_select_clause_elements
    _verify_where_clause(result, config, select_statement)
    if result.can_fix and len(result.errors) > 0:
        result.fixed = _convert_to_text(select_statement)
    return result


def _verify_select_clause(result: _VerificationResult, config: dict, select_clause: list) -> list:
    updated_select_clause_elements = []
    for k, e in _get_elements(select_clause):
        add_to_select = True
        if k == "select_clause_element":
            if "expression" in e:
                pass
            if e.get("wildcard_expression", {}).get("wildcard_identifier", {}).get("star", "") == "*":
                result.add_error("SELECT * is not allowed")
                if result.can_fix:
                    for t in config["tables"]:
                        for c in config["tables"][t]["columns"]:
                            updated_select_clause_elements.append({
                                "select_clause_element": {
                                    "column_reference": _created_reference_value(c)
                                }
                            })
                            updated_select_clause_elements.append({"comma": ","})
                            updated_select_clause_elements.append({"whitespace": " "})
                    updated_select_clause_elements.pop()
                    updated_select_clause_elements.pop()
                    add_to_select = False
            elif "column_reference" in e:
                col_name = _get_reference_value(e["column_reference"])
                column_found = False
                for t in config["tables"]:
                    if col_name in config["tables"][t]["columns"]:
                        column_found = True
                        break
                    if column_found:
                        break
                if not column_found:
                    result.add_error(f"Column {col_name} is not allowed. Column removed from SELECT clause")
                    remove_el = 0
                    for i in range(len(updated_select_clause_elements) - 1, 0, -1):
                        if "select_clause_element" not in updated_select_clause_elements[i]:
                            remove_el += 1
                        else:
                            break
                    updated_select_clause_elements = updated_select_clause_elements[:-remove_el]
                    add_to_select = False
        if add_to_select:
            updated_select_clause_elements.append({k: e})

    found = False
    for e in updated_select_clause_elements:
        if "select_clause_element" in e:
            found = True
            break
    if not found:
        result.add_error("No columns left in SELECT clause", False)

    return updated_select_clause_elements


def _get_from_clause_tables(from_clause: dict) -> List[str]:
    result = []
    for e in _get_elements_by_name(from_clause, "from_expression"):
        table_ref = e["from_expression_element"]["table_expression"]["table_reference"]
        result.append(_get_reference_value(table_ref))
    return result


def _get_elements(clause) ->  List[Tuple[str, object]]:
    if isinstance(clause, dict):
        return [(k, v) for k, v in clause.items()]
    elif isinstance(clause, list):
        result = []
        for e in clause:
            result.extend([(k, v) for k, v in e.items()])
        return result
    else:
        raise ValueError(f"Unexpected type {type(clause)}")


def _get_elements_by_name(clause: dict, name: str) -> list:
    result = []
    for child in clause:
        if isinstance(child, dict) and name in child:
            el = child[name]
        elif child == name:
            el = clause[child]
        else:
            continue
        result.append(el)
    return result


def _get_clause(clause: list, name: str) -> Optional[dict]:
    for c in clause:
        if f"{name}_clause" in c:
            return c[f"{name}_clause"]
    return None


def _convert_to_text(item) -> str:
    result = ""
    if isinstance(item, dict):
        for k in item:
            result += _convert_to_text(item[k])
    elif isinstance(item, list):
        for e in item:
            result += _convert_to_text(e)
    else:
        result += item
    return result
