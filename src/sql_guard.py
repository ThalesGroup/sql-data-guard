import logging
import os
from logging.config import fileConfig
from typing import Optional, List, Tuple

import sqlfluff
from sqlfluff.api import APIParsingError

fileConfig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.conf"))


def verify_sql(sql: str, config: dict, fix: bool = False, dialect: str = "ansi") -> dict:
    try:
        sql_statement = None
        parse_tree = sqlfluff.parse(sql, dialect=dialect)
        for s in parse_tree["file"] if isinstance(parse_tree["file"], list) else [parse_tree["file"]]:
            if "statement" in s and "select_statement" in s["statement"]:
                sql_statement = s["statement"]
                break
    except APIParsingError as e:
        logging.error(f"SQL: {sql}\nError parsing SQL: {e}")
        return { "allowed": False, "errors": [e.msg]}
    if sql_statement is None:
        return {"allowed": False, "errors": ["Could not find a select statement"]}
    errors, fixed = _verify_tables_and_columns(sql_statement, config)
    return { "allowed": len(errors) == 0, "errors": errors, "fixed": fixed  }


def _verify_where_clause(statement: dict, config: dict) -> List[str]:
    result = []
    where_clause = _get_clause(statement, "where")
    if where_clause is None:
        where_str = " WHERE "
        for t in config["tables"]:
            for r in config["tables"][t].get("restrictions", []):
                where_str += f"{r['column']} = {r['value']}"
                result.append(f"Missing restriction for table: {t} column: {r['column']} value: {r['value']}")
        statement["where_clause"] = where_str
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
                    result.append(f"Missing restriction for table: {t} column: {r['column']} value: {r['value']}")
                    where_clause[f"{t}_{idx}"] = f" AND {r['column']} = {r['value']}"
    return result

def _verify_restriction(restriction: dict, exp: list) -> bool:
    columns_found = False
    op_found = False
    value_found = False
    for e in exp:
        if "column_reference" in e:
            if e["column_reference"]["naked_identifier"] == restriction["column"]:
                columns_found = True
        if "comparison_operator" in e:
            if e["comparison_operator"]["raw_comparison_operator"] == "=":
                op_found = True
        if "numeric_literal" in e:
            if int(e["numeric_literal"]) == restriction["value"]:
                value_found = True
    return columns_found and op_found and value_found


def _verify_tables_and_columns(parse_tree: dict, config: dict) -> (List[str], Optional[str]):
    errors = []
    can_fix = True
    from_clause = _get_clause(parse_tree, "from")
    tables = _get_from_clause_tables(from_clause)
    for t in tables:
        if t not in config["tables"]:
            errors.append(f"Table {t} is not allowed")
            can_fix = False
    if not can_fix:
        return errors, None
    select_clause = _get_clause(parse_tree, "select")
    updated_select_clause_elements = []
    for k, e in _get_elements(select_clause):
        add_to_select = True
        if k == "select_clause_element":
            if e.get("wildcard_expression", {}).get("wildcard_identifier", {}).get("star", "") == "*":
                errors.append("SELECT * is not allowed")
                if can_fix:
                    for t in tables:
                        for c in config["tables"][t]["columns"]:
                            updated_select_clause_elements.append({
                                "select_clause_element": {
                                    "column_reference": {
                                        "naked_identifier": c
                                    }
                                }
                            })
                            updated_select_clause_elements.append({"comma": ","})
                            updated_select_clause_elements.append({"whitespace": " "})
                    updated_select_clause_elements.pop()
                    updated_select_clause_elements.pop()
                    add_to_select = False
            elif "column_reference" in e:
                col_name = e["column_reference"]["naked_identifier"]
                column_found = False
                for t in config["tables"]:
                    if col_name in config["tables"][t]["columns"]:
                        column_found = True
                        break
                    if column_found:
                        break
                if not column_found:
                    errors.append(f"Column {col_name} is not allowed. Column removed from SELECT clause")
                    remove_el = 0
                    for i in range(len(updated_select_clause_elements) - 1, 0, -1):
                        if "select_clause_element" not in updated_select_clause_elements[i]:
                            remove_el += 1
                        else:
                            break
                    updated_select_clause_elements = updated_select_clause_elements[:-remove_el]
                    el_found = False
                    for el in updated_select_clause_elements:
                        if "select_clause_element" in el:
                            el_found = True
                    if not el_found:
                        can_fix = False
                    add_to_select = False
        if add_to_select:
            updated_select_clause_elements.append({k: e})
    select_clause.clear()
    if isinstance(select_clause, list):
        select_clause.extend(updated_select_clause_elements)
    elif isinstance(select_clause, dict):
        select_clause["select_clause"] = updated_select_clause_elements
    where_clause_errors = _verify_where_clause(parse_tree, config)
    errors.extend(where_clause_errors)
    return errors, _convert_to_text(parse_tree) if can_fix else None

def _get_from_clause_tables(from_clause: dict) -> List[str]:
    result = []
    for e in _get_elements_by_name(from_clause, "from_expression"):
        result.append(e["from_expression_element"]["table_expression"]["table_reference"]["naked_identifier"])
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


def _get_clause(d: dict, name: str) -> dict:
    clause = d["select_statement"]
    if isinstance(clause, list):
        for c in clause:
            if f"{name}_clause" in c:
                clause = c[f"{name}_clause"]
                break
    elif f"{name}_clause" in clause:
        clause = clause[f"{name}_clause"]
    else:
        clause = None
    return clause


def _convert_to_text(parse_tree: dict) -> str:
    result = ""
    for v in parse_tree.values():
        if isinstance(v, dict):
            result += _convert_to_text(v)
        elif isinstance(v, list):
            for e in v:
                result += _convert_to_text(e)
        else:
            result += v
    return result
