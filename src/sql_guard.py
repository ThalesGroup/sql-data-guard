import logging
import os
from logging.config import fileConfig
from typing import Optional, List, Tuple, Generator, NamedTuple

import sqlfluff
from sqlfluff.api import APIParsingError

fileConfig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.conf"))

class _TableRef(NamedTuple):
    table_name: str
    db_name: str = None

class _ColumnRef(NamedTuple):
    column_name: str
    table_name: str = None
    db_name: str = None



class _VerificationContext:

    def __init__(self, config: dict):
        super().__init__()
        self._can_fix = True
        self._errors = []
        self._fixed = None
        self._from_tables = []
        self._config = config

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

    def add_from_table(self, t: _TableRef):
        self._from_tables.append(t)

    @property
    def from_tables(self) -> List[_TableRef]:
        return self._from_tables

    @property
    def config(self) -> dict:
        return self._config


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


def _verify_where_clause(result: _VerificationContext, statement: list):
    where_clause = _get_clause(statement, "where")
    if where_clause is None:
        where_str = " WHERE "
        for t in result.config["tables"]:
            for r in t.get("restrictions", []):
                where_str += f"{r['column']} = {r['value']}"
                result.add_error(f"Missing restriction for table: {t['table_name']} column: {r['column']} value: {r['value']}")

        for idx, e in enumerate(statement):
            if isinstance(e, dict) and "from_clause" in e:
                statement.insert(idx + 1, {"where_clause": where_str})
                break
    else:
        for t in result.config["tables"]:
            for idx, r in enumerate(t.get("restrictions", [])):
                found = False
                for k, e in _get_elements(where_clause):
                    if k == "expression":
                        for sub_exp in _split_expression(e, "AND"):
                            if _verify_restriction(r, sub_exp):
                                found = True
                                break
                if not found:
                    result.add_error(f"Missing restriction for table: {t['table_name']} column: {r['column']} value: {r['value']}")
                    where_clause[f"{t}_{idx}"] = f" AND {r['column']} = {r['value']}"

def _split_expression(e: list, binary_operator: str) -> List[list]:
    result = []
    current = []
    for el in e:
        if isinstance(el, dict) and el.get("binary_operator", "") == binary_operator:
            result.append(current)
            current = []
        else:
            current.append(el)
    if len(current) > 0:
        result.append(current)
    return result

def _get_ref_table(e) -> _TableRef:
    if isinstance(e, dict):
        return _TableRef(table_name=_get_ref_value(e))
    elif isinstance(e, list):
        vals = _get_ref_values(e)
        if len(vals) == 2:
            return _TableRef(db_name=vals[0], table_name=vals[1])
        elif len(vals) == 3:
            return _TableRef(db_name=vals[0], table_name=vals[1], alias=vals[2])


def _get_ref_value(e: dict) -> str:
    if "naked_identifier" in e:
        return e["naked_identifier"]
    elif "quoted_identifier" in e:
        return e["quoted_identifier"].strip('"')
    else:
        raise ValueError(f"Unexpected column reference: {e}")

def _get_ref_values(l: list) -> List[str]:
    result = []
    for e in l:
        if "naked_identifier" in e or "quoted_identifier" in e:
            result.append(_get_ref_value(e))
    return result


def _get_ref_col(e) -> _ColumnRef:
    if isinstance(e, dict):
        return _ColumnRef(column_name=_get_ref_value(e))
    elif isinstance(e, list):
        return _get_ref_col(e[-1])

def _created_reference_value(value: str) -> dict:
    return {"quoted_identifier": _quote_identifier(value)}

def _quote_identifier(value: str) -> str:
    if "-" in value or " " in value:
        return f'"{value}"'
    else:
        return value

def _verify_restriction(restriction: dict, exp: list) -> bool:
    sub_exps = _split_expression(exp, "OR")
    if len(sub_exps) > 1:
        return False
    for sub_exp in sub_exps:
        columns_found = False
        op_found = False
        value_found = False
        for e in sub_exp:
            if isinstance(e, dict):
                if "column_reference" in e:
                    if _get_ref_col(e["column_reference"]).column_name == restriction["column"]:
                        columns_found = True
                if "comparison_operator" in e:
                    if e["comparison_operator"]["raw_comparison_operator"] == "=":
                        op_found = True
                if "numeric_literal" in e:
                    if int(e["numeric_literal"]) == restriction["value"]:
                        value_found = True
                if columns_found and op_found and value_found:
                    return True


def _verify_tables_and_columns(select_statement, config: dict) -> _VerificationContext:
    result = _VerificationContext(config)
    if isinstance(select_statement, dict):
        select_statement = [{k: select_statement[k]} for k in select_statement]
    from_clause = _get_clause(select_statement, "from")
    _update_from_clause_tables(from_clause, result)
    for t in result.from_tables:
        found = False
        for config_t in result.config["tables"]:
            if t.table_name == config_t["table_name"]:
                found = True
        if not found:
            result.add_error(f"Table {t.table_name} is not allowed", False)
    if not result.can_fix:
        return result
    select_clause = _get_clause(select_statement, "select")
    _verify_select_clause(result, select_clause)
    _verify_where_clause(result, select_statement)
    if result.can_fix and len(result.errors) > 0:
        result.fixed = _convert_to_text(select_statement)
    return result


def _verify_select_clause(result: _VerificationContext, select_clause: list):
    updated_select_clause = []
    has_legal_elements = False
    is_first_element = True
    for e_name, e in _get_elements(select_clause):
        if e_name == "select_clause_element":
            if _verify_select_clause_element(result, e):
                if not is_first_element and not has_legal_elements:
                    while len(updated_select_clause) > 0 and isinstance(updated_select_clause[-1], str):
                        updated_select_clause.pop()
                updated_select_clause.append(e)
                has_legal_elements = True
            else:
                if has_legal_elements:
                    while len(updated_select_clause) > 0 and isinstance(updated_select_clause[-1], str):
                        updated_select_clause.pop()
                updated_select_clause.append({})
            is_first_element = False
        else:
            updated_select_clause.append(e)
    if not has_legal_elements:
        result.add_error("No legal elements in SELECT clause", False)
    else:
        select_clause.clear()
        if isinstance(select_clause, dict):
            select_clause["select_clause_element"] = updated_select_clause
        else:
            select_clause.extend(updated_select_clause)

def _verify_select_clause_element(result: _VerificationContext, e: list):
    for el_name, el in _get_elements(e):
        if el_name == "wildcard_expression" and el.get("wildcard_identifier", {}).get("star", "") == "*":
            result.add_error("SELECT * is not allowed", True)
            sql_cols = ""
            for t in result.from_tables:
                for config_t in result.config["tables"]:
                    if t.table_name == config_t["table_name"]:
                        for c in config_t["columns"]:
                            sql_cols += f"{_quote_identifier(c)}, "
            sql_cols = sql_cols[:-2]
            el.get("wildcard_identifier")["star"] = sql_cols
            return True
        elif el_name == "column_reference":
            col_name = _get_ref_col(el).column_name
            if not _find_column(col_name, result):
                result.add_error(f"Column {col_name} is not allowed. Column removed from SELECT clause")
                return False
        elif el_name in ["expression", "function"]:
            error_found = False
            for _, r_e in _get_elements(el, "column_reference", True):
                col_name = _get_ref_col(r_e).column_name
                if not _find_column(col_name, result):
                    result.add_error(f"Column {col_name} is not allowed. Column removed from SELECT clause")
                    error_found = True
            return not error_found
    return True


def _find_column(col_name: str, result: _VerificationContext) -> bool:
    for t in result.from_tables:
        for config_t in result.config["tables"]:
            if t.table_name == config_t["table_name"]:
                if col_name in config_t["columns"]:
                    return True
    return False


def _update_from_clause_tables(from_clause: dict, result: _VerificationContext):
    for _, e in _get_elements(from_clause, "from_expression"):
        table_ref = e["from_expression_element"]["table_expression"]["table_reference"]
        result.add_from_table(_get_ref_table(table_ref))


def _get_elements(clause, name: str = None, recursive: bool = False) ->  Generator[Tuple[str, any], None, None]:
    if isinstance(clause, dict):
        for k, v in clause.items():
            if name is None or name == k:
                yield k, v
            if recursive:
                for e in _get_elements(v, name, recursive):
                    yield e
    elif isinstance(clause, list):
        for e in clause:
            for k, v in e.items():
                if name is None or name == k:
                    yield k, v
                if recursive:
                    for ek in _get_elements(v, name, recursive):
                        yield ek


def _get_clause(clause: list, name: str):
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
