import logging
from typing import Optional, List, Tuple, Generator, NamedTuple, Set

import sqlfluff
from sqlfluff.api import APIParsingError


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
        self._config = config
        self._dynamic_tables: Set[str] = set()

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


    @property
    def config(self) -> dict:
        return self._config

    @property
    def dynamic_tables(self) -> Set[str]:
        return self._dynamic_tables


def verify_sql(sql: str, config: dict, dialect: str = "ansi") -> dict:
    try:
        sql_statement = None
        parse_tree = sqlfluff.parse(sql, dialect=dialect)
        for s in parse_tree["file"] if isinstance(parse_tree["file"], list) else [parse_tree["file"]]:
            if "statement" in s and "select_statement" in s["statement"]:
                sql_statement = s["statement"]["select_statement"]
                break
            if "statement" in s and "with_compound_statement" in s["statement"]:
                sql_statement = s["statement"]["with_compound_statement"]
                break
    except APIParsingError as e:
        logging.error(f"SQL: {sql}\nError parsing SQL: {e}")
        return { "allowed": False, "errors": [e.msg]}
    if sql_statement is None:
        return {"allowed": False, "errors": ["Could not find a select statement"]}
    result = _VerificationContext(config)
    sql_statement = _verify_statement(sql_statement, result)
    if result.can_fix and len(result.errors) > 0:
        result.fixed = _convert_to_text(sql_statement)
    return { "allowed": len(result.errors) == 0, "errors": result.errors, "fixed": result.fixed }


def _verify_where_clause(result: _VerificationContext, statement: list, from_tables: List[_TableRef]):
    where_clause = _get_clause(statement, "where")
    if where_clause is None:
        _build_restrictions(result, statement, from_tables)
        return
    if isinstance(where_clause, dict):
        updated_where_clause = [{k: v} for k, v in where_clause.items()]
        where_clause.clear()
        where_clause["updated"] = updated_where_clause
        where_clause = updated_where_clause
    sub_exps = []
    for _, e in _get_elements(where_clause, "expression"):
        if isinstance(e, dict):
            sub_exps.append([e])
        else:
            sub_exps.extend(_split_expression(e, "AND"))
    if not _verify_static_expression(result, sub_exps):
        return
    for t in [c_t for c_t in result.config["tables"] if c_t["table_name"] in [t.table_name for t in from_tables]]:
        for idx, r in enumerate(t.get("restrictions", [])):
            found = False
            for sub_exp in sub_exps:
                if _verify_restriction(r, sub_exp):
                    found = True
                    break
            if not found:
                result.add_error(f"Missing restriction for table: {t['table_name']} column: {r['column']} value: {r['value']}")
                where_clause.insert(1, {"opening_b": " ("})
                where_clause.append({f"{t}_{idx}": f") AND {r['column']} = {r['value']}"})


def _verify_static_expression(result: _VerificationContext, sub_exps: list) -> bool:
    has_static_exp = False
    for e in sub_exps:
        for or_exp in _split_expression(e, "OR"):
            has_ref_col = False
            for _ in _get_elements(or_exp, "column_reference", True):
                has_ref_col = True
                break
            if not has_ref_col:
                has_static_exp = True
                break
    if has_static_exp:
        result.add_error("Static expression is not allowed", False)
    return not has_static_exp


def _build_restrictions(result: _VerificationContext, statement, from_tables: List[_TableRef]):
    if all(t.table_name in result.dynamic_tables for t in from_tables):
        return
    where_str = " WHERE "
    for t in [c_t for c_t in result.config["tables"] if c_t["table_name"] in [t.table_name for t in from_tables]]:
        for r in t.get("restrictions", []):
            where_str += f"{r['column']} = {r['value']}"
            result.add_error(
                f"Missing restriction for table: {t['table_name']} column: {r['column']} value: {r['value']}")
    for idx, e in enumerate(statement):
        if isinstance(e, dict) and "from_clause" in e:
            statement.insert(idx + 1, {"where_clause": where_str})
            break


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
            return _TableRef(db_name=vals[0], table_name=vals[1])


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
    found = False
    for sub_exp in sub_exps:
        columns_found = False
        op_found = False
        value_found = False
        static_exp = True
        for e in sub_exp:
            if isinstance(e, dict):
                if "column_reference" in e:
                    if _get_ref_col(e["column_reference"]).column_name == restriction["column"]:
                        columns_found = True
                    static_exp = False
                if "comparison_operator" in e and _convert_to_text(e) == '=':
                    op_found = True
                if "numeric_literal" in e:
                    if int(e["numeric_literal"]) == restriction["value"]:
                        value_found = True
        if columns_found and op_found and value_found:
            found = True
        elif not static_exp:
            found = False
            break
    return found


def _verify_statement(statement, result: _VerificationContext) -> list:
    if isinstance(statement, dict):
        statement = [{k: statement[k]} for k in statement]
    if len(statement) > 0 and statement[0].get("keyword", "").upper() == "WITH":
        for s in statement:
            if "common_table_expression" in s:
                for sub_s in s["common_table_expression"]:
                    if "naked_identifier" in sub_s:
                        result.dynamic_tables.add(sub_s["naked_identifier"])
                    if "bracketed" in sub_s and "select_statement" in sub_s["bracketed"]:
                        sub_s["bracketed"]["select_statement"] = _verify_select_statement(sub_s["bracketed"]["select_statement"], result)
                    if "bracketed" in sub_s and "with_compound_statement" in sub_s["bracketed"]:
                        sub_s["bracketed"]["with_compound_statement"] = _verify_statement(sub_s["bracketed"]["with_compound_statement"], result)
            if "select_statement" in s:
                s["select_statement"] = _verify_statement(s["select_statement"], result)
    else:
        _verify_select_statement(statement, result)
    return statement

def _verify_select_statement(select_statement, result: _VerificationContext) -> list:
    if isinstance(select_statement, dict):
        select_statement = [{k: select_statement[k]} for k in select_statement]

    from_clause = _get_clause(select_statement, "from")
    from_tables = _get_from_clause_tables(from_clause)
    for t in from_tables:
        found = False
        for config_t in result.config["tables"]:
            if t.table_name == config_t["table_name"] or t.table_name in result.dynamic_tables:
                found = True
        if not found:
            result.add_error(f"Table {t.table_name} is not allowed", False)
    if not result.can_fix:
        return select_statement
    select_clause = _get_clause(select_statement, "select")
    _verify_select_clause(result, select_clause, from_tables)
    _verify_where_clause(result, select_statement, from_tables)
    return select_statement


def _verify_select_clause(result: _VerificationContext, select_clause: list, from_tables: List[_TableRef]):
    updated_select_clause = []
    has_legal_elements = False
    is_first_element = True
    for e_name, e in _get_elements(select_clause):
        if e_name == "select_clause_element":
            if _verify_select_clause_element(from_tables, result, e):
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

def _verify_select_clause_element(from_tables: List[_TableRef], result: _VerificationContext, e: list):
    for el_name, el in _get_elements(e):
        if el_name == "wildcard_expression" and el.get("wildcard_identifier", {}).get("star", "") == "*":
            result.add_error("SELECT * is not allowed", True)
            sql_cols = ""
            for t in from_tables:
                for config_t in result.config["tables"]:
                    if t.table_name == config_t["table_name"]:
                        for c in config_t["columns"]:
                            sql_cols += f"{_quote_identifier(c)}, "
            sql_cols = sql_cols[:-2]
            el.get("wildcard_identifier")["star"] = sql_cols
            return True
        elif el_name == "column_reference":
            col_name = _get_ref_col(el).column_name
            if not _find_column(col_name, from_tables, result):
                result.add_error(f"Column {col_name} is not allowed. Column removed from SELECT clause")
                return False
        elif el_name in ["expression", "function"]:
            error_found = False
            for _, r_e in _get_elements(el, "column_reference", True):
                col_name = _get_ref_col(r_e).column_name
                if not _find_column(col_name, from_tables, result):
                    result.add_error(f"Column {col_name} is not allowed. Column removed from SELECT clause")
                    error_found = True
            return not error_found
    return True


def _find_column(col_name: str, from_tables: List[_TableRef], result: _VerificationContext) -> bool:
    if all(t.table_name in result.dynamic_tables for t in from_tables):
        return True
    for t in from_tables:
        for config_t in result.config["tables"]:
            if t.table_name == config_t["table_name"]:
                if col_name in config_t["columns"]:
                    return True
    return False


def _get_from_clause_tables(from_clause: dict) -> List[_TableRef]:
    result = []
    for _, e in _get_elements(from_clause, "from_expression"):
        for _, f in _get_elements(e, "from_expression_element", True):
            table_ref = f["table_expression"]["table_reference"]
            result.append(_get_ref_table(table_ref))
    return result




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
