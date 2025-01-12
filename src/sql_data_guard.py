import logging
from typing import Optional, List, Tuple, Generator, NamedTuple, Set

import sqlfluff
from sqlfluff.api import APIParsingError


class _TableRef(NamedTuple):
    """
    Represents a reference to a table in the SQL query.

    Attributes:
        table_name (str): The name of the table.
        db_name (Optional[str]): The name of the database. Defaults to None.
    """
    table_name: str
    db_name: Optional[str] = None


class _ColumnRef(NamedTuple):
    """
    Represents a reference to a column in the SQL query.

    Attributes:
        column_name (str): The name of the column.
        table_name (Optional[str]): The name of the table. Defaults to None.
        db_name (Optional[str]): The name of the database. Defaults to None.
    """
    column_name: str
    table_name: Optional[str] = None
    db_name: Optional[str] = None


class _VerificationContext:
    """
    Context for verifying SQL queries against a given configuration.

    Attributes:
        _can_fix (bool): Indicates if the query can be fixed.
        _errors (List[str]): List of errors found during verification.
        _fixed (Optional[str]): The fixed query if modifications were made.
        _config (dict): The configuration used for verification.
        _dynamic_tables (Set[str]): Set of dynamic tables found in the query, like sub select and WITH clauses.
        _dialect (str): The SQL dialect to use for parsing.
    """
    def __init__(self, config: dict, dialect: str):
        super().__init__()
        self._can_fix = True
        self._errors = set()
        self._fixed = None
        self._config = config
        self._dynamic_tables: Set[str] = set()
        self._dialect = dialect

    @property
    def can_fix(self) -> bool:
        return self._can_fix

    def add_error(self, error: str, can_fix: bool = True):
        self._errors.add(error)
        if not can_fix:
            self._can_fix = False

    @property
    def errors(self) -> Set[str]:
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

    @property
    def dialect(self) -> str:
        return self._dialect


def verify_sql(sql: str, config: dict, dialect: str = "ansi") -> dict:
    """
    Verifies an SQL query against a given configuration and optionally fixes it.

    Args:
        sql (str): The SQL query to verify.
        config (dict): The configuration specifying allowed tables, columns, and restrictions.
        dialect (str, optional): The SQL dialect to use for parsing. Defaults to "ansi".

    Returns:
        dict: A dictionary containing:
            - "allowed" (bool): Whether the query is allowed to run.
            - "errors" (List[str]): List of errors found during verification.
            - "fixed" (Optional[str]): The fixed query if modifications were made.
    """
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
    result = _VerificationContext(config, dialect)
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
    sub_exps = _get_expressions(where_clause)
    if not _verify_static_expression(result, sub_exps):
        return
    for t in [c_t for c_t in result.config["tables"] if c_t["table_name"] in [t.table_name for t in from_tables]]:
        for idx, r in enumerate(t.get("restrictions", [])):
            found = False
            for sub_exp in sub_exps:
                sub_exp = _extract_bracketed(sub_exp)
                if _verify_restriction(r, sub_exp):
                    found = True
                    break
            if not found:
                result.add_error(f"Missing restriction for table: {t['table_name']} column: {r['column']} value: {r['value']}")
                where_clause.insert(1, {"opening_b": " ("})
                value = f"'{r['value']}'" if isinstance(r["value"], str) else r["value"]
                where_clause.append({f"{t}_{idx}": f") AND {r['column']} = {value}"})

def _get_expressions(parent) -> List[list]:
    result = []
    for _, e in _get_elements(parent, "expression"):
        if isinstance(e, dict):
            result.append([e])
        else:
            result.extend(_split_expression(e, "AND"))
    return result


def _verify_static_expression(result: _VerificationContext, sub_exps: list) -> bool:
    has_static_exp = False
    for e in sub_exps:
        if _has_static_expression(e):
            has_static_exp = True
            break
    if has_static_exp:
        result.add_error("Static expression is not allowed", False)
    return not has_static_exp

def _has_static_expression(exp: list) -> bool:
    exp = _extract_bracketed(exp)
    for or_exp in _split_expression(exp, "OR"):
        or_exp = _extract_bracketed(or_exp)
        has_binary_op = False
        for e in or_exp:
            if isinstance(e, dict) and "binary_operator" in e:
                has_binary_op = True
                break
        if not has_binary_op:
            if _get_element(or_exp, "column_reference") is None:
                return True
        else:
            for and_exp in _split_expression(or_exp, "AND"):
                if _has_static_expression(and_exp):
                    return True
    return False


def _extract_bracketed(e: list, exp_name: str = "expression") -> list:
    sub_e = _get_element(e, exp_name)
    return sub_e if sub_e else e


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
        if isinstance(el, dict) and el.get("binary_operator", "").upper() == binary_operator:
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
    """
    Finds a referenced column inside an expression.

    Args:
        e (Union[dict, list]): The expression to search within.

    Returns:
        _ColumnRef: The column reference to find in the expression.
    """
    if isinstance(e, dict):
        return _ColumnRef(column_name=_get_ref_value(e))
    elif isinstance(e, list):
        if len(e) == 3:
            return _ColumnRef(table_name=_get_ref_value(e[0]), column_name=_get_ref_value(e[-1]))
        else:
            return _get_ref_col(e[-1])

def _created_reference_value(value: str) -> dict:
    return {"quoted_identifier": _quote_identifier(value)}

def _quote_identifier(value: str) -> str:
    if "-" in value or " " in value:
        return f'"{value}"'
    else:
        return value

def _verify_restriction(restriction: dict, exp: list) -> bool:
    """
       Verifies if a given restriction is satisfied within an SQL expression.

       Args:
           restriction (dict): The restriction to verify, containing 'column' and 'value' keys.
           exp (list): The SQL expression to check against the restriction.

       Returns:
           bool: True if the restriction is satisfied, False otherwise.
   """
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
                elif "comparison_operator" in e and _convert_to_text(e) == '=':
                    op_found = True
                elif "numeric_literal" in e:
                    if int(e["numeric_literal"]) == restriction["value"]:
                        value_found = True
                elif "boolean_literal" in e:
                    if e["boolean_literal"].upper() == str(restriction["value"]).upper():
                        value_found = True
                elif "quoted_literal" in e:
                    if e["quoted_literal"].strip("'") == restriction["value"]:
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
                    if "bracketed" in sub_s:
                        if isinstance(sub_s["bracketed"], dict):
                            sub_s["bracketed"] = [{k: sub_s["bracketed"][k]} for k in sub_s["bracketed"]]
                        for e_name in ["select_statement", "with_compound_statement"]:
                            for sub_s_dict in sub_s["bracketed"]:
                                if e_name in sub_s_dict:
                                    sub_s_dict[e_name] = _verify_statement(sub_s_dict[e_name], result)
            if "select_statement" in s:
                s["select_statement"] = _verify_statement(s["select_statement"], result)
    else:
        _verify_select_statement(statement, result)
    return statement

def _verify_select_statement(select_statement, result: _VerificationContext) -> list:
    if isinstance(select_statement, dict):
        select_statement = [{k: select_statement[k]} for k in select_statement]

    from_clause = _get_clause(select_statement, "from")
    from_tables = _get_from_clause_tables(from_clause, result)
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
            if not _verify_col(_get_ref_col(el), from_tables, result):
                return False
        elif el_name == "expression":
            error_found = False
            for e in el.values() if isinstance(el, dict) else el:
                if isinstance(e, dict) or isinstance(e, list):
                    if not _verify_select_clause_element(from_tables, result, e):
                        error_found = True
            return not error_found
        elif el_name ==  "function":
            if "function_contents" in el:
                content = el["function_contents"]["bracketed"]
            else:
                content = el["bracketed"]
            error_found = False
            max_param_index = _get_max_param_index(el["function_name"]["function_name_identifier"], el, result.dialect)
            param_index = 0
            for e in content.values() if isinstance(content, dict) else content:
                if isinstance(e, dict) or isinstance(e, list):
                    if not _verify_select_clause_element(from_tables, result, e):
                        error_found = True
                if e == {"comma": ","}:
                    param_index += 1
                    if param_index == max_param_index:
                        break
            return not error_found
    return True

def _verify_col(ref_col: _ColumnRef, from_tables: List[_TableRef], context: _VerificationContext) -> bool:
    """
    Verifies if a column reference is allowed based on the provided tables and context.

    Args:
        ref_col (_ColumnRef): The column reference to verify.
        from_tables (List[_TableRef]): The list of tables to search within.
        context (_VerificationContext): The context for verification.

    Returns:
        bool: True if the column reference is allowed, False otherwise.
    """
    if ref_col.table_name and ref_col.table_name in context.dynamic_tables:
        pass
    elif not _find_column(ref_col.column_name, from_tables, context):
        context.add_error(f"Column {ref_col.column_name} is not allowed. Column removed from SELECT clause")
        return False
    return True


_TRINO_FUNCTION_MAX_PARAM_1 = {"REDUCE", "TRANSFORM", "TRANSFORM_KEYS", "TRANSFORM_VALUES", "MAP_FILTER", "FILTER",
                               "ALL_MATCH", "ANY_MATCH", "NONE_MATCH"}

def _get_max_param_index(func_name: str, el: dict, dialect: str) -> int:
    """
    Get the number of parameters for a function to search in for column reference
    :param func_name: function name
    :param el: function element
    :param dialect: sql dialect
    :return: -1 if all parameters should be checked, 0 if no parameters should be checked, or max index  parameter to check
    """
    if dialect in {"trino", "athena"}:
        if func_name.upper() in _TRINO_FUNCTION_MAX_PARAM_1:
            return 1
    return -1


def _find_column(col_name: str, from_tables: List[_TableRef], result: _VerificationContext) -> bool:
    """
    Finds a column in the given tables based on the provided column name.

    Args:
        col_name (str): The name of the column to find.
        from_tables (List[_TableRef]): The list of tables to search within.
        result (_VerificationContext): The context for verification.

    Returns:
        bool: True if the column is found in any of the tables, False otherwise.
    """
    if all(t.table_name in result.dynamic_tables for t in from_tables):
        return True
    for t in from_tables:
        for config_t in result.config["tables"]:
            if t.table_name == config_t["table_name"]:
                if col_name in config_t["columns"]:
                    return True
    return False


def _get_from_clause_tables(from_clause: dict, context: _VerificationContext) -> List[_TableRef]:
    """
        Extracts table references from the FROM clause of an SQL query.

        Args:
            from_clause (dict): The FROM clause of the SQL query.
            context (_VerificationContext): The context for verification.

        Returns:
            List[_TableRef]: A list of table references to find in the FROM clause.
    """
    result = []
    for _, e in _get_elements(from_clause, "from_expression"):
        for _, f in _get_elements(e, "from_expression_element"):
            table_ref = _handle_from_element(f, context)
            if table_ref:
                result.append(table_ref)
        for _, j in _get_elements(e, "join_clause"):
            for _, f in _get_elements(j, "from_expression_element"):
                table_ref = _handle_from_element(f, context)
                if table_ref:
                    result.append(table_ref)
    return result

def _handle_from_element(f: dict, context: _VerificationContext) -> Optional[_TableRef]:
    table_ref = f["table_expression"].get("table_reference")
    if table_ref:
        result = _get_ref_table(table_ref)
    else:
        result = None
        s = _get_element(f["table_expression"], "select_statement")
        updated = _verify_select_statement(s, context)
        s.clear()
        s[""] = updated
    if "alias_expression" in f:
        context.dynamic_tables.add(_get_ref_value(f["alias_expression"]))
    return result


def _get_elements(clause, name: str = None, max_param_index: int = -1) ->  Generator[Tuple[str, any], None, None]:
    """
        Retrieves elements from a given SQL clause.

        Args:
            clause: The SQL clause to search within, which can be a list or a dict.
            name (str, optional): The name of the element to find. Defaults to None.
            max_param_index (int, optional): The maximum parameter index to search up to. Defaults to -1.

        Yields:
            Tuple[str, any]: A tuple containing the element name and the element itself.
    """
    p_index = 0
    for e in clause if isinstance(clause, list) else [clause]:
        for k, v in e.items():
            if k == "bracketed":
                yield from _get_elements(v, name)
            if name is None or name == k:
                yield k, v
            if k == "comma":
                p_index += 1
                if p_index == max_param_index:
                    break
        if p_index == max_param_index:
            break

def _get_element(clause, name: str = None) -> Optional[dict]:
    """
        Retrieves the first element in the clause that matches the given name.

        Args:
            clause: The clause to search within, which can be a list or a dict.
            name (str, optional): The name of the element to find. Defaults to None.

        Returns:
            Optional[dict]: The first matching element if found, otherwise None.
    """
    for e in _get_elements(clause, name):
        if e:
            return e[1]
    return None

def _get_clause(clause: list, name: str):
    for c in clause:
        if f"{name}_clause" in c:
            return c[f"{name}_clause"]
    return None


def _convert_to_text(item) -> str:
    """
        Converts a parsed SQL element into its textual representation.

        Args:
            item: The parsed SQL element, which can be a dict, list, or a string.

        Returns:
            str: The textual representation of the SQL element.
    """
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
