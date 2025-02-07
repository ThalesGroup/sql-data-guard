import logging
from typing import Optional, List, Generator, Set, Type

import sqlglot
import sqlglot.expressions as expr


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


def verify_sql(sql: str, config: dict, dialect: str = None) -> dict:
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
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        logging.error(f"SQL: {sql}\nError parsing SQL: {e}")
        return { "allowed": False, "errors": e.errors}
    if not isinstance(parsed, expr.Select):
        return {"allowed": False, "errors": ["Could not find a select statement"]}
    result = _VerificationContext(config, dialect)
    _verify_select_statement(parsed, result)
    if result.can_fix and len(result.errors) > 0:
        result.fixed = parsed.sql()
    return { "allowed": len(result.errors) == 0, "errors": result.errors, "fixed": result.fixed }


def _verify_where_clause(result: _VerificationContext, select_statement: expr.Select,
                         from_tables: List[expr.Table]):
    where_clause = select_statement.find(expr.Where)
    if where_clause is None:
        where_clause = select_statement.find(expr.Where)
        and_exps = []
    else:
        and_exps = list(_split_to_expressions(where_clause.this, expr.And))
    if not _verify_static_expression(result, and_exps):
        return
    for t in [c_t for c_t in result.config["tables"] if c_t["table_name"] in [t.name for t in from_tables]]:
        for idx, r in enumerate(t.get("restrictions", [])):
            found = False
            for sub_exp in and_exps:
                if _verify_restriction(r, sub_exp):
                    found = True
                    break
            if not found:
                result.add_error(f"Missing restriction for table: {t['table_name']} column: {r['column']} value: {r['value']}")
                value = f"'{r['value']}'" if isinstance(r["value"], str) else r["value"]
                new_condition = sqlglot.parse_one(f"{r['column']} = {value}", dialect=result.dialect)
                if where_clause is None:
                    where_clause = expr.Where(this=new_condition)
                    select_statement.set("where", where_clause)
                else:
                    where_clause = where_clause.replace(expr.Where(this=expr.And(this=expr.paren(where_clause.this),
                                                                                 expression=new_condition)))


def _verify_static_expression(context: _VerificationContext, exps: List[expr.Expression]) -> bool:
    has_static_exp = False
    for e in exps:
        if _has_static_expression(context, e):
            has_static_exp = True
    return not has_static_exp

def _has_static_expression(context: _VerificationContext, exp: expr.Expression) -> bool:
    if isinstance(exp, expr.Not):
        return _has_static_expression(context, exp.this)
    result = False
    for sub_exp in _split_to_expressions(exp, expr.Or):
        if isinstance(sub_exp, (expr.Or, expr.And)):
            result = _has_static_expression(context, sub_exp)
        elif not sub_exp.find(expr.Column):
            context.add_error(
                f"Static expression is not allowed: {sub_exp.sql()}", False)
            result = True
    return result


def _verify_restriction(restriction: dict, exp: expr.Expression) -> bool:
    """
       Verifies if a given restriction is satisfied within an SQL expression.

       Args:
           restriction (dict): The restriction to verify, containing 'column' and 'value' keys.
           exp (list): The SQL expression to check against the restriction.

       Returns:
           bool: True if the restriction is satisfied, False otherwise.
   """
    if isinstance(exp, expr.Not):
        return False
    if isinstance(exp, expr.Paren):
        return _verify_restriction(restriction, exp.this)

    if not isinstance(exp.this, expr.Column):
        return False
    if not exp.this.name == restriction["column"]:
        return False
    if not isinstance(exp, expr.EQ):
        return False
    if isinstance(exp.right, expr.Condition):
        if isinstance(exp.right, expr.Boolean):
            return exp.right.this == restriction["value"]
        else:
            return exp.right.this == str(restriction["value"])
    return False

def _verify_select_statement(select_statement: expr.Select,
                             context: _VerificationContext):
    for cte in select_statement.ctes:
        context.dynamic_tables.add(cte.alias)
        _verify_select_statement(cte.this, context)
    from_tables = _get_from_clause_tables(select_statement, context)
    for t in from_tables:
        found = False
        for config_t in context.config["tables"]:
            if t.name == config_t["table_name"] or t.name in context.dynamic_tables:
                found = True
        if not found:
            context.add_error(f"Table {t.name} is not allowed", False)
    if not context.can_fix:
        return select_statement
    _verify_select_clause(context, select_statement, from_tables)
    _verify_where_clause(context, select_statement, from_tables)
    return select_statement


def _verify_select_clause(context: _VerificationContext,
                          select_clause: expr.Select,
                          from_tables: List[expr.Table]):
    to_remove = []
    for e in select_clause.expressions:
        if not _verify_select_clause_element(from_tables, context, e):
            to_remove.append(e)
    for e in to_remove:
        select_clause.expressions.remove(e)
    if len(select_clause.expressions) == 0:
        context.add_error("No legal elements in SELECT clause", False)

def _verify_select_clause_element(from_tables: List[expr.Table], context: _VerificationContext,
                                  e: expr.Expression):
    if isinstance(e, expr.Column):
        if not _verify_col(e, from_tables, context):
            return False
    elif isinstance(e, expr.Star):
        context.add_error("SELECT * is not allowed", True)
        for t in from_tables:
            for config_t in context.config["tables"]:
                if t.name == config_t["table_name"]:
                    for c in config_t["columns"]:
                        e.parent.set("expressions", e.parent.expressions + [sqlglot.parse_one(c)])
        return False
    elif isinstance(e, expr.Tuple):
        result = True
        for e in e.expressions:
            if not _verify_select_clause_element(from_tables, context, e):
                result = False
        return result
    else:
        for func_args in e.find_all(expr.Column):
            if not _verify_select_clause_element(from_tables, context, func_args):
                return False
    return True

def _verify_col(col: expr.Column, from_tables: List[expr.Table], context: _VerificationContext) -> bool:
    """
    Verifies if a column reference is allowed based on the provided tables and context.

    Args:
        col (Column): The column reference to verify.
        from_tables (List[_TableRef]): The list of tables to search within.
        context (_VerificationContext): The context for verification.

    Returns:
        bool: True if the column reference is allowed, False otherwise.
    """
    if col.table == "sub_select" or  col.table != "" and col.table in context.dynamic_tables:
        pass
    elif not _find_column(col.name, from_tables, context):
        context.add_error(f"Column {col.name} is not allowed. Column removed from SELECT clause")
        return False
    return True


def _find_column(col_name: str, from_tables: List[expr.Table], result: _VerificationContext) -> bool:
    """
    Finds a column in the given tables based on the provided column name.

    Args:
        col_name (str): The name of the column to find.
        from_tables (List[expr.Table]): The list of tables to search within.
        result (_VerificationContext): The context for verification.

    Returns:
        bool: True if the column is found in any of the tables, False otherwise.
    """
    if all(t.name in result.dynamic_tables for t in from_tables):
        return True
    for t in from_tables:
        for config_t in result.config["tables"]:
            if t.name == config_t["table_name"]:
                if col_name in config_t["columns"]:
                    return True
    return False


def _get_from_clause_tables(select_clause: expr.Select, context: _VerificationContext) -> List[expr.Table]:
    """
        Extracts table references from the FROM clause of an SQL query.

        Args:
            select_clause (dict): The FROM clause of the SQL query.
            context (_VerificationContext): The context for verification.

        Returns:
            List[_TableRef]: A list of table references to find in the FROM clause.
    """
    result = []
    from_clause = select_clause.find(expr.From)
    join_clause = select_clause.find(expr.Join)
    for clause in [from_clause, join_clause]:
        if clause:
            for t in _find_direct(clause, expr.Table):
                if t.alias != "":
                    context.dynamic_tables.add(t.alias)
                if isinstance(t, expr.Table):
                    result.append(t)
            for j in _find_direct(clause, expr.Subquery):
                if j.alias != "":
                    context.dynamic_tables.add(j.alias)
                _verify_select_statement(j.this, context)
    if join_clause:
        for j in _find_direct(clause, expr.Lateral):
            if j.alias != "":
                context.dynamic_tables.add(j.alias)
            _verify_select_statement(j.this.find(expr.Select), context)
    return result


def _split_to_expressions(exp: expr.Expression,
                          exp_type: Type[expr.Expression]) -> Generator[expr.Expression, None, None]:
    if isinstance(exp, exp_type):
        yield from exp.flatten()
    else:
        yield exp

def _find_direct(exp: expr.Expression, exp_type: Type[expr.Expression]):
    for child in exp.args.values():
        if isinstance(child, exp_type):
            yield child
