"""
SQL Data Guard core verification library.

This module provides the main verify_sql function and internal verification routines
to validate SQL queries against strict schema restriction policies.
"""

import logging
from typing import Any

import sqlglot
import sqlglot.expressions as expr
from sqlglot.optimizer.simplify import simplify

from .restriction_validation import UnsupportedRestrictionError, validate_restrictions
from .restriction_verification import verify_restrictions
from .verification_context import VerificationContext
from .verification_utils import find_direct, split_to_expressions

_DEFAULT_MAX_LENGTH = 10_000


def verify_sql(sql: str, config: dict[str, Any], dialect: str | None = None) -> dict[str, Any]:
    """
    Verifies an SQL query against a given configuration and optionally fixes it.

    Args:
        sql (str): The SQL query to verify.
        config (dict): The configuration specifying allowed tables, columns, and restrictions.
        dialect (str, optional): The SQL dialect to use for parsing

    Returns:
        dict: A dictionary containing:
            - "allowed" (bool): Whether the query is allowed to run.
            - "errors" (List[str]): List of errors found during verification.
            - "fixed" (Optional[str]): The fixed query if modifications were made.
            - "risk" (float): Verification risk score (0 - no risk, 1 - high risk)
    """
    # Check if the config is empty or invalid (e.g., no 'tables' key)
    if not config or not isinstance(config, dict) or "tables" not in config:
        return {
            "allowed": False,
            "errors": ["Invalid configuration provided. The configuration must include 'tables'."],
            "fixed": None,
            "risk": 1.0,
        }
    max_length = config.get("max_length", _DEFAULT_MAX_LENGTH)
    if len(sql) > max_length:
        return {
            "allowed": False,
            "errors": [f"SQL exceeds maximum length of {max_length} characters."],
            "fixed": None,
            "risk": 1.0,
        }

    # First, validate restrictions
    try:
        validate_restrictions(config)
    except UnsupportedRestrictionError as e:
        return {"allowed": False, "errors": [str(e)], "fixed": None, "risk": 1.0}

    result = VerificationContext(config, dialect)
    try:
        try:
            parsed = sqlglot.parse_one(sql, dialect=dialect)
        except (sqlglot.errors.ParseError, RecursionError, ValueError) as e:
            logging.error(f"SQL: {sql}\nError parsing SQL: {e}")
            result.add_error(f"Error parsing sql: {e}", False, 0.9)
            parsed = None
        if parsed:
            if isinstance(parsed, expr.Block):
                active_exprs = [e for e in parsed.expressions if not isinstance(e, expr.Semicolon)]
                if len(active_exprs) > 1:
                    result.add_error("Stacked queries are not allowed", False, 0.9)
                    parsed = None
                elif len(active_exprs) == 1:
                    parsed = active_exprs[0]
                else:
                    result.add_error("Could not find a query statement", False, 0.7)
                    parsed = None
            if isinstance(parsed, expr.Command):
                result.add_error(f"{parsed.name} statement is not allowed", False, 0.9)
            elif isinstance(parsed, (expr.Delete, expr.Insert, expr.Update, expr.Create)):
                result.add_error(f"{parsed.key.upper()} statement is not allowed", False, 0.9)
            elif isinstance(parsed, expr.Query):
                _verify_query_statement(parsed, result)
            else:
                result.add_error("Could not find a query statement", False, 0.7)
        if result.can_fix and len(result.errors) > 0 and parsed:
            result.fixed = parsed.sql(dialect=dialect)
    except RecursionError as e:
        logging.error(f"Recursion error during verification of SQL: {sql}\nError: {e}")
        result.add_error(f"Query nesting depth exceeded safe limits: {e}", False, 0.9)
    return {
        "allowed": len(result.errors) == 0,
        "errors": result.errors,
        "fixed": result.fixed,
        "risk": result.risk,
    }


def _verify_where_clause(
    context: VerificationContext,
    select_statement: expr.Query,
    from_tables: list[expr.Table],
) -> None:
    """
    Verifies the WHERE clause of a query, auditing subqueries and verifying restrictions.

    Args:
        context (VerificationContext): The current verification context.
        select_statement (expr.Query): The query select statement containing the WHERE clause.
        from_tables (list[expr.Table]): List of tables referenced in the query's FROM/JOIN clauses.
    """
    where_clause = select_statement.find(expr.Where)
    if where_clause:
        for sub in where_clause.find_all(expr.Subquery, expr.Exists):
            _verify_query_statement(sub.this, context)
    _verify_static_expression(select_statement, context)
    verify_restrictions(select_statement, context, from_tables)


def _verify_static_expression(select_statement: expr.Query, context: VerificationContext) -> bool:
    """
    Checks for and simplifies any static (constant) expressions in the WHERE clause.

    Args:
        select_statement (expr.Query): The query select statement to inspect.
        context (VerificationContext): The current verification context.

    Returns:
        bool: True if there were no static expressions, False if static expressions were found.
    """
    has_static_exp = False
    where_clause = select_statement.find(expr.Where)
    if where_clause:
        and_exps = list(split_to_expressions(where_clause.this, expr.And))
        for e in and_exps:
            if _has_static_expression(context, e):
                has_static_exp = True
    if has_static_exp and where_clause is not None:
        simplify(where_clause)
    return not has_static_exp


def _has_static_expression(context: VerificationContext, exp: expr.Expression) -> bool:
    """
    Recursively audits an expression to identify unauthorized static/constant parts.

    Args:
        context (VerificationContext): The current verification context.
        exp (expr.Expression): The SQL expression to audit.

    Returns:
        bool: True if a static expression is detected, False otherwise.
    """
    if isinstance(exp, expr.Not):
        return _has_static_expression(context, exp.this)
    if isinstance(exp, expr.And):
        for sub_and_exp in split_to_expressions(exp, expr.And):
            if _has_static_expression(context, sub_and_exp):
                return True
    result = False
    to_replace = []
    for sub_exp in split_to_expressions(exp, expr.Or):
        if isinstance(sub_exp, expr.Or):
            result = _has_static_expression(context, sub_exp)
        elif not sub_exp.find(expr.Column):
            context.add_error(f"Static expression is not allowed: {sub_exp.sql()}", True, 0.8)
            par = sub_exp.parent
            while isinstance(par, expr.Paren):
                par = par.parent
            if isinstance(par, expr.Or):
                to_replace.append(sub_exp)
            result = True
    for e in to_replace:
        e.replace(expr.Boolean(this=False))
    return result


def _get_in_scope_table_names(query_statement: expr.Query, context: VerificationContext) -> set[str]:
    in_scope = set()
    from_clause = query_statement.find(expr.From)
    join_clauses = query_statement.args.get("joins", [])
    for clause in [from_clause] + join_clauses:
        if clause:
            for alias in clause.find_all(expr.TableAlias):
                in_scope.add(alias.alias_or_name)
            for t in clause.find_all(expr.Table):
                in_scope.add(t.alias_or_name)
                in_scope.add(t.name)
    return in_scope


def _verify_query_statement(query_statement: expr.Query, context: VerificationContext) -> None:
    """
    Recursively verifies CTEs, UNION branches, FROM tables, SELECT expressions, and WHERE clauses in a query.

    Args:
        query_statement (expr.Query): The parsed query expression.
        context (VerificationContext): The current verification context.
    """
    if isinstance(query_statement, expr.Union):
        _verify_query_statement(query_statement.left, context)
        _verify_query_statement(query_statement.right, context)
        return
    old_in_scope = context.current_in_scope_tables
    context.current_in_scope_tables = old_in_scope | _get_in_scope_table_names(query_statement, context)
    for cte in query_statement.ctes:
        cte_old_in_scope = context.current_in_scope_tables
        context.current_in_scope_tables = set(context.dynamic_tables.keys())
        _verify_query_statement(cte.this, context)
        context.current_in_scope_tables = cte_old_in_scope
        _add_table_alias(cte, context)
    from_tables = _verify_from_tables(context, query_statement)
    if context.can_fix:
        _verify_select_clause(context, query_statement, from_tables)
        _verify_where_clause(context, query_statement, from_tables)
        _verify_sub_queries(context, query_statement)
    context.current_in_scope_tables = old_in_scope


def _verify_from_tables(context: VerificationContext, query_statement: expr.Query) -> list[expr.Table]:
    """
    Validates that all tables referenced in the query's FROM/JOIN clause are authorized.

    Args:
        context (VerificationContext): The current verification context.
        query_statement (expr.Query): The parsed query containing the FROM clause.

    Returns:
        list[expr.Table]: A list of verified referenced tables.
    """
    from_tables = _get_from_clause_tables(query_statement, context)
    for t in from_tables:
        found = False
        for config_t in context.config["tables"]:
            if t.name == config_t["table_name"] or t.name in context.dynamic_tables:
                found = True
        if not found:
            context.add_error(f"Table {t.name} is not allowed", False, 1)
    return from_tables


def _verify_sub_queries(context: VerificationContext, query_statement: expr.Query) -> None:
    """
    Audits other non-FROM query clauses (like ORDER BY, LIMIT, HAVING, GROUP BY) for nested subqueries.

    Args:
        context (VerificationContext): The current verification context.
        query_statement (expr.Query): The query statement to audit.
    """
    for exp_type in [expr.Order, expr.Offset, expr.Limit, expr.Group, expr.Having]:
        for exp in find_direct(query_statement, exp_type):
            if exp:
                for sub in exp.find_all(expr.Subquery):
                    _verify_query_statement(sub.this, context)


def _verify_select_clause(
    context: VerificationContext,
    select_clause: expr.Query,
    from_tables: list[expr.Table],
) -> None:
    """
    Validates all select elements in the SELECT clause, removing unauthorized columns.

    Args:
        context (VerificationContext): The current verification context.
        select_clause (expr.Query): The SELECT statement to verify.
        from_tables (list[expr.Table]): List of referenced tables.
    """
    for select in select_clause.selects:
        for sub in select.find_all(expr.Subquery):
            _add_table_alias(sub, context)
            _verify_query_statement(sub.this, context)
    to_remove = []
    for e in select_clause.expressions:
        if not _verify_select_clause_element(from_tables, context, e):
            to_remove.append(e)
    for e in to_remove:
        select_clause.expressions.remove(e)
    if len(select_clause.expressions) == 0:
        context.add_error("No legal elements in SELECT clause", False, 0.5)


def _verify_select_clause_element(
    from_tables: list[expr.Table], context: VerificationContext, e: expr.Expression
) -> bool:
    """
    Checks if an individual expression element in the SELECT clause is allowed.

    Args:
        from_tables (list[expr.Table]): List of referenced tables.
        context (VerificationContext): The current verification context.
        e (expr.Expression): The individual SELECT element to check.

    Returns:
        bool: True if the element is allowed/valid, False otherwise.
    """
    if isinstance(e, expr.Column):
        if not _verify_col(e, from_tables, context):
            return False
    elif isinstance(e, expr.Star):
        context.add_error("SELECT * is not allowed", True, 0.1)
        for t in from_tables:
            for config_t in context.config["tables"]:
                if t.name == config_t["table_name"]:
                    for c in config_t["columns"]:
                        if e.parent is not None:
                            e.parent.set(
                                "expressions",
                                e.parent.expressions + [sqlglot.parse_one(c)],
                            )
        return False
    elif isinstance(e, expr.Tuple):
        result = True
        for sub_e in e.expressions:
            if not _verify_select_clause_element(from_tables, context, sub_e):
                result = False
        return result
    else:
        for func_args in e.find_all(expr.Column):
            if not _verify_select_clause_element(from_tables, context, func_args):
                return False
    return True


def _verify_col(col: expr.Column, from_tables: list[expr.Table], context: VerificationContext) -> bool:
    """
    Verifies if a column reference is allowed based on the provided tables and context.

    Args:
        col (Column): The column reference to verify.
        from_tables (List[_TableRef]): The list of tables to search within.
        context (VerificationContext): The context for verification.

    Returns:
        bool: True if the column reference is allowed, False otherwise.
    """
    if (
        col.table == "sub_select"
        or (col.table != "" and col.table in context.dynamic_tables)
        or (all(t.name in context.dynamic_tables for t in from_tables))
        or (
            col.table == ""
            and any(col.name in context.dynamic_tables.get(tbl, set()) for tbl in context.current_in_scope_tables)
        )
        or (
            any(
                col.name in config_t["columns"]
                for config_t in context.config["tables"]
                for t in from_tables
                if t.name == config_t["table_name"]
            )
        )
    ):
        return True
    context.add_error(
        f"Column {col.name} is not allowed. Column removed from SELECT clause",
        True,
        0.3,
    )
    return False


def _get_from_clause_tables(select_clause: expr.Query, context: VerificationContext) -> list[expr.Table]:
    """
    Extracts table references from the FROM clause of an SQL query.

    Args:
        select_clause (dict): The FROM clause of the SQL query.
        context (VerificationContext): The context for verification.

    Returns:
        List[_TableRef]: A list of table references to find in the FROM clause.
    """
    result = []
    from_clause = select_clause.find(expr.From)
    join_clauses = select_clause.args.get("joins", [])
    for clause in [from_clause] + join_clauses:
        if clause:
            for t in find_direct(clause, expr.Table):
                if isinstance(t, expr.Table):
                    result.append(t)
            for subq in find_direct(clause, expr.Subquery):
                _add_table_alias(subq, context)
                _verify_query_statement(subq.this, context)
    for join_clause in join_clauses:
        for lat in find_direct(join_clause, expr.Lateral):
            _add_table_alias(lat, context)
            _verify_query_statement(lat.this.find(expr.Select), context)
        for u in find_direct(join_clause, expr.Unnest):
            _add_table_alias(u, context)
    return result


def _add_table_alias(exp: expr.Expression, context: VerificationContext) -> None:
    """
    Registers a dynamic table alias and its generated column names into the verification context.

    Args:
        exp (expr.Expression): The expression containing a table alias.
        context (VerificationContext): The current verification context.
    """
    for table_alias in find_direct(exp, expr.TableAlias):
        if isinstance(table_alias, expr.TableAlias):
            if len(table_alias.columns) > 0:
                column_names = {col.alias_or_name for col in table_alias.columns}
            else:
                column_names = set(exp.this.named_selects)
            context.dynamic_tables[table_alias.alias_or_name] = column_names
