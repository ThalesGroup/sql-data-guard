import logging
from typing import List

import sqlglot
import sqlglot.expressions as expr
from sqlglot.optimizer.simplify import simplify

from .restriction_validation import validate_restrictions, UnsupportedRestrictionError
from .restriction_verification import verify_restrictions
from .verification_context import VerificationContext
from .verification_utils import (
    split_to_expressions,
    find_direct,
    table_matches,
    get_where,
    get_from,
)

_DEFAULT_MAX_LENGTH = 10_000


def verify_sql(sql: str, config: dict, dialect: str = None) -> dict:
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
            "errors": [
                "Invalid configuration provided. The configuration must include 'tables'."
            ],
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
    except (UnsupportedRestrictionError, ValueError) as e:
        return {"allowed": False, "errors": [str(e)], "fixed": None, "risk": 1.0}

    result = VerificationContext(config, dialect)
    parsed = None
    try:
        statements = [s for s in sqlglot.parse(sql, dialect=dialect) if s is not None]
    except sqlglot.errors.SqlglotError as e:
        # Do not log the SQL itself: it may contain sensitive literals.
        logging.error(f"Error parsing SQL ({type(e).__name__}, length {len(sql)})")
        result.add_error(f"Error parsing sql: {e}", False, 0.9)
    else:
        if len(statements) == 0:
            result.add_error("Error parsing sql: no statement found", False, 0.9)
        elif len(statements) > 1:
            result.add_error("Multiple statements are not allowed", False, 1)
        else:
            parsed = statements[0]
    if parsed:
        if isinstance(parsed, expr.Command):
            result.add_error(f"{parsed.name} statement is not allowed", False, 0.9)
        elif isinstance(parsed, (expr.Delete, expr.Insert, expr.Update, expr.Create)):
            result.add_error(
                f"{parsed.key.upper()} statement is not allowed", False, 0.9
            )
        elif isinstance(parsed, expr.Query):
            _verify_functions(parsed, result)
            _verify_query_statement(parsed, result)
        else:
            result.add_error("Could not find a query statement", False, 0.7)
    if result.can_fix and len(result.errors) > 0:
        result.fixed = parsed.sql(dialect=dialect)
    return {
        "allowed": len(result.errors) == 0,
        "errors": result.errors,
        "fixed": result.fixed,
        "risk": result.risk,
    }


# Functions that read files, run commands, sleep or otherwise reach outside the query.
_DEFAULT_BLOCKED_FUNCTIONS = frozenset(
    {
        "pg_read_file",
        "pg_read_binary_file",
        "pg_ls_dir",
        "pg_stat_file",
        "pg_sleep",
        "pg_sleep_for",
        "pg_sleep_until",
        "pg_terminate_backend",
        "pg_cancel_backend",
        "lo_import",
        "lo_export",
        "dblink",
        "dblink_exec",
        "load_extension",
        "readfile",
        "writefile",
        "load_file",
        "sleep",
        "benchmark",
        "xp_cmdshell",
        "read_csv",
        "read_csv_auto",
        "read_parquet",
        "read_json",
        "read_json_auto",
        "read_text",
        "read_blob",
        "glob",
    }
)


def _verify_functions(query: expr.Expression, context: VerificationContext):
    """Rejects calls to blocked functions (defaults plus config["blocked_functions"])."""
    blocked = _DEFAULT_BLOCKED_FUNCTIONS | {
        f.lower() for f in context.config.get("blocked_functions", [])
    }
    for func in query.find_all(expr.Func):
        name = (func.name if isinstance(func, expr.Anonymous) else func.sql_name()).lower()
        if name in blocked:
            context.add_error(f"Function {name} is not allowed", False, 0.9)


def _verify_where_clause(
    context: VerificationContext,
    select_statement: expr.Query,
    from_tables: List[expr.Table],
):
    where_clause = get_where(select_statement)
    if where_clause:
        for sub in where_clause.find_all(expr.Subquery, expr.Exists):
            _verify_query_statement(sub.this, context)
    _verify_static_expression(select_statement, context)
    verify_restrictions(select_statement, context, from_tables)


def _verify_static_expression(
    select_statement: expr.Query, context: VerificationContext
) -> bool:
    has_static_exp = False
    where_clause = get_where(select_statement)
    if where_clause:
        and_exps = list(split_to_expressions(where_clause.this, expr.And))
        for e in and_exps:
            if _has_static_expression(context, e):
                has_static_exp = True
    if has_static_exp:
        simplify(where_clause)
    return not has_static_exp


def _has_static_expression(context: VerificationContext, exp: expr.Expression) -> bool:
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
            context.add_error(
                f"Static expression is not allowed: {sub_exp.sql()}", True, 0.8
            )
            par = sub_exp.parent
            while isinstance(par, expr.Paren):
                par = par.parent
            if isinstance(par, expr.Or):
                to_replace.append(sub_exp)
            result = True
    for e in to_replace:
        e.replace(expr.Boolean(this=False))
    return result


def _verify_query_statement(query_statement: expr.Query, context: VerificationContext):
    if isinstance(query_statement, expr.Union):
        _verify_query_statement(query_statement.left, context)
        _verify_query_statement(query_statement.right, context)
        return
    for cte in query_statement.ctes:
        context.cte_names.add(cte.alias_or_name)
        _verify_query_statement(cte.this, context)
        # registered only after the body is verified, so a body cannot authorize its own columns
        _add_table_alias(cte, context)
    from_tables = _verify_from_tables(context, query_statement)
    try:
        if context.can_fix:
            _verify_select_clause(context, query_statement, from_tables)
            _verify_other_clauses_columns(context, query_statement, from_tables)
            _verify_where_clause(context, query_statement, from_tables)
            _verify_sub_queries(context, query_statement)
    finally:
        context.pop_sources()


def _verify_from_tables(context, query_statement):
    sources = set()
    from_tables = _get_from_clause_tables(query_statement, context, sources)
    for t in from_tables:
        if not t.db and t.name in context.cte_names:
            sources.add(t.name)
            if t.alias:  # CTE referenced through an alias: the alias exposes the same columns
                context.dynamic_tables[t.alias] = context.dynamic_tables.get(t.name, set())
                sources.add(t.alias)
    context.push_sources(sources)  # popped by _verify_query_statement
    for t in from_tables:
        found = False
        for config_t in context.config["tables"]:
            # Only CTE names can be referenced as tables. Subquery aliases
            # live in the same namespace for columns, not for table lookups.
            if table_matches(t, config_t) or (
                not t.db and t.name in context.cte_names
            ):
                found = True
        if not found:
            context.add_error(f"Table {t.name} is not allowed", False, 1)
    return from_tables


def _verify_sub_queries(context, query_statement):
    for exp_type in [expr.Order, expr.Offset, expr.Limit, expr.Group, expr.Having]:
        for exp in find_direct(query_statement, exp_type):
            if exp:
                for sub in exp.find_all(expr.Subquery):
                    _verify_query_statement(sub.this, context)


def _verify_select_clause(
    context: VerificationContext,
    select_clause: expr.Query,
    from_tables: List[expr.Table],
):
    for select in select_clause.selects:
        for sub in select.find_all(expr.Subquery):
            _verify_query_statement(sub.this, context)
            _add_table_alias(sub, context)
    to_remove = []
    for e in select_clause.expressions:
        if not _verify_select_clause_element(from_tables, context, e):
            to_remove.append(e)
    for e in to_remove:
        select_clause.expressions.remove(e)
    if len(select_clause.expressions) == 0:
        context.add_error("No legal elements in SELECT clause", False, 0.5)


def _verify_select_clause_element(
    from_tables: List[expr.Table], context: VerificationContext, e: expr.Expression
):
    if isinstance(e, expr.Column):
        if not _verify_col(e, from_tables, context):
            return False
    elif isinstance(e, expr.Star):
        context.add_error("SELECT * is not allowed", True, 0.1)
        for t in from_tables:
            for config_t in context.config["tables"]:
                if t.name == config_t["table_name"]:
                    for c in config_t["columns"]:
                        e.parent.set(
                            "expressions", e.parent.expressions + [sqlglot.parse_one(c)]
                        )
        return False
    elif isinstance(e, expr.Tuple):
        result = True
        for e in e.expressions:
            if not _verify_select_clause_element(from_tables, context, e):
                result = False
        return result
    else:
        for func_args in e.find_all(expr.Column):
            # columns of nested queries are verified together with those queries
            if func_args.parent_select is not e.parent_select:
                continue
            if not _verify_select_clause_element(from_tables, context, func_args):
                return False
    return True


def _verify_col(
    col: expr.Column,
    from_tables: List[expr.Table],
    context: VerificationContext,
    in_select: bool = True,
) -> bool:
    """
    Verifies if a column reference is allowed based on the provided tables and context.

    Args:
        col (Column): The column reference to verify.
        from_tables (List[_TableRef]): The list of tables to search within.
        context (VerificationContext): The context for verification.

    Returns:
        bool: True if the column reference is allowed, False otherwise.
    """
    if col.table:
        # A qualifier that names a table in this FROM clause is resolved strictly
        # against that table, even if a dynamic table shares the same name.
        for t in from_tables:
            if t.alias_or_name == col.table and t.name not in context.cte_names:
                if any(
                    col.name in config_t["columns"]
                    for config_t in context.config["tables"]
                    if config_t["table_name"] == t.name
                ):
                    return True
                return _reject_col(col, context, in_select)
    visible = context.visible_dynamic_tables
    if (
        (col.table != "" and col.table in visible)
        or (
            col.table == ""
            and col.name in [c for t_cols in visible.values() for c in t_cols]
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
    return _reject_col(col, context, in_select)


def _reject_col(
    col: expr.Column, context: VerificationContext, in_select: bool
) -> bool:
    if in_select:
        context.add_error(
            f"Column {col.name} is not allowed. Column removed from SELECT clause",
            True,
            0.3,
        )
    else:
        # A disallowed column outside SELECT (WHERE, JOIN, GROUP BY...) cannot be
        # safely removed, so the query is rejected.
        context.add_error(f"Column {col.name} is not allowed", False, 0.8)
    return False


def _verify_other_clauses_columns(
    context: VerificationContext,
    query_statement: expr.Query,
    from_tables: List[expr.Table],
):
    """Verifies columns used in WHERE, JOIN ON, GROUP BY, HAVING, ORDER BY and QUALIFY."""
    clauses = [
        query_statement.args.get(k)
        for k in ("where", "group", "having", "order", "qualify")
    ]
    clauses += [j.args.get("on") for j in query_statement.args.get("joins") or []]
    select_aliases = {s.alias for s in query_statement.selects if s.alias}
    for clause in clauses:
        if clause is None:
            continue
        for col in clause.find_all(expr.Column):
            # columns of nested queries are verified together with those queries
            if col.parent_select is not query_statement:
                continue
            if col.table == "" and col.name in select_aliases:
                continue
            _verify_col(col, from_tables, context, in_select=False)


def _get_from_clause_tables(
    select_clause: expr.Query, context: VerificationContext, sources: set
) -> List[expr.Table]:
    """
    Extracts table references from the FROM clause of an SQL query.

    Args:
        select_clause (dict): The FROM clause of the SQL query.
        context (VerificationContext): The context for verification.
        sources (set): Filled with the names of dynamic tables (subquery/lateral/unnest aliases).

    Returns:
        List[_TableRef]: A list of table references to find in the FROM clause.
    """
    result = []
    from_clause = get_from(select_clause)
    join_clauses = select_clause.args.get("joins", [])
    for clause in [from_clause] + join_clauses:
        if clause:
            for t in find_direct(clause, expr.Table):
                if isinstance(t, expr.Table):
                    result.append(t)
            for l in find_direct(clause, expr.Subquery):
                _verify_query_statement(l.this, context)
                _add_table_alias(l, context, sources)
    for join_clause in join_clauses:
        for l in find_direct(join_clause, expr.Lateral):
            _verify_query_statement(l.this.find(expr.Select), context)
            _add_table_alias(l, context, sources)
        for u in find_direct(join_clause, expr.Unnest):
            _add_table_alias(u, context, sources)
    return result


def _add_table_alias(
    exp: expr.Expression, context: VerificationContext, sources: set = None
):
    if sources is not None and not list(find_direct(exp, expr.TableAlias)):
        # derived table without an alias: still a source of columns for its query
        if isinstance(exp, expr.Subquery) and hasattr(exp.this, "named_selects"):
            name = f"__unnamed_{id(exp)}"
            context.dynamic_tables[name] = set(exp.this.named_selects)
            sources.add(name)
    for table_alias in find_direct(exp, expr.TableAlias):
        if isinstance(table_alias, expr.TableAlias):
            if len(table_alias.columns) > 0:
                column_names = {col.alias_or_name for col in table_alias.columns}
            else:
                column_names = {c for c in exp.this.named_selects}
            context.dynamic_tables[table_alias.alias_or_name] = column_names
            if sources is not None:
                sources.add(table_alias.alias_or_name)
