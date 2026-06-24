import logging
from typing import List

import sqlglot
import sqlglot.expressions as expr
from sqlglot.optimizer.simplify import simplify

from .column_masking import build_mask_expression, validate_column_masks
from .injection_detection import (
    function_name,
    is_stacked_statement,
    scan_parsed_sql,
    scan_raw_sql,
)
from .restriction_validation import validate_restrictions, UnsupportedRestrictionError
from .restriction_verification import verify_restrictions
from .verification_context import VerificationContext
from .verification_utils import split_to_expressions, find_direct


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

    # First, validate restrictions and column masks
    try:
        validate_restrictions(config)
        validate_column_masks(config)
    except UnsupportedRestrictionError as e:
        return {"allowed": False, "errors": [str(e)], "fixed": None, "risk": 1.0}
    except ValueError as e:
        return {"allowed": False, "errors": [str(e)], "fixed": None, "risk": 1.0}

    result = VerificationContext(config, dialect)
    parsed = None
    # Pre-parse scan for attacks invisible to the AST (e.g. comment evasion).
    scan_raw_sql(sql, result)
    try:
        statements = [s for s in sqlglot.parse(sql, dialect=dialect) if s is not None]
    except sqlglot.errors.ParseError as e:
        logging.error(f"SQL: {sql}\nError parsing SQL: {e}")
        result.add_error(f"Error parsing sql: {e}", False, 0.9)
        statements = []
    if len(statements) > 1:
        # Reject stacked / multi-statement payloads explicitly rather than relying on
        # sqlglot incidentally wrapping them in a non-query node (S7). Uses the same
        # intent-revealing message as the injection_detection module's stacked scan.
        result.add_error(
            "Stacked query detected: multiple statements are not allowed", False, 0.9
        )
    elif len(statements) == 1:
        parsed = statements[0]
    elif len(result.errors) == 0:
        result.add_error("Could not find a query statement", False, 0.7)
    if parsed:
        # AST-based malicious-payload scan (stacked queries, dangerous
        # functions, system-catalog probing). Always on.
        scan_parsed_sql(parsed, result)
        # Config-driven function allow-list / deny-list (feature F2).
        _verify_functions(parsed, result)
        if isinstance(parsed, expr.Command):
            result.add_error(f"{parsed.name} statement is not allowed", False, 0.9)
        elif isinstance(parsed, (expr.Delete, expr.Insert, expr.Update, expr.Create)):
            result.add_error(
                f"{parsed.key.upper()} statement is not allowed", False, 0.9
            )
        elif isinstance(parsed, expr.Query):
            _verify_query_statement(parsed, result)
        elif is_stacked_statement(parsed):
            # Stacked statements are already reported by scan_parsed_sql with an
            # intent-revealing message; avoid the misleading generic error.
            pass
        else:
            result.add_error("Could not find a query statement", False, 0.7)

    if parsed is not None and isinstance(parsed, expr.Query) and result.can_fix:
        _enforce_force_limit(parsed, result)
    if result.can_fix and parsed is not None and len(result.errors) > 0:
        result.fixed = parsed.sql(dialect=dialect)

    allowed = len(result.errors) == 0
    fixed = result.fixed
    max_risk = config.get("max_risk")
    if max_risk is not None and result.risk > max_risk:
        # Risk exceeds the configured threshold: refuse to auto-fix and hard-block (S5).
        allowed = False
        fixed = None
    return {
        "allowed": allowed,
        "errors": result.errors,
        "fixed": fixed,
        "risk": result.risk,
    }


def _verify_functions(parsed: expr.Expression, context: VerificationContext):
    """Enforce a config-driven function allow-list / deny-list (feature F2).

    Two optional, top-level config keys (function names are matched
    case-insensitively):

    * ``blocked_functions`` -- any call to one of these functions is blocked.
    * ``allowed_functions`` -- if present, *only* these functions may be called;
      any other function is blocked.

    A violation is a hard block (not auto-fixable): stripping a function call
    from an arbitrary position could silently change query semantics or produce
    invalid SQL, so the query is rejected outright. This composes with the
    always-on dangerous-function deny-list in :mod:`injection_detection`.
    """
    blocked = context.config.get("blocked_functions")
    allowed = context.config.get("allowed_functions")
    if not blocked and not allowed:
        return
    blocked_lower = {f.lower() for f in blocked} if blocked else set()
    allowed_lower = {f.lower() for f in allowed} if allowed else None
    for func in parsed.find_all(expr.Func):
        name = function_name(func)
        if not name:
            continue
        lname = name.lower()
        if lname in blocked_lower:
            context.add_error(f"Function {name} is not allowed", False, 0.9)
        elif allowed_lower is not None and lname not in allowed_lower:
            context.add_error(
                f"Function {name} is not in the allowed functions list",
                False,
                0.9,
            )


def _enforce_force_limit(parsed: expr.Query, context: VerificationContext):
    """Enforce a mandatory row cap on the outermost query (feature F3).

    If the config sets ``force_limit`` to a positive integer, the outermost
    query must not return more rows than that cap:

    * No ``LIMIT`` present -> inject ``LIMIT force_limit``.
    * ``LIMIT`` larger than the cap -> clamp it down to ``force_limit``.
    * ``LIMIT`` at or below the cap -> left unchanged.

    Only the outermost statement is touched (sub-queries/CTEs are not), because
    the cap protects the final result set returned to the caller. The rewrite is
    fixable, so it surfaces in the ``fixed`` query just like other auto-fixes.
    """
    force_limit = context.config.get("force_limit")
    if not isinstance(force_limit, int) or isinstance(force_limit, bool):
        return
    if force_limit <= 0:
        return
    limit = parsed.args.get("limit")
    if limit is not None:
        try:
            current = int(limit.expression.name)
        except (AttributeError, ValueError, TypeError):
            current = None
        if current is not None and current <= force_limit:
            return
        action = f"clamped from {current} to" if current is not None else "set to"
    else:
        action = "set to"
    parsed.set("limit", expr.Limit(expression=expr.Literal.number(force_limit)))
    context.add_error(f"Row limit enforced: LIMIT {action} {force_limit}", True, 0.3)


def _verify_where_clause(
    context: VerificationContext,
    select_statement: expr.Query,
    from_tables: List[expr.Table],
):
    where_clause = select_statement.find(expr.Where)
    if where_clause:
        for sub in where_clause.find_all(expr.Subquery, expr.Exists):
            _verify_query_statement(sub.this, context)
    _verify_static_expression(select_statement, context)
    verify_restrictions(select_statement, context, from_tables)


def _verify_static_expression(
    select_statement: expr.Query, context: VerificationContext
) -> bool:
    has_static_exp = False
    where_clause = select_statement.find(expr.Where)
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
    if isinstance(query_statement, expr.SetOperation):
        # Covers UNION, EXCEPT and INTERSECT (all share the SetOperation base).
        _verify_query_statement(query_statement.left, context)
        _verify_query_statement(query_statement.right, context)
        return
    for cte in query_statement.ctes:
        # Register early so recursive/forward CTE references resolve, then refresh
        # after verification to capture columns expanded from SELECT * (S3).
        _add_table_alias(cte, context)
        _verify_query_statement(cte.this, context)
        _add_table_alias(cte, context)
    from_tables = _verify_from_tables(context, query_statement)
    if context.can_fix:
        _verify_select_clause(context, query_statement, from_tables)
        _verify_where_clause(context, query_statement, from_tables)
        _verify_sub_queries(context, query_statement)


def _verify_from_tables(context, query_statement):
    from_tables = _get_from_clause_tables(query_statement, context)
    for t in from_tables:
        found = False
        for config_t in context.config["tables"]:
            if t.name == config_t["table_name"] or t.name in context.dynamic_tables:
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
    from_tables: List[expr.Table], context: VerificationContext, e: expr.Expression
):
    if isinstance(e, expr.Column) and e.name == "*":
        # Table-qualified wildcard (``t.*``) -- expand like ``*`` but scoped to
        # the named table (feature F10).
        _expand_star(e, from_tables, context, table_filter=e.table)
        return False
    elif isinstance(e, expr.Column):
        if not _verify_col(e, from_tables, context):
            return False
        _apply_column_mask(e, from_tables, context)
    elif isinstance(e, expr.Star):
        _expand_star(e, from_tables, context)
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


def _expand_star(
    e: expr.Expression,
    from_tables: List[expr.Table],
    context: VerificationContext,
    table_filter: str = "",
):
    """Replace a ``*`` / ``table.*`` wildcard with the allowed column list.

    Columns listed in a table's ``denied_columns`` are excluded from the
    expansion (feature F10), so ``SELECT *`` never silently surfaces a denied
    column. ``table_filter`` restricts expansion to a single table for the
    qualified ``table.*`` form.
    """
    context.add_error("SELECT * is not allowed", True, 0.1)
    for t in from_tables:
        if table_filter and table_filter not in (t.name, t.alias):
            continue
        for config_t in context.config["tables"]:
            if t.name == config_t["table_name"]:
                denied = set(config_t.get("denied_columns", []))
                for c in config_t["columns"]:
                    if c in denied:
                        continue
                    e.parent.set(
                        "expressions", e.parent.expressions + [sqlglot.parse_one(c)]
                    )


def _denied_columns(
    from_tables: List[expr.Table], context: VerificationContext
) -> set:
    """Union of ``denied_columns`` across the config tables in this query."""
    denied = set()
    for config_t in context.config["tables"]:
        if any(t.name == config_t["table_name"] for t in from_tables):
            denied.update(config_t.get("denied_columns", []))
    return denied


def _verify_col(
    col: expr.Column, from_tables: List[expr.Table], context: VerificationContext
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
    # A denied column (feature F10) is rejected even if it is otherwise
    # allow-listed: deny wins, and the column is stripped from the SELECT.
    if col.name in _denied_columns(from_tables, context):
        context.add_error(
            f"Column {col.name} is denied. Column should be removed from SELECT clause",
            True,
            0.3,
        )
        return False
    if (
        col.table == "sub_select"
        or (col.table != "" and col.table in context.dynamic_tables)
        or (
            # All FROM tables are dynamic: allow only columns those dynamic tables
            # actually expose, instead of blindly allowing everything (S3).
            len(from_tables) > 0
            and all(t.name in context.dynamic_tables for t in from_tables)
            and any(
                col.name in context.dynamic_tables.get(t.name, set())
                for t in from_tables
            )
        )
        or (
            col.table == ""
            and col.name
            in [c for t_cols in context.dynamic_tables.values() for c in t_cols]
        )
        or (col.table == "" and col.name in context.dynamic_columns)
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
    else:
        context.add_error(
            f"Column {col.name} is not allowed. Column removed from SELECT clause",
            True,
            0.3,
        )
        return False


def _apply_column_mask(
    col: expr.Column, from_tables: List[expr.Table], context: VerificationContext
):
    """Rewrite a SELECT column into its masking expression, if one is configured.

    The column is only masked when it can be unambiguously attributed to a single
    configured table that declares a mask for it. The rewrite preserves the output
    column name, so the query shape is unchanged.
    """
    if not context.column_masks:
        return

    masked_tables = [
        t
        for t in from_tables
        if t.name in context.column_masks and col.name in context.column_masks[t.name]
    ]
    if len(masked_tables) != 1:
        # No mask for this column, or ambiguous across multiple masked tables.
        return
    table = masked_tables[0]

    # If the column is table-qualified, it must refer to this table (by alias or name).
    if col.table and col.table not in (table.alias, table.name):
        return

    mask = context.column_masks[table.name][col.name]
    masked_expr = build_mask_expression(mask, col, context.dialect)
    col.replace(masked_expr)
    context.add_error(
        f"Column {col.name} is masked ({mask['policy']})",
        True,
        0.2,
    )


def _get_from_clause_tables(
    select_clause: expr.Query, context: VerificationContext
) -> List[expr.Table]:
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
            for l in find_direct(clause, expr.Subquery):
                # Verify (and expand SELECT *) before recording exposed columns so
                # that the alias maps to the real, allowed columns rather than "*".
                _verify_query_statement(l.this, context)
                _add_table_alias(l, context)
                _register_dynamic_columns(l.this, context)
    for join_clause in join_clauses:
        for l in find_direct(join_clause, expr.Lateral):
            _add_table_alias(l, context)
            _verify_query_statement(l.this.find(expr.Select), context)
        for u in find_direct(join_clause, expr.Unnest):
            _add_table_alias(u, context)
    return result


def _add_table_alias(exp: expr.Expression, context: VerificationContext):
    for table_alias in find_direct(exp, expr.TableAlias):
        if isinstance(table_alias, expr.TableAlias):
            if len(table_alias.columns) > 0:
                column_names = {col.alias_or_name for col in table_alias.columns}
            else:
                column_names = {c for c in exp.this.named_selects}
            context.dynamic_tables[table_alias.alias_or_name] = column_names


def _register_dynamic_columns(query: expr.Expression, context: VerificationContext):
    """
    Record the columns a (possibly un-aliased) dynamic source exposes, so the outer
    query may reference them un-prefixed without blindly allowing every column.
    """
    if query is None:
        return
    for name in query.named_selects:
        if name and name != "*":
            context.dynamic_columns.add(name)
