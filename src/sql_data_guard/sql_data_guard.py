import logging
from typing import List, Optional

import sqlglot
import sqlglot.expressions as expr
from sqlglot.optimizer.simplify import simplify

from .metadata_statements import (
    Classified,
    classify_describe,
    classify_show,
    starts_with_show,
)
from .column_masking import find_mask, masked_select_item, mask_value, table_masks
from .restriction_validation import validate_restrictions, UnsupportedRestrictionError
from .restriction_verification import verify_restrictions
from .verification_context import VerificationContext
from .verification_utils import (
    split_to_expressions,
    find_direct,
    table_matches,
    column_allowed,
    WILDCARD,
    same_name,
    get_where,
    get_from,
)

_DEFAULT_MAX_LENGTH = 10_000

logger = logging.getLogger(__name__)


def verify_sql(sql: str, config: dict, dialect: Optional[str] = None) -> dict:
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
    try:
        return _verify_sql(sql, config, dialect)
    except Exception as e:  # noqa: BLE001 - fail closed on anything unexpected
        # Log the exception type only: messages may contain parts of the SQL.
        logger.error(f"Unexpected error while verifying SQL ({type(e).__name__})")
        return {
            "allowed": False,
            "errors": [f"Internal error while verifying SQL ({type(e).__name__})"],
            "fixed": None,
            "risk": 1.0,
        }


def validate_config(config: dict) -> List[str]:
    """
    Validates a configuration without verifying a query.

    Returns the problems found (an empty list when the configuration is valid). verify_sql
    reports the same problems as a rejection, so this tells a bad configuration apart from a
    blocked query. Validation stops at the first problem, so the list has at most one entry
    for now. Never raises.
    """
    try:
        if not config or not isinstance(config, dict) or "tables" not in config:
            return [
                "Invalid configuration provided. The configuration must include 'tables'."
            ]
        validate_restrictions(config)
    except (UnsupportedRestrictionError, ValueError) as e:
        return [str(e)]
    except Exception as e:  # noqa: BLE001 - a malformed config must not raise
        return [f"Invalid configuration ({type(e).__name__})"]
    return []


def _effective_config(config: dict) -> dict:
    """
    The configuration as the verification uses it: a copy in which each table has the top-level
    catalog_name unless it sets its own. The caller's configuration is not modified.
    """
    catalog = config.get("catalog_name")
    if not catalog:
        return config
    return {
        **config,
        "tables": [
            t if t.get("catalog_name") else {**t, "catalog_name": catalog}
            for t in config["tables"]
        ],
    }


def _verify_sql(
    sql: str,
    config: dict,
    dialect: Optional[str] = None,
    violations: Optional[List[dict]] = None,
) -> dict:
    """
    The implementation of verify_sql. If a list is passed as `violations`, the table and column
    allow-list violations found are appended to it (see VerificationContext.add_violation).
    """
    if not isinstance(sql, str):
        return {
            "allowed": False,
            "errors": ["SQL must be a string."],
            "fixed": None,
            "risk": 1.0,
        }
    if dialect is not None:
        try:
            sqlglot.Dialect.get_or_raise(dialect)
        except ValueError as e:
            return {"allowed": False, "errors": [str(e)], "fixed": None, "risk": 1.0}
    config_errors = validate_config(config)
    if config_errors:
        return {"allowed": False, "errors": config_errors, "fixed": None, "risk": 1.0}
    config = _effective_config(config)

    max_length = config.get("max_length", _DEFAULT_MAX_LENGTH)
    if len(sql) > max_length:
        return {
            "allowed": False,
            "errors": [f"SQL exceeds maximum length of {max_length} characters."],
            "fixed": None,
            "risk": 1.0,
        }

    result = VerificationContext(config, dialect)
    if config.get("allow_metadata") and starts_with_show(sql, dialect):
        # SHOW is matched on its tokens: most dialects do not parse it
        _verify_metadata(classify_show(sql, dialect), result)
        return _result(result, violations)
    parsed = None
    try:
        statements = [s for s in sqlglot.parse(sql, dialect=dialect) if s is not None]
    except sqlglot.errors.SqlglotError as e:
        # Do not log the SQL itself: it may contain sensitive literals.
        logger.error(f"Error parsing SQL ({type(e).__name__}, length {len(sql)})")
        result.add_error(f"Error parsing sql: {e}", False, 0.9)
    else:
        if len(statements) == 0:
            result.add_error("Error parsing sql: no statement found", False, 0.9)
        elif len(statements) > 1:
            result.add_error("Multiple statements are not allowed", False, 1)
        else:
            parsed = statements[0]
    if parsed and config.get("allow_metadata") and isinstance(parsed, expr.Describe):
        _verify_metadata(classify_describe(parsed), result)
        return _result(result, violations)
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
    if isinstance(parsed, expr.Query) and result.can_fix:
        _enforce_force_limit(parsed, result)
    if parsed is not None and result.can_fix and len(result.errors) > 0:
        result.fixed = parsed.sql(dialect=dialect)
    return _result(result, violations)


def _result(context: VerificationContext, violations: Optional[List[dict]]) -> dict:
    if violations is not None:
        violations.extend(context.violations)
    return {
        "allowed": len(context.errors) == 0,
        "errors": context.errors,
        "fixed": context.fixed,
        "risk": context.risk,
    }


def _verify_metadata(statement: Classified, context: VerificationContext):
    """
    Verifies a SHOW / DESCRIBE statement (config["allow_metadata"]). A statement about a table
    needs a configured table, with the same checks as a table in FROM; one about a database or a
    catalog follows `databases`, `strict_database` and `catalog_name`. Never fixable.
    """
    if isinstance(statement, str):
        context.add_error(f"{statement} is not a supported metadata statement", False, 0.9)
        return
    config = context.config
    if statement.table is not None:
        _verify_table_reference(statement.table, context)
        return
    if statement.catalog is not None and not _catalog_allowed(statement.catalog, config):
        context.add_error(f"Catalog {statement.catalog} is not allowed", False, 1)
        return
    if statement.database is None:
        if statement.form in ("SHOW TABLES", "SHOW VIEWS") and config.get("strict_database"):
            context.add_error(
                f"{statement.form} must name its database (strict_database)", False, 0.7
            )
        return
    if not _database_allowed(statement.database, config):
        context.add_error(f"Database {statement.database} is not allowed", False, 1)
        context.add_violation(
            {"type": "database", "table_name": None, "database_name": statement.database}
        )


def _database_allowed(database: str, config: dict) -> bool:
    """
    A database named by a metadata statement: listed in `databases`, or without that list, the
    database of a configured table (any database when no table has a database_name).
    """
    if "databases" in config:
        return database.lower() in {d.lower() for d in config["databases"]}
    known = {t["database_name"].lower() for t in config["tables"] if t.get("database_name")}
    return not known or database.lower() in known


def _catalog_allowed(catalog: str, config: dict) -> bool:
    """The configured catalogs (top level and per table); any catalog when none is configured."""
    known = {t["catalog_name"].lower() for t in config["tables"] if t.get("catalog_name")}
    if config.get("catalog_name"):
        known.add(config["catalog_name"].lower())
    return not known or catalog.lower() in known


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
        "set_config",
        "getenv",
        "randomblob",
        "zeroblob",
        "version",
        "current_version",  # sqlglot maps version()/sqlite_version() to this
        "current_setting",
        "sqlite_version",
        "sqlite_source_id",
    }
)

# Package/schema prefixes (Oracle, SQL Server...) whose functions are never allowed.
_BLOCKED_FUNCTION_PREFIXES = ("utl_", "dbms_", "sys.", "xp_", "sp_")


# sqlglot models these as functions, but they are syntax rather than calls to a named function.
# AND / OR / XOR (Connector) are functions in newer sqlglot versions (XOR already in 25.x).
_SYNTAX_FUNCTIONS = (
    expr.Case,
    expr.Cast,
    expr.TryCast,
    expr.Exists,
    expr.Array,
    expr.Connector,
    expr.Collate,
)


def _is_syntax(func: expr.Func) -> bool:
    # the WHEN branches of a CASE are If nodes; a stand-alone IF(a, b, c) is a real function
    return isinstance(func, _SYNTAX_FUNCTIONS) or (
        isinstance(func, expr.If) and isinstance(func.parent, expr.Case)
    )


def _function_names(func: expr.Func):
    """
    The names a function call can be listed under in the configuration.

    Returns (display_name, names): `names` are lower case. For a call qualified with a schema or
    package (`pkg.fn(...)`) they are the qualified names (`pkg.fn`), so an unknown schema cannot
    borrow the permission of a bare name. sqlglot normalises names (SUBSTR -> SUBSTRING), so every
    spelling registered for the node class is included; the display name is the one to put in the
    configuration.
    """
    primary = (func.name if isinstance(func, expr.Anonymous) else func.sql_name()).lower()
    names = {primary}
    if not isinstance(func, expr.Anonymous):
        names |= {n.lower() for n in type(func).sql_names()}
    if isinstance(func.parent, expr.Dot) and func.parent.expression is func:
        qualifier = func.parent.this.sql().lower()
        return f"{qualifier}.{primary}", {f"{qualifier}.{n}" for n in names}
    return primary, names


def _verify_functions(query: expr.Expression, context: VerificationContext):
    """
    Rejects calls to blocked functions (built-in list plus config["blocked_functions"]) and, when
    config["allowed_functions"] is set, to any function that is not in that list.
    """
    blocked = _DEFAULT_BLOCKED_FUNCTIONS | {
        f.lower() for f in context.config.get("blocked_functions") or []
    }
    allowed_config = context.config.get("allowed_functions")
    allowed = {f.lower() for f in allowed_config} if allowed_config is not None else None
    for func in query.find_all(expr.Func):
        if _is_syntax(func):
            continue
        display, names = _function_names(func)
        # a blocked function is blocked whatever schema it is called through
        bare_names = {n.rsplit(".", 1)[-1] for n in names}
        if bare_names & blocked or any(
            n.startswith(_BLOCKED_FUNCTION_PREFIXES) for n in names
        ):
            context.add_error(f"Function {display} is not allowed", False, 0.9)
        elif allowed is not None and not names & allowed:
            context.add_error(
                f"Function {display} is not in the allowed functions list", False, 0.9
            )
    # system variables such as @@version
    for param in query.find_all(expr.SessionParameter, expr.Parameter):
        if param.sql().startswith("@@"):
            context.add_error(f"System variable {param.sql()} is not allowed", False, 0.7)


def _literal_int(limit: Optional[expr.Expression]) -> Optional[int]:
    """The row count of a plain `LIMIT <integer literal>`, else None (ALL, NULL, -1, expressions, FETCH ...)."""
    if isinstance(limit, expr.Limit):
        count = limit.expression
        if isinstance(count, expr.Literal) and count.is_int:
            return int(count.name)
    return None


def _enforce_force_limit(query: expr.Query, context: VerificationContext):
    """
    Caps the number of rows of the outermost query when the config sets `force_limit`.

    No LIMIT is added, a larger one is clamped, and anything that is not a plain positive integer
    literal (`LIMIT ALL`, `LIMIT NULL`, `LIMIT -1`, an expression, FETCH FIRST...) is replaced, as
    several databases treat those as "no limit". Only the outermost query is changed. OFFSET is kept.
    """
    cap = context.config.get("force_limit")
    if cap is None:
        return
    current = _literal_int(query.args.get("limit"))
    if current is not None and current <= cap:
        return
    query.set("limit", expr.Limit(expression=expr.Literal.number(cap)))
    context.add_error(f"Row limit enforced: LIMIT {cap}", True, 0.3)


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
        # simplify() rewrites the WHERE clause in place; its return value is not needed
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
    if not isinstance(query_statement, expr.Query):
        # e.g. `FROM (payroll)` or `FROM (a JOIN b ON ...)`: sqlglot parses the parenthesized
        # table as the body of a sub-query.
        if isinstance(query_statement, expr.Table):
            context.add_error("Parenthesized table references are not supported", False, 0.7)
        else:
            context.add_error("Unsupported nested query", False, 0.7)
        return
    context.push_cte_scope()
    try:
        _verify_ctes(query_statement, context)
        if isinstance(query_statement, expr.Union):
            _verify_query_statement(query_statement.left, context)
            _verify_query_statement(query_statement.right, context)
            return
        if query_statement.args.get("into"):
            context.add_error("SELECT INTO is not allowed", False, 0.9)
        if query_statement.args.get("locks"):
            context.add_error(
                "Locking clauses (FOR UPDATE / FOR SHARE) are not allowed", False, 0.7
            )
        from_tables = _verify_from_tables(context, query_statement)
        context.push_tables(from_tables)
        try:
            if context.can_fix:
                _verify_select_clause(context, query_statement, from_tables)
                _verify_other_clauses_columns(context, query_statement, from_tables)
                _verify_where_clause(context, query_statement, from_tables)
                _verify_sub_queries(context, query_statement)
        finally:
            context.pop_tables()
            context.pop_sources()
    finally:
        context.pop_cte_scope()


def _verify_ctes(query_statement: expr.Query, context: VerificationContext):
    """Verifies the WITH clause of a query (a SELECT or a set operation) and scopes its names."""
    with_clause = query_statement.args.get("with") or query_statement.args.get("with_")
    recursive = bool(with_clause and with_clause.args.get("recursive"))
    for cte in query_statement.ctes:
        name = cte.alias_or_name
        if not isinstance(cte.this, expr.Query):
            context.add_error(
                f"{cte.this.key.upper()} statement is not allowed in a WITH clause", False, 0.9
            )
            continue
        if recursive:
            # only a recursive CTE can refer to itself; otherwise the name in its body is a real table.
            # Its columns are the declared ones, or those of the anchor (left-most) SELECT, which is
            # verified on its own, so this does not let the body authorize columns.
            context.add_cte_name(name)
            anchor = cte.this
            while isinstance(anchor, expr.Union):
                anchor = anchor.left
            declared = {c.alias_or_name for c in cte.args["alias"].columns} if cte.args.get("alias") else set()
            context.dynamic_tables[name] = declared or set(getattr(anchor, "named_selects", []))
        _verify_query_statement(cte.this, context)
        context.add_cte_name(name)
        # registered only after the body is verified, so a body cannot authorize its own columns
        _add_table_alias(cte, context)


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
        is_cte = not t.db and t.name in context.cte_names
        if not is_cte:
            _verify_table_reference(t, context)
    return from_tables


def _verify_table_reference(t: expr.Table, context: VerificationContext) -> bool:
    """
    Checks a reference to a real table (not a CTE): its name, database and catalog must match a
    configured table. Records the error (and the violation) and returns False if not.
    """
    if len(t.parts) > 3:
        # `a.b.c.orders`: catalog/db/name are only three of the parts, so the configuration
        # cannot be matched against the table the database would really read
        context.add_error(
            f"Table {'.'.join(p.name for p in t.parts)} is not allowed: "
            "names with more than three parts are not supported",
            False,
            1,
        )
        return False
    if not _verify_table_database(t, context):
        return False
    if any(table_matches(t, config_t) for config_t in context.config["tables"]):
        return True
    if _catalog_mismatch(t, context):
        context.add_error(
            f"Table {t.name} is not allowed: catalog {t.catalog} is not allowed", False, 1
        )
        context.add_violation(
            {"type": "catalog", "table_name": t.name, "catalog_name": t.catalog}
        )
        return False
    context.add_error(f"Table {t.name} is not allowed", False, 1)
    context.add_violation(
        {"type": "table", "table_name": t.name, "database_name": t.db or None}
    )
    return False


def _verify_table_database(table: expr.Table, context: VerificationContext) -> bool:
    """
    The database checks of a real table reference: strict mode needs a qualifier, and a
    qualifier must be in the `databases` allow-list when there is one. False if rejected.
    """
    config = context.config
    if not table.db:
        if config.get("strict_database"):
            context.add_error(
                f"Table {table.name} must be qualified with its database", False, 0.7
            )
            return False
        return True
    databases = config.get("databases")
    if databases is not None and table.db.lower() not in {d.lower() for d in databases}:
        context.add_error(f"Database {table.db} is not allowed", False, 1)
        context.add_violation(
            {"type": "database", "table_name": table.name, "database_name": table.db}
        )
        # the table itself is checked too, so a suggestion can add both
    return True


def _catalog_mismatch(table: expr.Table, context: VerificationContext) -> bool:
    """
    True if a reference is rejected only because of its catalog: the configuration has the
    table (name and database) in another catalog, or, for a table it does not have, its default
    catalog_name is another one. Widening the allow-list cannot help such a query.
    """
    if not table.catalog:
        return False
    same_table = [
        c
        for c in context.config["tables"]
        if table_matches(table, {**c, "catalog_name": None})
    ]
    if same_table:
        return True  # it matches by name and database, so the catalog is what differs
    default = context.config.get("catalog_name")
    return default is not None and default.lower() != table.catalog.lower()


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
        _apply_mask(e, from_tables, context)
    elif isinstance(e, expr.Star):
        if _star_allowed(from_tables, context):
            return True  # table_level_access: every source is approved as a whole
        context.add_error("SELECT * is not allowed", True, 0.1)
        parent = e.parent
        # With a table-level table in the query, `*` is replaced table by table: `t.*` for a
        # table approved as a whole, qualified columns for the others (the result stays unambiguous).
        mixed = any(
            "columns" not in config_t and table_matches(t, config_t)
            for t in from_tables
            for config_t in context.config["tables"]
        )
        for t in from_tables:
            if not t.db and t.name in context.cte_names:
                continue  # a CTE, even one named like a configured table
            for config_t in context.config["tables"]:
                if table_matches(t, config_t) and parent is not None:
                    qualifier = t.alias_or_name if mixed else ""
                    if "columns" not in config_t:
                        items = [expr.Column(this=expr.Star(), table=expr.to_identifier(qualifier))]
                    else:
                        masks = table_masks(config_t)
                        items = []
                        for c in config_t["columns"]:
                            # a masked column is expanded to its mask, not to the real column
                            if c.lower() in masks:
                                items.append(masked_select_item(masks[c.lower()], c))
                            elif mixed:
                                items.append(expr.column(c, table=qualifier))
                            else:
                                items.append(sqlglot.parse_one(c))
                    parent.set("expressions", parent.expressions + items)
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


def _star_allowed(from_tables: List[expr.Table], context: VerificationContext) -> bool:
    """
    True if `*` can be kept as written: every source of the query is approved as a whole. That is
    a configured table without `columns` (only possible with `table_level_access`), or a CTE /
    derived table whose own SELECT kept a `*` for such tables. With any other source, a column
    list is needed to expand `*` and it is rewritten to the allowed columns.
    """
    sources = context.current_sources
    real = [t for t in from_tables if t.db or t.name not in context.cte_names]
    if not real and not sources:
        return False
    for t in real:
        matching = [c for c in context.config["tables"] if table_matches(t, c)]
        if not matching or any("columns" in c for c in matching):
            return False
    dynamic = context.dynamic_tables
    return all(WILDCARD in dynamic.get(name, ()) for name in sources)


def _apply_mask(col: expr.Column, from_tables: List[expr.Table], context: VerificationContext):
    """
    Masks a selected column. A column that is a direct item of the SELECT list (`ssn` or
    `ssn AS x`) is rewritten to its mask; a masked column used any other way (inside a function
    or expression) cannot be masked and is rejected.
    """
    mask = find_mask(col, context.table_scopes or [from_tables], context.config)
    if mask is None:
        return
    parent = col.parent
    if isinstance(parent, expr.Select):
        col.replace(masked_select_item(mask, col.name))
    elif isinstance(parent, expr.Alias) and isinstance(parent.parent, expr.Select):
        col.replace(mask_value(mask))
    else:
        _reject_masked(col, context)
        return
    context.add_error(f"Column {col.name} is masked", True, 0.1)


def _reject_masked(col: expr.Column, context: VerificationContext):
    context.add_error(
        f"Column {col.name} is masked and can only be selected directly", False, 0.8
    )


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
        # A qualifier that names a table is resolved strictly against that table, even if a dynamic
        # table shares the name. The innermost query that has such a table wins: a correlated
        # sub-query can refer to a table of an enclosing query (`o.notes`), and the column must be
        # allowed on *that* table, not on whichever table the sub-query happens to select from.
        for scope in context.table_scopes or [from_tables]:
            matches = [
                t
                for t in scope
                if same_name(t.alias_or_name, col.table) and t.name not in context.cte_names
            ]
            if matches:
                if any(
                    column_allowed(col, config_t)
                    for t in matches
                    for config_t in context.config["tables"]
                    if table_matches(t, config_t)
                ):
                    return True
                return _reject_col(col, context, in_select, table=matches[0])
    visible = context.visible_dynamic_tables
    if col.table != "":
        # a qualifier naming a CTE / derived table: the column must be one it exposes (a column
        # removed from its SELECT list, or never in it, does not exist there)
        dynamic = [c for n, c in visible.items() if same_name(n, col.table)]
        if dynamic:
            if col.is_star or any(
                c == WILDCARD or same_name(col.name, c) for cols in dynamic for c in cols
            ):
                return True
            return _reject_col(col, context, in_select, from_tables=from_tables)
    if (
        (
            col.table == ""
            and (
                col.name.lower() in {c.lower() for t_cols in visible.values() for c in t_cols}
                or any(WILDCARD in t_cols for t_cols in visible.values())
            )
        )
        or (
            any(
                column_allowed(col, config_t)
                for config_t in context.config["tables"]
                for t in from_tables
                if table_matches(t, config_t)
            )
        )
    ):
        return True
    return _reject_col(col, context, in_select, from_tables=from_tables)


def _column_owner(
    col: expr.Column,
    context: VerificationContext,
    table: Optional[expr.Table],
    from_tables: Optional[List[expr.Table]],
) -> Optional[expr.Table]:
    """
    The real table a column reference belongs to, or None if that cannot be told (an unknown
    qualifier, or an unqualified column in a query over several tables).
    """
    if table is not None:
        return table
    scopes = context.table_scopes or [from_tables or []]
    if col.table:
        for scope in scopes:
            for t in scope:
                if same_name(t.alias_or_name, col.table) and t.name not in context.cte_names:
                    return t
        return None
    real = [t for t in scopes[0] if t.db or t.name not in context.cte_names]
    return real[0] if len(real) == 1 else None


def _reject_col(
    col: expr.Column,
    context: VerificationContext,
    in_select: bool,
    table: Optional[expr.Table] = None,
    from_tables: Optional[List[expr.Table]] = None,
) -> bool:
    owner = _column_owner(col, context, table, from_tables)
    context.add_violation(
        {
            "type": "column",
            "column": col.name,
            "table_name": owner.name if owner is not None else None,
            "database_name": (owner.db or None) if owner is not None else None,
        }
    )
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
    for join in query_statement.args.get("joins") or []:
        for ident in join.args.get("using") or []:
            _verify_using_column(ident, join, from_tables, context)
    select_aliases = {s.alias for s in query_statement.selects if s.alias}
    for clause in clauses:
        if clause is None:
            continue
        for col in clause.find_all(expr.Column):
            # columns of nested queries are verified together with those queries
            if col.parent_select is not query_statement:
                continue
            if col.table == "" and col.name.lower() in {a.lower() for a in select_aliases}:
                continue
            _verify_col(col, from_tables, context, in_select=False)
            if find_mask(col, context.table_scopes or [from_tables], context.config):
                _reject_masked(col, context)


def _verify_using_column(
    ident: expr.Expression,
    join: expr.Join,
    from_tables: List[expr.Table],
    context: VerificationContext,
):
    """JOIN ... USING (col) compares col on both sides, so it must be allowed on the joined table too."""
    name = ident.name
    right = join.this
    if isinstance(right, expr.Table):
        configured = [c for c in context.config["tables"] if table_matches(right, c)]
        if configured and not any(
            column_allowed(expr.column(name), c) for c in configured
        ):
            _reject_col(expr.column(name), context, False, table=right)
            return
    _verify_col(expr.column(name), from_tables, context, in_select=False)
    if find_mask(expr.column(name), context.table_scopes or [from_tables], context.config):
        _reject_masked(expr.column(name), context)


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
    clauses = [c for c in [from_clause] + join_clauses if c]
    for clause in clauses:
        for t in find_direct(clause, expr.Table):
            if isinstance(t, expr.Table):
                result.append(t)
    # Derived tables and LATERAL bodies can refer to the other tables of this FROM clause
    # (LATERAL in PostgreSQL, MySQL, DuckDB...), so they are verified with those tables in scope.
    context.push_tables(result)
    try:
        for clause in clauses:
            for sub in find_direct(clause, expr.Subquery):
                _verify_query_statement(sub.this, context)
                _add_table_alias(sub, context, sources)
        for join_clause in join_clauses:
            for lateral in find_direct(join_clause, expr.Lateral):
                _verify_query_statement(lateral.this.find(expr.Select), context)
                _add_table_alias(lateral, context, sources)
            for unnest in find_direct(join_clause, expr.Unnest):
                _add_table_alias(unnest, context, sources)
    finally:
        context.pop_tables()
    return result


def _add_table_alias(
    exp: expr.Expression, context: VerificationContext, sources: Optional[set] = None
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
