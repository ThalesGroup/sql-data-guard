from typing import Generator, Type

import sqlglot.expressions as expr


def split_to_expressions(
    exp: expr.Expression, exp_type: Type[expr.Expression]
) -> Generator[expr.Expression, None, None]:
    if isinstance(exp, exp_type):
        yield from exp.flatten()
    else:
        yield exp


def find_direct(exp: expr.Expression, exp_type: Type[expr.Expression]):
    for child in exp.args.values():
        if isinstance(child, exp_type):
            yield child


def names_equal(node: expr.Expression, config_name: str) -> bool:
    """
    Compares the name of a table/column node with a name from the configuration.

    Unquoted identifiers are case-insensitive in SQL; quoted ones are case-sensitive in most
    databases, so a quoted identifier must match the configuration exactly.
    """
    ident = node.this
    if isinstance(ident, expr.Identifier) and ident.quoted:
        return node.name == config_name
    return node.name.lower() == str(config_name).lower()


def same_name(a: str, b: str) -> bool:
    """Case-insensitive comparison of two (already unquoted) qualifiers or aliases."""
    return a.lower() == b.lower()


def column_in(col: expr.Column, columns) -> bool:
    """True if the column node is one of the configured column names."""
    return any(names_equal(col, c) for c in columns)


# Column name of a CTE / derived table whose SELECT kept a `*` (see `table_level_access`): every
# column of the tables it reads is approved, so any column may be referenced through it.
WILDCARD = "*"


def column_allowed(col: expr.Column, config_table: dict) -> bool:
    """
    True if the configured table allows the column. A table without `columns` is approved as a
    whole (only valid with `table_level_access`), so any column is allowed.
    """
    return "columns" not in config_table or column_in(col, config_table["columns"])


def table_matches(table: expr.Table, config_table: dict) -> bool:
    """
    Checks if a table reference in a query refers to a table in the configuration.

    The table name must match. If the query qualifies the table with a schema/database
    and the configuration defines a database_name, they must match too; the same for the
    catalog/project of a 3-part name (`catalog.schema.table`) and catalog_name.
    """
    if not names_equal(table, config_table["table_name"]):
        return False
    catalog_name = config_table.get("catalog_name")
    if table.catalog and catalog_name and table.catalog.lower() != catalog_name.lower():
        return False
    database_name = config_table.get("database_name")
    if table.db and database_name:
        return table.db.lower() == database_name.lower()
    return True


def get_where(query: expr.Expression):
    """The WHERE clause of this query only (not of nested CTEs or subqueries)."""
    return query.args.get("where")


def get_from(query: expr.Expression):
    """The FROM clause of this query only. Newer sqlglot versions call the arg "from_"."""
    return query.args.get("from") or query.args.get("from_")
