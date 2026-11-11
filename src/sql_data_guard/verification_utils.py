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


def table_matches(table: expr.Table, config_table: dict) -> bool:
    """
    Checks if a table reference in a query refers to a table in the configuration.

    The table name must match. If the query qualifies the table with a schema/database
    and the configuration defines a database_name, they must match too.
    """
    if table.name != config_table["table_name"]:
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
