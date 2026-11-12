from typing import List, Optional

import sqlglot
import sqlglot.expressions as expr

from .verification_context import VerificationContext
from .verification_utils import (
    split_to_expressions,
    get_where,
    table_matches,
    names_equal,
    same_name,
)


def verify_restrictions(
    select_statement: expr.Query,
    context: VerificationContext,
    from_tables: List[expr.Table],
):
    where_clause = get_where(select_statement)
    if where_clause is None:
        and_exps = []
    else:
        and_exps = list(split_to_expressions(where_clause.this, expr.And))
    for c_t in context.config["tables"]:
        for from_t in [
            t
            for t in from_tables
            # a CTE with the name of a configured table is not that table (its body is verified,
            # with its restrictions, on its own)
            if table_matches(t, c_t) and (t.db or t.name not in context.cte_names)
        ]:
            for r in c_t.get("restrictions", []):
                found = False
                for sub_exp in and_exps:
                    if _verify_restriction(r, from_t, sub_exp):
                        found = True
                        break
                if not found:
                    if from_t.alias:
                        t_prefix = f"{from_t.alias}."
                    elif (
                        len([t for t in from_tables if same_name(t.name, from_t.name)])
                        > 1
                    ):
                        t_prefix = f"{from_t.name}."
                    else:
                        t_prefix = ""

                    context.add_error(
                        f"Missing restriction for table: {c_t['table_name']} column: {t_prefix}{r['column']} value: {r.get('values', r.get('value'))}",
                        True,
                        0.5,
                    )
                    new_condition = _create_new_condition(context, r, t_prefix)
                    if where_clause is None:
                        where_clause = expr.Where(this=new_condition)
                        select_statement.set("where", where_clause)
                    else:
                        where_clause = where_clause.replace(
                            expr.Where(
                                this=expr.And(
                                    this=expr.paren(where_clause.this),
                                    expression=new_condition,
                                )
                            )
                        )


def _create_new_condition(
    context: VerificationContext, restriction: dict, table_prefix: str
) -> expr.Expression:
    """
    Used to create a restriction condition for a given restriction.

    Args:
        context: verification context
        restriction: restriction to create condition for
        table_prefix: table prefix to use in the condition

    Returns: condition expression

    """
    dialect = context.dialect
    operation = restriction.get("operation")
    if operation == "BETWEEN":
        operator = "BETWEEN"
        operand = (
            f"{_format_value(restriction['values'][0], dialect)} AND "
            f"{_format_value(restriction['values'][1], dialect)}"
        )
    elif operation == "IN":
        operator = "IN"
        values = restriction.get("values", [restriction.get("value")])
        operand = f"({', '.join(_format_value(v, dialect) for v in values)})"
    else:
        operator = operation if operation in _COMPARISON_OPERATORS else "="
        values = (
            [restriction["value"]] if "value" in restriction else restriction["values"]
        )
        if operator == "=" and len(values) > 1:
            # `=` with several allowed values: the column may equal any of them
            operator = "IN"
            operand = f"({', '.join(_format_value(v, dialect) for v in values)})"
        else:
            operand = _format_value(values[0], dialect)
    new_condition = sqlglot.parse_one(
        f"{table_prefix}{restriction['column']} {operator} {operand}",
        dialect=dialect,
    )
    return new_condition


_COMPARISON_OPERATORS = (">", "<", ">=", "<=")


def _format_value(value, dialect=None):
    """Formats a config value as an SQL literal, escaping it for the dialect."""
    if isinstance(value, str):
        return expr.Literal.string(value).sql(dialect=dialect)
    return str(value)


def _to_number(value):
    """Returns value as float, or None if it is not numeric."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _verify_restriction(
    restriction: dict, from_table: expr.Table, exp: expr.Expression
) -> bool:
    """
    Verifies if a given restriction is satisfied within an SQL expression.

    Args:
        restriction (dict): The restriction to verify, containing 'column' and 'value' or 'values'.
        from_table (Table): The table reference to check the restriction against.
        exp (Expression): The SQL expression to check against the restriction.

    Returns:
        bool: True if the restriction is satisfied, False otherwise.
    """

    if isinstance(exp, expr.Not):
        return False

    if isinstance(exp, expr.Paren):
        return _verify_restriction(restriction, from_table, exp.this)

    if not isinstance(exp.this, expr.Column) or not names_equal(
        exp.this, restriction["column"]
    ):
        return False

    if (
        exp.this.table
        and from_table.alias
        and not same_name(exp.this.table, from_table.alias)
    ):
        return False
    if (
        exp.this.table
        and not from_table.alias
        and not same_name(exp.this.table, from_table.name)
    ):
        return False

    values = _get_restriction_values(restriction)  # Get correct restriction values
    operation = restriction.get("operation")

    # Only literal values count: a column (`region = eu`, where `eu` is a column of a derived
    # table), a sub-query (`IN (SELECT 999)`) or an expression could evaluate to anything.
    if isinstance(exp, expr.In):
        if exp.args.get("query") or exp.args.get("unnest") or not exp.expressions:
            return False
        expr_values = [_literal_value(val) for val in exp.expressions]
        return all(
            v is not None and _value_allowed(operation, values, v) for v in expr_values
        )

    if isinstance(exp, expr.EQ):
        value = _literal_value(exp.right)
        return value is not None and _value_allowed(operation, values, value)

    if isinstance(exp, expr.Between):
        low = _to_number(_literal_value(exp.args["low"]))
        high = _to_number(_literal_value(exp.args["high"]))
        if low is None or high is None:
            return False
        if operation == "BETWEEN":
            low_limit, high_limit = (_to_number(v) for v in values)
            return (
                low_limit is not None
                and high_limit is not None
                and low_limit <= low
                and high <= high_limit
            )
        if operation in (">", ">="):
            return _value_allowed(operation, values, str(low))
        if operation in ("<", "<="):
            return _value_allowed(operation, values, str(high))
        return False

    if isinstance(exp, (expr.LT, expr.LTE, expr.GT, expr.GTE)):
        if operation not in _COMPARISON_OPERATORS:
            return False
        upper = isinstance(exp, (expr.LT, expr.LTE))  # `col < x` / `col <= x`
        if upper != (operation in ("<", "<=")):
            return False
        bound = _to_number(_literal_value(exp.right))
        limit = _to_number(values[0])
        if bound is None or limit is None:
            return False
        # the query's bound must be at least as strict as the restriction's
        if operation == "<" and isinstance(exp, expr.LTE):
            return bound < limit
        if operation == ">" and isinstance(exp, expr.GTE):
            return bound > limit
        return bound <= limit if upper else bound >= limit
    return False


def _literal_value(node: Optional[expr.Expression]) -> Optional[str]:
    """
    The value of a literal operand, as a string (`'eu'`, `123`, `-5`, `TRUE`), or None if the
    operand is anything else (a column, a sub-query, a function or other expression).
    """
    while isinstance(node, expr.Paren):
        node = node.this
    if isinstance(node, expr.Literal):
        return node.this
    if isinstance(node, expr.Boolean):
        return str(node.this)
    if (
        isinstance(node, expr.Neg)
        and isinstance(node.this, expr.Literal)
        and not node.this.is_string
    ):
        return f"-{node.this.this}"
    return None


def _value_allowed(operation: Optional[str], values: List[str], value: str) -> bool:
    """True if a column equal to `value` satisfies the restriction."""
    if operation == "BETWEEN":
        number, low, high = (_to_number(v) for v in (value, *values))
        return None not in (number, low, high) and low <= number <= high
    if operation in _COMPARISON_OPERATORS:
        number, limit = _to_number(value), _to_number(values[0])
        if number is None or limit is None:
            return False
        return {
            "<": number < limit,
            "<=": number <= limit,
            ">": number > limit,
            ">=": number >= limit,
        }[operation]
    return value in values


def _get_restriction_values(restriction: dict) -> List[str]:
    if "values" in restriction:
        values = [str(v) for v in restriction["values"]]
    else:
        values = [str(restriction["value"])]
    return values
