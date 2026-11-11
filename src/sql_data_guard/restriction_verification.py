from typing import List

import sqlglot
import sqlglot.expressions as expr

from .verification_context import VerificationContext
from .verification_utils import split_to_expressions


def verify_restrictions(
    select_statement: expr.Query,
    context: VerificationContext,
    from_tables: List[expr.Table],
):
    where_clause = select_statement.find(expr.Where)
    if where_clause is None:
        where_clause = select_statement.find(expr.Where)
        and_exps = []
    else:
        and_exps = list(split_to_expressions(where_clause.this, expr.And))
    for c_t in context.config["tables"]:
        for from_t in [t for t in from_tables if t.name == c_t["table_name"]]:
            for r in c_t.get("restrictions", []):
                found = False
                for sub_exp in and_exps:
                    if _verify_restriction(r, from_t, sub_exp):
                        found = True
                        break
                if not found:
                    if from_t.alias:
                        t_prefix = f"{from_t.alias}."
                    elif len([t for t in from_tables if t.name == from_t.name]) > 1:
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
        operand = (
            _format_value(restriction["value"], dialect)
            if "value" in restriction
            else str(restriction["values"])[1:-1]
        )
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

    if not isinstance(exp.this, expr.Column) or exp.this.name != restriction["column"]:
        return False

    if exp.this.table and from_table.alias and exp.this.table != from_table.alias:
        return False
    if exp.this.table and not from_table.alias and exp.this.table != from_table.name:
        return False

    values = _get_restriction_values(restriction)  # Get correct restriction values

    # Handle IN condition correctly
    if isinstance(exp, expr.In):
        expr_values = [str(val.this) for val in exp.expressions]
        return all(v in values for v in expr_values)

    # Handle EQ (=) condition
    if isinstance(exp, expr.EQ) and isinstance(exp.right, expr.Condition):
        return str(exp.right.this) in values

    if isinstance(exp, expr.Between):
        low = _to_number(exp.args["low"].this)
        high = _to_number(exp.args["high"].this)
        restriction_values = [_to_number(v) for v in values]
        if len(values) == 2 and None not in [low, high, *restriction_values]:
            return restriction_values[0] <= low and high <= restriction_values[1]
        return False

    if isinstance(exp, (expr.LT, expr.LTE, expr.GT, expr.GTE)) and isinstance(
        exp.right, expr.Condition
    ):
        if restriction.get("operation") not in _COMPARISON_OPERATORS:
            return False
        if len(values) != 1:
            return False
        actual = _to_number(exp.right.this)
        limit = _to_number(values[0])
        if actual is None or limit is None:
            return False
        if isinstance(exp, expr.LT) and restriction["operation"] == "<":
            return actual < limit
        elif isinstance(exp, expr.LTE) and restriction["operation"] == "<=":
            return actual <= limit
        elif isinstance(exp, expr.GT) and restriction["operation"] == ">":
            return actual > limit
        elif isinstance(exp, expr.GTE) and restriction["operation"] == ">=":
            return actual >= limit
        else:
            return False
    return False


def _get_restriction_values(restriction: dict) -> List[str]:
    if "values" in restriction:
        values = [str(v) for v in restriction["values"]]
    else:
        values = [str(restriction["value"])]
    return values
