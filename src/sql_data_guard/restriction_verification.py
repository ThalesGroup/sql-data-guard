from typing import List

import sqlglot
import sqlglot.expressions as expr

from sql_data_guard.verification_context import VerificationContext
from sql_data_guard.verification_utils import split_to_expressions


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
            for idx, r in enumerate(c_t.get("restrictions", [])):
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
    if restriction.get("operation") == "BETWEEN":
        operator = "BETWEEN"
        operand = f"{_format_value(restriction["values"][0])} AND {_format_value(restriction["values"][1])}"
    elif restriction.get("operation") == "IN":
        operator = "IN"
        values = restriction.get("values", [restriction.get("value")])
        operand = f"({', '.join(map(str, values))})"
    else:
        operator = "="
        operand = (
            _format_value(restriction["value"])
            if "value" in restriction
            else str(restriction["values"])[1:-1]
        )
    new_condition = sqlglot.parse_one(
        f"{table_prefix}{restriction['column']} {operator} {operand}",
        dialect=context.dialect,
    )
    return new_condition


def _format_value(value):
    if isinstance(value, str):
        return f"'{value}'"
    else:
        return value


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
        return any(v in values for v in expr_values)

    # Handle EQ (=) condition
    if isinstance(exp, expr.EQ) and isinstance(exp.right, expr.Condition):
        return str(exp.right.this) in values

    # Handle BETWEEN conditions correctly
    if isinstance(exp, expr.Between):
        low, high = int(exp.args["low"].this), int(exp.args["high"].this)
        if len(values) == 2:  # Ensure we have exactly two values
            restriction_low, restriction_high = map(int, values)
            return restriction_low <= low and high <= restriction_high

    # Handle comparison operators (<, >, <=, >=)
    def check_comparison_operator(exp1, restriction_, operator):
        """Handles LT (<), GT (>), LTE (<=), and GTE (>=) conditions."""
        if not isinstance(exp1, operator):
            return False

        column_name = (
            exp1.this.name
            if not isinstance(exp1.this, expr.Avg)
            else exp1.this.this.name
        )

        # Ensure it's checking the correct column
        if column_name != restriction_["column"]:
            return False

        value = int(exp1.expression.this)  # Extract the number after the operator

        if len(values) == 2:  # If a range is given (e.g., [80, 150])
            low_restriction, high_restriction = map(int, values)
            if operator in [expr.GT, expr.GTE]:
                return low_restriction <= value <= high_restriction
            return low_restriction <= value  # For LT, LTE

        elif len(values) == 1:  # If only a single value exists
            restriction_value = int(values[0])
            return {
                expr.GT: value > restriction_value,
                expr.GTE: value >= restriction_value,
                expr.LT: value < restriction_value,
                expr.LTE: value <= restriction_value,
            }[operator]

        return False  # Default case

    if any(
        check_comparison_operator(exp, restriction, op)
        for op in [expr.LT, expr.GT, expr.LTE, expr.GTE]
    ):
        return True

    return False


def _get_restriction_values(restriction: dict) -> List[str]:
    if "values" in restriction:
        values = [str(v) for v in restriction["values"]]
    else:
        values = [str(restriction["value"])]
    return values
