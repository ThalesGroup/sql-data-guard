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
        restriction (dict): The restriction to verify, containing 'column' and 'value' keys.
        from_table (Table): The table reference to check the restriction against.
        exp (list): The SQL expression to check against the restriction.

    Returns:
        bool: True if the restriction is satisfied, False otherwise.
    """
    if isinstance(exp, expr.Not) and isinstance(exp.this, expr.In):
        expr_values = [int(val.this) for val in exp.this.expressions]  # Convert to int
        restriction_values = [
            int(val) for val in restriction["values"]
        ]  # Convert to int

        # NOT IN should be valid if it only excludes values in the restriction
        return all(val in restriction_values for val in expr_values)

    if isinstance(exp, expr.Paren):
        return _verify_restriction(restriction, from_table, exp.this)
    if not isinstance(exp.this, expr.Column):
        return False
    if not exp.this.name == restriction["column"]:
        return False
    if exp.this.table and from_table.alias and exp.this.table != from_table.alias:
        return False
    if exp.this.table and not from_table.alias and exp.this.table != from_table.name:
        return False
    if isinstance(exp, expr.EQ) and isinstance(exp.right, expr.Condition):
        if isinstance(exp.right, expr.Boolean):
            return exp.right.this == restriction["value"]
        else:
            values = _get_restriction_values(restriction)
            return exp.right.this in values

    # Check if the expression is a BETWEEN condition (e.g., price BETWEEN 80 AND 150)
    if isinstance(exp, expr.Between):
        low = int(exp.args["low"].this)  # Extract the lower bound
        high = int(exp.args["high"].this)  # Extract the upper bound
        restriction_low, restriction_high = map(
            int, restriction["values"]
        )  # Get allowed range from restriction
        # Return True only if the given range is within the allowed range
        return restriction_low <= low and high <= restriction_high

        # Check if the expression is a NOT BETWEEN condition (e.g., price NOT BETWEEN 80 AND 150)
    if isinstance(exp, expr.Not) and isinstance(exp.this, expr.Between):
        low = int(exp.this.args["low"].this)  # Extract lower bound
        high = int(exp.this.args["high"].this)  # Extract upper bound
        restriction_low, restriction_high = map(
            int, restriction["values"]
        )  # Convert to int
        # NOT BETWEEN should be valid if the range is completely outside the restriction
        # Ensures it's fully outside
        return (
            low < restriction_low or high > restriction_high
        )  # Ensures it's fully outside the allowed range

    # Check if the expression is a LESS THAN condition (e.g., price < 100)
    if isinstance(exp, expr.LT):
        if (
            exp.this.name == restriction["column"]
        ):  # Ensure it's checking the correct column
            value = int(exp.expression.this)  # Extract the number after '<'

            if "values" in restriction:  # If a range is given (e.g., [80, 150])
                restriction_low, restriction_high = map(int, restriction["values"])
                return (
                    restriction_low <= value
                )  # Ensure the value is above the lower bound
            else:
                restriction_value = int(
                    restriction["value"]
                )  # If only a single value exists
                return value < restriction_value  # Ensure value is less than allowed

    # Check if the expression is a GREATER THAN condition (e.g., price > 100)
    # Also handles AVG(price) > X conditions
    if isinstance(exp, expr.GT) or (
        isinstance(exp, expr.GT) and isinstance(exp.this, expr.Avg)
    ):
        if exp.this.name == restriction["column"] or (
            isinstance(exp.this, expr.Avg)
            and exp.this.this.name == restriction["column"]
        ):
            value = int(exp.expression.this)  # Extract the number after '>'

            if "values" in restriction:  # If a range is given (e.g., [80, 150])
                restriction_low, restriction_high = map(int, restriction["values"])
                return (
                    restriction_low <= value <= restriction_high
                )  # Ensure value is within range
            else:
                restriction_value = int(
                    restriction["value"]
                )  # If only a single value exists
                return value > restriction_value  # Ensure value is greater than allowed

    # Check if the expression is an IN condition (e.g., price IN (100, 120, 150))
    if isinstance(exp, expr.In):
        expr_values = [int(val.this) for val in exp.expressions]  # Extract SQL values
        restriction_values = [
            int(val) for val in restriction["values"]
        ]  # Extract allowed values

        return any(v in restriction_values for v in expr_values)

    return False


def _get_restriction_values(restriction: dict) -> List[str]:
    if "values" in restriction:
        values = [str(v) for v in restriction["values"]]
    else:
        values = [str(restriction["value"])]
    return values
