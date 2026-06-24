"""
SQL Data Guard verification utilities.

This module provides helper utilities for flattening expressions and finding
child nodes directly within parsed SQL expression trees.
"""

from collections.abc import Generator
from typing import Any

import sqlglot.expressions as expr


def split_to_expressions(
    exp: expr.Expression, exp_type: type[expr.Expression]
) -> Generator[expr.Expression]:
    """
    Flattens an expression into a generator of sub-expressions if it matches the target type, or yields it.

    Args:
        exp (expr.Expression): The SQL expression to split/flatten.
        exp_type (type[expr.Expression]): The SQLGlot expression class (e.g., expr.And or expr.Or) to split on.

    Yields:
        Generator[expr.Expression]: Flattened SQL expressions.
    """
    if isinstance(exp, exp_type):
        yield from exp.flatten()  # type: ignore
    else:
        yield exp


def find_direct(exp: Any, exp_type: type[Any]) -> Generator[Any]:
    """
    Finds and yields immediate child nodes of the given expression matching the specified type.

    Args:
        exp (Any): The parent SQL expression.
        exp_type (type[Any]): The target child expression class to search for.

    Yields:
        Generator[Any]: Immediate matching children of the parent expression.
    """
    for child in exp.args.values():
        if isinstance(child, exp_type):
            yield child
