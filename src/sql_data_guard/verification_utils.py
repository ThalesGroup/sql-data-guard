from collections.abc import Generator
from typing import Any

import sqlglot.expressions as expr


def split_to_expressions(
    exp: expr.Expression, exp_type: type[expr.Expression]
) -> Generator[expr.Expression]:
    if isinstance(exp, exp_type):
        yield from exp.flatten()  # type: ignore
    else:
        yield exp


def find_direct(exp: Any, exp_type: type[Any]) -> Generator[Any]:
    for child in exp.args.values():
        if isinstance(child, exp_type):
            yield child
