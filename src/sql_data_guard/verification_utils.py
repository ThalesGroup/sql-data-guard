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
