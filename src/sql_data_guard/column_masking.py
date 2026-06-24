"""Column-level masking / redaction.

Instead of *removing* a sensitive column from the SELECT clause (which changes the
result shape and breaks downstream applications), sql-data-guard can rewrite the
column expression into a masking function. The query keeps returning a column with
the same output name, but the sensitive value never leaves the trust boundary.

Masking is declared per table via a ``column_masks`` list in the config::

    {
        "table_name": "users",
        "columns": ["id", "email", "credit_card"],
        "column_masks": [
            {"column": "credit_card", "policy": "partial", "show_last": 4},
            {"column": "email", "policy": "hash"},
            {"column": "ssn", "policy": "redact"}
        ]
    }

Supported policies:

* ``redact``  -> replace the value with a constant string (default ``'****'``).
                 Configure the constant via ``"replacement"``.
* ``hash``    -> replace the value with a one-way hash (``MD5(col)``), portable
                 across dialects via sqlglot.
* ``partial`` -> keep the last ``show_last`` characters, mask the rest
                 (e.g. ``****1234``). ``show_last`` defaults to 4.
"""

from typing import Dict, Optional

import sqlglot
import sqlglot.expressions as expr

SUPPORTED_MASK_POLICIES = {"redact", "hash", "partial"}

_DEFAULT_REPLACEMENT = "****"
_DEFAULT_SHOW_LAST = 4


def validate_column_masks(config: dict) -> None:
    """Validate the ``column_masks`` section of every table in the config.

    Raises:
        ValueError: if a mask references an unknown policy, targets a column that
            is not in the table's ``columns`` allow-list, or uses an invalid
            ``show_last`` value.
    """
    for table in config.get("tables", []):
        masks = table.get("column_masks")
        if not masks:
            continue
        if not isinstance(masks, list):
            raise ValueError(
                f"'column_masks' for table '{table.get('table_name')}' must be a list."
            )
        allowed_columns = set(table.get("columns", []))
        for mask in masks:
            column = mask.get("column")
            if not column:
                raise ValueError(
                    f"Each column mask in table '{table.get('table_name')}' must have a 'column'."
                )
            if column not in allowed_columns:
                raise ValueError(
                    f"Masked column '{column}' must also appear in the 'columns' "
                    f"allow-list of table '{table.get('table_name')}'."
                )
            policy = mask.get("policy")
            if policy not in SUPPORTED_MASK_POLICIES:
                raise ValueError(
                    f"Unsupported mask policy '{policy}' for column '{column}'. "
                    f"Supported policies: {sorted(SUPPORTED_MASK_POLICIES)}."
                )
            if policy == "partial":
                show_last = mask.get("show_last", _DEFAULT_SHOW_LAST)
                if not isinstance(show_last, int) or show_last < 0:
                    raise ValueError(
                        f"'show_last' for column '{column}' must be a non-negative integer."
                    )


def build_mask_lookup(config: dict) -> Dict[str, Dict[str, dict]]:
    """Build a ``{table_name: {column_name: mask_spec}}`` lookup from the config."""
    lookup: Dict[str, Dict[str, dict]] = {}
    for table in config.get("tables", []):
        masks = table.get("column_masks")
        if not masks:
            continue
        lookup[table["table_name"]] = {m["column"]: m for m in masks}
    return lookup


def build_mask_expression(
    mask: dict, column: expr.Column, dialect: Optional[str]
) -> expr.Expression:
    """Build the masking expression for ``column`` according to ``mask``.

    The returned expression preserves the original output name via an alias so the
    rewritten query yields a column with the same name the caller asked for.
    """
    output_name = column.alias_or_name
    policy = mask["policy"]

    if policy == "redact":
        replacement = mask.get("replacement", _DEFAULT_REPLACEMENT)
        masked: expr.Expression = expr.Literal.string(replacement)
    elif policy == "hash":
        masked = sqlglot.parse_one(
            f"MD5({column.sql(dialect=dialect)})", dialect=dialect
        )
    elif policy == "partial":
        show_last = mask.get("show_last", _DEFAULT_SHOW_LAST)
        replacement = mask.get("replacement", _DEFAULT_REPLACEMENT)
        col_sql = column.sql(dialect=dialect)
        # '****' || SUBSTRING(col FROM -show_last) — keep the last `show_last` chars.
        # SUBSTRING with a negative start is normalized by sqlglot per dialect on
        # output (e.g. SUBSTR for sqlite), which keeps the mask portable.
        masked = sqlglot.parse_one(
            f"CONCAT('{replacement}', SUBSTRING(CAST({col_sql} AS VARCHAR), -{show_last}))",
            dialect=dialect,
        )
    else:  # pragma: no cover - guarded by validate_column_masks
        raise ValueError(f"Unsupported mask policy '{policy}'.")

    return expr.alias_(masked, output_name)
