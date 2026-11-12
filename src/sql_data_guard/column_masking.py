"""
Column masking.

Instead of removing a sensitive column from the SELECT list (which changes the shape of the
result and can break the caller), a table can declare it as *masked*: the column is still
returned under the same name, but its value is replaced, so the real value never leaves the
database.

    {
        "table_name": "users",
        "columns": ["id", "name", "ssn"],
        "column_masks": [{"column": "ssn", "policy": "redact", "replacement": "****"}]
    }

A masked column can only be selected directly (`ssn`, `u.ssn`, `ssn AS x`, or through `SELECT *`).
Any other use (WHERE, JOIN, GROUP BY, ORDER BY, inside a function or expression...) would let a
caller learn the real value one comparison at a time, and cannot be masked, so it is rejected.
"""

from typing import Dict, List, Optional

import sqlglot.expressions as expr

from .verification_utils import names_equal, same_name, table_matches

# policy name -> builds the replacement expression of a mask. More policies (for example a
# partial mask) need functions whose names differ per database, and can be registered here.
_DEFAULT_REPLACEMENT = "****"


def _redact(mask: dict) -> expr.Expression:
    return expr.Literal.string(mask.get("replacement", _DEFAULT_REPLACEMENT))


MASK_POLICIES = {"redact": _redact}


def validate_column_masks(config: dict) -> None:
    """
    Validates the `column_masks` of every table.

    Raises:
        ValueError: if a mask is malformed, uses an unknown policy, masks a column that is not in
            the table's `columns`, masks a column twice, or masks a column used by a restriction.
    """
    for table in config.get("tables", []):
        if "column_masks" not in table:
            continue
        name = table.get("table_name")
        masks = table["column_masks"]
        if not isinstance(masks, list):
            raise ValueError(f"'column_masks' of table '{name}' must be a list.")
        allowed = [str(c).lower() for c in table.get("columns", [])]
        restricted = [str(r.get("column")).lower() for r in table.get("restrictions") or []]
        seen = set()
        for mask in masks:
            if not isinstance(mask, dict) or not isinstance(mask.get("column"), str) or not mask["column"]:
                raise ValueError(f"Each column mask of table '{name}' must be an object with a 'column'.")
            column = mask["column"].lower()
            if column not in allowed:
                raise ValueError(
                    f"Masked column '{mask['column']}' must also be in the 'columns' of table '{name}'."
                )
            if column in seen:
                raise ValueError(f"Column '{mask['column']}' of table '{name}' is masked more than once.")
            seen.add(column)
            if column in restricted:
                raise ValueError(
                    f"Column '{mask['column']}' of table '{name}' cannot be both masked and restricted."
                )
            policy = mask.get("policy")
            if policy not in MASK_POLICIES:
                raise ValueError(
                    f"Unsupported mask policy {policy!r} for column '{mask['column']}'. "
                    f"Supported policies: {sorted(MASK_POLICIES)}."
                )
            replacement = mask.get("replacement", _DEFAULT_REPLACEMENT)
            if not isinstance(replacement, str):
                raise ValueError(f"'replacement' for column '{mask['column']}' must be a string.")


def find_mask(col: expr.Column, scopes: List[List[expr.Table]], config: dict) -> Optional[dict]:
    """
    The mask that applies to a column reference, or None.

    `scopes` are the tables of the query being verified and of its enclosing queries, innermost
    first, so a correlated sub-query that refers to a table of the outer query is found too. A
    qualified column is resolved to the innermost table with that name or alias; an unqualified
    column to the innermost query that has a configured table with that column. If several tables
    of that query could provide it, it is masked when any of them masks it (the safe choice).
    """
    for tables in scopes:
        candidates = []
        for table in tables:
            if col.table and not same_name(col.table, table.alias_or_name):
                continue
            for config_table in config["tables"]:
                if table_matches(table, config_table) and (
                    col.table or any(names_equal(col, c) for c in config_table.get("columns", []))
                ):
                    candidates.append(config_table)
        if candidates:
            return next(
                (
                    mask
                    for config_table in candidates
                    for mask in config_table.get("column_masks") or []
                    if names_equal(col, mask["column"])
                ),
                None,
            )
    return None


def mask_value(mask: dict) -> expr.Expression:
    """The expression that replaces the value of a masked column."""
    return MASK_POLICIES[mask["policy"]](mask)


def masked_select_item(mask: dict, name: str) -> expr.Expression:
    """`<mask value> AS <name>`: what a masked column is rewritten to in the SELECT list."""
    return expr.alias_(mask_value(mask), expr.to_identifier(name))


def table_masks(config_table: dict) -> Dict[str, dict]:
    """Masks of a configured table, by lower case column name."""
    return {m["column"].lower(): m for m in config_table.get("column_masks") or []}
