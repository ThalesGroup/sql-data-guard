import math

import sqlglot
import sqlglot.expressions as expr

from .column_masking import validate_column_masks


class UnsupportedRestrictionError(Exception):
    pass


def _validate_max_length(config: dict):
    if "max_length" not in config:
        return
    value = config["max_length"]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(
            f"Invalid 'max_length'. Expected a positive integer. Received: {value!r}"
        )


def _is_scalar(value) -> bool:
    """A restriction value: a string, a bool or a finite number."""
    if isinstance(value, float):
        return math.isfinite(value)
    return isinstance(value, (bool, int, str))


def _is_number(value) -> bool:
    return _is_scalar(value) and not isinstance(value, (bool, str))


def _is_column_name(name) -> bool:
    """A plain (optionally quoted) column name, as it is pasted into the condition that is added."""
    if not isinstance(name, str) or not name.strip():
        return False
    try:
        statements = sqlglot.parse(name)
    except sqlglot.errors.SqlglotError:
        return False
    if len(statements) != 1:  # `id; DROP TABLE x`
        return False
    parsed = statements[0]
    return isinstance(parsed, expr.Column) and not parsed.table


def _validate_columns(config: dict, table: dict):
    if "columns" not in table and config.get("table_level_access"):
        return  # the table is approved as a whole
    columns = table.get("columns")
    if not isinstance(columns, list) or not columns:
        raise ValueError(
            "Each table must have a 'columns' key with valid column definitions."
        )
    if not all(isinstance(c, str) and c.strip() for c in columns):
        raise ValueError(
            f"The 'columns' of table '{table['table_name']}' must be non-empty strings."
        )


def _validate_restriction_shape(restriction, table_name: str):
    """Checks the keys every restriction needs, whatever its operation."""
    if not isinstance(restriction, dict):
        raise ValueError(f"Each restriction of table '{table_name}' must be an object.")
    if not _is_column_name(restriction.get("column")):
        raise ValueError(
            f"Each restriction of table '{table_name}' must have a 'column' with a column name. "
            f"Received: {restriction.get('column')!r}"
        )
    operation = restriction.get("operation")
    if operation is not None and not isinstance(operation, str):
        raise UnsupportedRestrictionError(
            f"Invalid restriction: 'operation={operation!r}' is not supported."
        )
    if operation in (None, "="):
        # `value`, or `values` (the column may then equal any of them)
        if "value" in restriction:
            ok = _is_scalar(restriction["value"])
        else:
            values = restriction.get("values")
            ok = (
                isinstance(values, list)
                and len(values) > 0
                and all(_is_scalar(v) for v in values)
            )
        if not ok:
            raise ValueError(
                f"Invalid restriction for column '{restriction['column']}' in table '{table_name}'. "
                "Expected a 'value' (a number, string or boolean) or a non-empty list of 'values'."
            )


def _non_empty_string(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_database_options(config: dict):
    """`databases`, `strict_database` and `catalog_name` (top level and per table)."""
    for key in ("strict_database", "allow_metadata", "table_level_access"):
        if not isinstance(config.get(key, False), bool):
            raise ValueError(
                f"Invalid '{key}'. Expected true or false. Received: {config[key]!r}"
            )
    if "catalog_name" in config and not _non_empty_string(config["catalog_name"]):
        raise ValueError(
            f"Invalid 'catalog_name'. Expected a non-empty string. Received: {config['catalog_name']!r}"
        )
    if "databases" in config:
        databases = config["databases"]
        if not isinstance(databases, list) or not all(
            _non_empty_string(d) for d in databases
        ):
            raise ValueError(
                f"Invalid 'databases'. Expected a list of database names. Received: {databases!r}"
            )


def _validate_table_database(config: dict, table: dict):
    name = table["table_name"]
    if "catalog_name" in table and not _non_empty_string(table["catalog_name"]):
        raise ValueError(f"The 'catalog_name' of table '{name}' must be a non-empty string.")
    database_name = table.get("database_name")
    if config.get("strict_database") and not _non_empty_string(database_name):
        raise ValueError(
            f"Table '{name}' must have a 'database_name' when 'strict_database' is set."
        )
    if database_name and "databases" in config:
        if database_name.lower() not in {d.lower() for d in config["databases"]}:
            raise ValueError(
                f"The database '{database_name}' of table '{name}' is not in 'databases'."
            )


def _validate_force_limit(config: dict):
    if "force_limit" not in config:
        return
    limit = config["force_limit"]
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError(
            f"Invalid 'force_limit'. Expected a positive integer. Received: {limit!r}"
        )


def _validate_function_list(config: dict, key: str):
    if key not in config:
        return
    names = config[key]
    if not isinstance(names, list) or not all(
        isinstance(n, str) and n.strip() for n in names
    ):
        raise ValueError(
            f"Invalid '{key}'. Expected a list of function names. Received: {names!r}"
        )


def validate_restrictions(config: dict):
    """
    Validates the restrictions in the configuration to ensure only supported operations are used.

    Args:
        config (dict): The configuration containing the restrictions to validate.

    Raises:
        UnsupportedRestrictionError: If an unsupported restriction operation is found.
        ValueError: If there are no tables in the configuration.
    """
    supported_operations = [
        "=",
        ">",
        "<",
        ">=",
        "<=",
        "BETWEEN",
        "IN",
    ]  # Allowed operations
    _validate_max_length(config)
    _validate_database_options(config)
    _validate_force_limit(config)
    _validate_function_list(config, "blocked_functions")
    _validate_function_list(config, "allowed_functions")
    # Ensure 'tables' exists in config and is not empty
    tables = config.get("tables", [])
    # Check if tables are empty
    if not tables:
        raise ValueError("Configuration must contain at least one table.")
    if not isinstance(tables, list):
        raise ValueError("'tables' must be a list.")

    seen = set()
    for table in tables:
        if not isinstance(table, dict):
            raise ValueError("Each table must be an object.")
        # Ensure that 'table_name' exists in each table
        if "table_name" not in table:
            raise ValueError("Each table must have a 'table_name' key.")
        if not isinstance(table["table_name"], str) or not table["table_name"].strip():
            raise ValueError("Each 'table_name' must be a non-empty string.")
        database_name = table.get("database_name")
        if database_name is not None and not isinstance(database_name, str):
            raise ValueError(
                f"The 'database_name' of table '{table['table_name']}' must be a string."
            )
        _validate_table_database(config, table)
        key = (
            table["table_name"].lower(),
            (database_name or "").lower(),
            str(table.get("catalog_name") or "").lower(),
        )
        if key in seen:
            raise ValueError(
                f"Table '{table['table_name']}' is defined more than once."
            )
        seen.add(key)
        _validate_columns(config, table)

        restrictions = table.get("restrictions", [])
        if not isinstance(restrictions, list):
            raise ValueError(
                f"The 'restrictions' of table '{table['table_name']}' must be a list."
            )

        for restriction in restrictions:
            _validate_restriction_shape(restriction, table["table_name"])
            operation = restriction.get("operation")
            if operation == "BETWEEN":
                values = restriction.get("values")
                if not (
                    isinstance(values, list)
                    and len(values) == 2
                    and all(_is_number(v) for v in values)
                    and values[0] < values[1]
                ):
                    raise ValueError(
                        f"Invalid 'BETWEEN' format. Expected list of two numeric values where min < max. Received: {values}"
                    )

            elif operation == "IN":
                values = restriction.get("values")
                if not (
                    isinstance(values, list)
                    and len(values) > 0
                    and all(_is_scalar(v) and not isinstance(v, bool) for v in values)
                ):
                    raise ValueError(
                        f"Invalid 'IN' format. Expected a non-empty list of numbers or strings. Received: {values}"
                    )

            elif operation in (">", "<", ">=", "<="):
                # comparison operators are evaluated numerically
                value = restriction.get("value")
                if not _is_number(value):
                    raise ValueError(
                        f"Invalid restriction value type for column '{restriction['column']}' in table '{table['table_name']}'. Expected a numeric value."
                    )

            elif operation and operation.lower() not in supported_operations:
                raise UnsupportedRestrictionError(
                    f"Invalid restriction: 'operation={operation}' is not supported."
                )

    validate_column_masks(config)
