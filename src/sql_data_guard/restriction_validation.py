class UnsupportedRestrictionError(Exception):
    pass


# Allowed restriction operations (comparison + range/membership operators).
SUPPORTED_OPERATIONS = {"=", ">", "<", ">=", "<=", "BETWEEN", "IN"}
_COMPARISON_OPERATIONS = {">", "<", ">=", "<="}


def validate_restrictions(config: dict):
    """
    Validates the restrictions in the configuration to ensure only supported operations are used.

    Args:
        config (dict): The configuration containing the restrictions to validate.

    Raises:
        UnsupportedRestrictionError: If an unsupported restriction operation is found.
        ValueError: If the configuration or a restriction value is structurally invalid.
    """
    # Validate the optional mandatory-row-cap option (feature F3).
    if "force_limit" in config:
        force_limit = config["force_limit"]
        if (
            not isinstance(force_limit, int)
            or isinstance(force_limit, bool)
            or force_limit <= 0
        ):
            raise UnsupportedRestrictionError(
                f"Invalid 'force_limit': expected a positive integer. Received: {force_limit}"
            )

    # Validate the optional function allow-list / deny-list (feature F2).
    for key in ("allowed_functions", "blocked_functions"):
        if key in config:
            value = config[key]
            if not isinstance(value, list) or not all(
                isinstance(v, str) for v in value
            ):
                raise UnsupportedRestrictionError(
                    f"Invalid '{key}': expected a list of function-name strings. Received: {value}"
                )

    tables = config.get("tables", [])
    if not tables:
        raise ValueError("Configuration must contain at least one table.")

    for table in tables:
        if "table_name" not in table:
            raise ValueError("Each table must have a 'table_name' key.")
        if "columns" not in table or not table["columns"]:
            raise ValueError(
                "Each table must have a 'columns' key with valid column definitions."
            )

        # Validate the optional negative (deny-list) column rules (feature F10).
        if "denied_columns" in table:
            denied = table["denied_columns"]
            if not isinstance(denied, list) or not all(
                isinstance(c, str) for c in denied
            ):
                raise UnsupportedRestrictionError(
                    f"Invalid 'denied_columns' for table '{table['table_name']}': "
                    f"expected a list of column-name strings. Received: {denied}"
                )

        for restriction in table.get("restrictions", []):
            _validate_restriction(restriction, table)


def _validate_restriction(restriction: dict, table: dict):
    """
    Validates a single restriction and normalizes its operation to upper case.

    Normalizing in place lets callers use case-insensitive operations (e.g. "between")
    while the downstream AST matching logic continues to compare against upper-case
    operators only.
    """
    raw_operation = restriction.get("operation")
    # Normalize so operation matching is case-insensitive (C3). Symbol operators
    # such as "=" are unaffected by upper().
    if isinstance(raw_operation, str):
        operation = raw_operation.upper()
        restriction["operation"] = operation
    else:
        # A missing operation defaults to scalar equality.
        operation = "=" if raw_operation is None else raw_operation

    if operation == "BETWEEN":
        _validate_between(restriction)
    elif operation == "IN":
        _validate_in(restriction)
    elif operation in _COMPARISON_OPERATIONS:
        _validate_comparison_value(restriction, table)
    elif operation == "=":
        if "value" not in restriction and "values" not in restriction:
            raise ValueError(
                f"Restriction for column '{restriction.get('column')}' with operation "
                f"'=' must have a 'value' (or 'values')."
            )
    else:
        raise UnsupportedRestrictionError(
            f"Invalid restriction: 'operation={raw_operation}' is not supported."
        )


def _validate_between(restriction: dict):
    values = restriction.get("values")
    if not (
        isinstance(values, list)
        and len(values) == 2
        and all(isinstance(v, (int, float)) for v in values)
        and values[0] < values[1]
    ):
        raise ValueError(
            f"Invalid 'BETWEEN' format. Expected list of two numeric values "
            f"where min < max. Received: {values}"
        )


def _validate_in(restriction: dict):
    values = restriction.get("values")
    if not (isinstance(values, list) and len(values) >= 1):
        raise ValueError(
            f"Invalid 'IN' format. Expected a non-empty list of values. Received: {values}"
        )
    all_numeric = all(isinstance(v, (int, float)) for v in values)
    all_strings = all(isinstance(v, str) for v in values)
    if not (all_numeric or all_strings):
        raise ValueError(
            f"Invalid 'IN' format. All values must be of the same type "
            f"(all numeric or all strings). Received: {values}"
        )


def _validate_comparison_value(restriction: dict, table: dict):
    value = restriction.get("value")
    if not isinstance(value, (int, float)):
        raise ValueError(
            f"Invalid restriction value type for column '{restriction.get('column')}' "
            f"in table '{table['table_name']}'. Expected a numeric value."
        )
