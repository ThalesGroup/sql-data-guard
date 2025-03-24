class UnsupportedRestrictionError(Exception):
    pass


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
    # Ensure 'tables' exists in config and is not empty
    tables = config.get("tables", [])
    # Check if tables are empty
    if not tables:
        raise ValueError("Configuration must contain at least one table.")

    for table in tables:
        # Ensure that 'table_name' exists in each table
        if "table_name" not in table:
            raise ValueError("Each table must have a 'table_name' key.")
            # Ensure that 'columns' exists and is not empty in each table
        if "columns" not in table or not table["columns"]:
            raise ValueError(
                "Each table must have a 'columns' key with valid column definitions."
            )

        restrictions = table.get("restrictions", [])
        if not restrictions:
            continue  # Skip if no restrictions are provided

        for restriction in restrictions:
            operation = restriction.get("operation")
            if operation == "BETWEEN":
                values = restriction.get("values")
                if not (
                    isinstance(values, list)
                    and len(values) == 2
                    and all(isinstance(v, (int, float)) for v in values)
                    and values[0] < values[1]
                ):
                    raise ValueError(
                        f"Invalid 'BETWEEN' format. Expected list of two numeric values where min < max. Received: {values}"
                    )

            elif operation == "IN":
                values = restriction.get("values")
                if not (
                    isinstance(values, list)
                    and len(values) == 2
                    and all(isinstance(v, (int, float)) for v in values)
                ):
                    raise ValueError(
                        f"Invalid 'IN' format. Expected list of two numeric values. Received: {values}"
                    )

            elif operation == ">=":
                # You may want to ensure the value provided is numeric for >=
                value = restriction.get("value")
                if not isinstance(value, (int, float)):
                    raise ValueError(
                        f"Invalid restriction value type for column '{restriction['column']}' in table '{table['table_name']}'. Expected a numeric value."
                    )

            elif operation and operation.lower() not in supported_operations:
                raise UnsupportedRestrictionError(
                    f"Invalid restriction: 'operation={operation}' is not supported."
                )
