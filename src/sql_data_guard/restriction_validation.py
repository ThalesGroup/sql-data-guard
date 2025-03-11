class UnsupportedRestrictionError(Exception):
    pass


def validate_restrictions(config: dict):
    """
    Validates the restrictions in the configuration to ensure only supported operations are used.

    Args:
        config (dict): The configuration containing the restrictions to validate.

    Raises:
        UnsupportedRestrictionError: If an unsupported restriction operation is found.
    """
    supported_operations = ["=", ">", "<", ">=", "<=", "!="]  # Allowed operations

    # Ensure 'tables' exists in config to avoid KeyError
    tables = config.get("tables", [])

    for table in tables:
        restrictions = table.get("restrictions", [])
        if not restrictions:
            continue  # Skip if no restrictions are provided

        for restriction in restrictions:
            operation = restriction.get("operation")
            if operation and operation.lower() not in supported_operations:
                raise UnsupportedRestrictionError(
                    f"Invalid restriction: 'operation={operation}' is not supported."
                )
