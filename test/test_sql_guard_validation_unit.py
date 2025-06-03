import pytest

from sql_data_guard.restriction_validation import (
    validate_restrictions,
    UnsupportedRestrictionError,
)


def test_valid_restrictions():
    config = {
        "tables": [
            {
                "table_name": "products",
                "columns": ["price", "category"],
                "restrictions": [
                    {"column": "price", "value": 100, "operation": ">="},
                    {"column": "category", "value": "A", "operation": "="},
                ],
            }
        ]
    }

    try:
        validate_restrictions(config)
    except UnsupportedRestrictionError as e:
        pytest.fail(f"Unexpected error: {e}")


def test_valid_between_restriction():
    config = {
        "tables": [
            {
                "table_name": "products",
                "columns": ["price"],
                "restrictions": [
                    {"column": "price", "values": [80, 150], "operation": "BETWEEN"},
                ],
            }
        ]
    }
    validate_restrictions(config)


def test_invalid_between_restriction():
    config = {
        "tables": [
            {
                "table_name": "products",
                "columns": ["price"],
                "restrictions": [
                    {"column": "price", "values": [150, 80], "operation": "BETWEEN"},
                ],
            }
        ]
    }
    with pytest.raises(ValueError):
        validate_restrictions(config)


# Test to ensure there is at least one table
def test_no_tables():
    config = {"tables": []}

    with pytest.raises(
        ValueError,
        match="Configuration must contain at least one table.",
    ):
        validate_restrictions(config)


# Test to ensure each table has a `table_name`
def test_missing_table_name():
    config = {
        "tables": [
            {
                "database_name": "orders_db",
                "columns": ["prod_id", "prod_name", "prod_category", "price"],
                "restrictions": [
                    {"column": "price", "value": 100, "operation": ">="},
                ],
            }
        ]
    }

    with pytest.raises(
        ValueError,
        match="Each table must have a 'table_name' key.",
    ):
        validate_restrictions(config)


# Test to ensure there are columns defined for the table
def test_missing_columns():
    config = {
        "tables": [
            {
                "table_name": "products",
                "database_name": "orders_db",
                "restrictions": [
                    {"column": "price", "value": 100, "operation": ">="},
                ],
            }
        ]
    }

    with pytest.raises(
        ValueError,
        match="Each table must have a 'columns' key with valid column definitions.",
    ):
        validate_restrictions(config)


# Test to validate the restriction operation is supported
def test_unsupported_restriction_operation():
    config = {
        "tables": [
            {
                "table_name": "products",
                "columns": ["price"],  # Add columns key here
                "restrictions": [
                    {"column": "price", "value": 100, "operation": "NotSupported"},
                ],
            }
        ]
    }

    with pytest.raises(
        UnsupportedRestrictionError,
        match="Invalid restriction: 'operation=NotSupported' is not supported.",
    ):
        validate_restrictions(config)


def test_valid_greater_than_equal_restriction():
    config = {
        "tables": [
            {
                "table_name": "products",  # Table name
                "columns": ["price"],  # Column name
                "restrictions": [
                    {
                        "column": "price",  # Column name
                        "value": 100,  # Value to compare
                        "operation": ">=",  # 'Greater than or equal' operation
                    },
                ],
            }
        ]
    }

    try:
        validate_restrictions(config)
    except UnsupportedRestrictionError as e:
        pytest.fail(f"Unexpected error: {e}")


def test_valid_greater_than_equal_with_float_value():
    config = {
        "tables": [
            {
                "table_name": "products",  # Table name
                "columns": ["price"],  # Column name
                "restrictions": [
                    {
                        "column": "price",  # Column name
                        "value": 99.99,  # Float value
                        "operation": ">=",  # 'Greater than or equal' operation
                    },
                ],
            }
        ]
    }

    try:
        validate_restrictions(config)
    except UnsupportedRestrictionError as e:
        pytest.fail(f"Unexpected error: {e}")
