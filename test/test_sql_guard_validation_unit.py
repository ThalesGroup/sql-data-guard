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


def test_invalid_restriction():
    config = {
        "tables": [
            {
                "table_name": "products",
                "restrictions": [
                    {"column": "price", "value": [80, 150], "operation": "BETWEEN"},
                ],
            }
        ]
    }

    with pytest.raises(
        UnsupportedRestrictionError,
        match="Invalid restriction: 'operation=BETWEEN' is not supported.",
    ):
        validate_restrictions(config)
