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


def _products(**restriction):
    return {
        "tables": [
            {
                "table_name": "products",
                "columns": ["id", "price"],
                "restrictions": [{"column": "price", **restriction}],
            }
        ]
    }


@pytest.mark.parametrize(
    "config",
    [
        {
            "tables": [
                {"table_name": "products", "columns": ["id"], "restrictions": None}
            ]
        },
        {
            "tables": [
                {"table_name": "products", "columns": ["id"], "restrictions": ["price"]}
            ]
        },
        {"tables": [{"table_name": "products", "columns": ["id", 5]}]},
        {"tables": [{"table_name": 5, "columns": ["id"]}]},
        {"tables": [{"table_name": "products", "columns": ["id"], "database_name": 5}]},
        {"tables": [{"table_name": "products", "columns": ["id"]}], "max_length": "x"},
        {"tables": [{"table_name": "products", "columns": ["id"]}], "max_length": 0},
        {"tables": {"table_name": "products", "columns": ["id"]}},
        _products(),
        _products(value=None),
        _products(value=[1]),
        _products(value=float("nan")),
        _products(values=[]),
        _products(value=1, operation=5),
        _products(value="1", operation=">"),
        _products(value=True, operation=">"),
        _products(values=[1, True], operation="BETWEEN"),
        _products(values=[True], operation="IN"),
    ],
)
def test_malformed_config_is_a_config_error(config):
    with pytest.raises((ValueError, UnsupportedRestrictionError)):
        validate_restrictions(config)


@pytest.mark.parametrize(
    "column", ["", "(", "price; DROP TABLE x", "p.price", "price x", "1"]
)
def test_restriction_column_must_be_a_column_name(column):
    config = _products(value=1)
    config["tables"][0]["restrictions"][0]["column"] = column
    with pytest.raises(ValueError, match="must have a 'column'"):
        validate_restrictions(config)


@pytest.mark.parametrize(
    "restriction",
    [
        {"value": 1},
        {"value": "a"},
        {"value": True},
        {"values": [1, 2]},
        {"values": [1, 2], "operation": "IN"},
        {"value": 1.5, "operation": ">="},
    ],
)
def test_valid_restriction_shapes(restriction):
    validate_restrictions(_products(**restriction))


def test_equality_with_several_values_is_injected_as_in():
    from conftest import verify_sql_test

    verify_sql_test(
        "SELECT id FROM products",
        _products(values=[1, 2]),
        errors={"Missing restriction for table: products column: price value: [1, 2]"},
        fix="SELECT id FROM products WHERE price IN (1, 2)",
    )


@pytest.mark.parametrize(
    "where",
    [
        "price = 3",  # `=` restriction with several values: only those values
        "price IN (1, 3)",
        "price IN (SELECT 1)",  # a sub-query could return anything
        "price = id",  # a column, even one named like an allowed value
        "price = 1 + 1",
        "price BETWEEN 1 AND 2",
    ],
)
def test_restriction_is_only_satisfied_by_allowed_literals(where):
    from conftest import verify_sql_test

    verify_sql_test(
        f"SELECT id FROM products WHERE {where}",
        _products(values=[1, 2]),
        errors={"Missing restriction for table: products column: price value: [1, 2]"},
        fix=f"SELECT id FROM products WHERE ({where}) AND price IN (1, 2)",
    )


@pytest.mark.parametrize(
    "where,satisfied",
    [
        ("price < 10", True),
        ("price < 5", True),
        ("price < (5)", True),
        ("price < 11", False),
        ("price = 10", False),  # equal to the limit of `< 10`: outside it
        ("price = 9", True),
        ("price IN (1, 10)", False),
        ("price IN (1, 9)", True),
        ("price BETWEEN 1 AND 9", True),
        ("price BETWEEN 1 AND 10", False),
        ("price < 'abc'", False),
        ("price < -1", True),
        ("price <= 9", True),
        ("price <= 10", False),
        ("price > 1", False),
        (
            "10 > price",
            False,
        ),  # the column must be on the left (the condition is added)
    ],
)
def test_comparison_restriction(where, satisfied):
    # restriction: price < 10
    from sql_data_guard import verify_sql

    result = verify_sql(
        f"SELECT id FROM products WHERE {where}", _products(value=10, operation="<")
    )
    assert result["allowed"] is satisfied, result


@pytest.mark.parametrize(
    "operation,where,satisfied",
    [
        (">", "price > 10", True),
        (">", "price > 11", True),
        (">", "price >= 10", False),
        (">", "price >= 11", True),
        (">", "price = 10", False),
        (">=", "price >= 10", True),
        (">=", "price > 9", False),  # 9.5 > 9 but not >= 10
        (">=", "price > 10", True),
        (">=", "price >= 9", False),
        (">=", "price BETWEEN 10 AND 100", True),
        (">=", "price BETWEEN 9 AND 100", False),
        ("<=", "price <= 10", True),
        ("<=", "price < 11", False),  # 10.5 < 11 but not <= 10
        ("<=", "price < 10", True),
        ("<=", "price = 10", True),
    ],
)
def test_comparison_restriction_bounds(operation, where, satisfied):
    from sql_data_guard import verify_sql

    result = verify_sql(
        f"SELECT id FROM products WHERE {where}",
        _products(value=10, operation=operation),
    )
    assert result["allowed"] is satisfied, result
