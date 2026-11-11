"""Unquoted identifiers are case-insensitive; quoted ones must match the config exactly."""

import pytest

from sql_data_guard import verify_sql

CONFIG = {
    "tables": [
        {
            "table_name": "orders",
            "columns": ["id", "product_name", "account_id"],
            "restrictions": [{"column": "account_id", "value": 123}],
        }
    ]
}


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ID FROM ORDERS WHERE ACCOUNT_ID = 123",
        "SELECT o.ID FROM Orders O WHERE o.Account_Id = 123",
        "SELECT Orders.Product_Name FROM orders WHERE orders.account_id = 123",
        'SELECT "id" FROM "orders" WHERE "account_id" = 123',
        "SELECT id FROM orders WHERE Account_ID = 123 AND ID IN (SELECT id FROM ORDERS WHERE account_id = 123)",
    ],
)
@pytest.mark.parametrize("dialect", ["sqlite", "postgres", "mysql"])
def test_case_variants_are_allowed(sql, dialect):
    if dialect == "mysql":
        sql = sql.replace('"', "`")
    result = verify_sql(sql, CONFIG, dialect)
    assert result["allowed"], result


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT id FROM "Orders" WHERE account_id = 123',  # different table when quoted
        'SELECT "ID" FROM orders WHERE account_id = 123',  # different column when quoted
        'SELECT id FROM orders WHERE "Account_Id" = 123',
        "SELECT id FROM ORDERS",  # still needs the restriction, whatever the case
    ],
)
def test_quoted_mismatch_is_not_treated_as_a_match(sql):
    result = verify_sql(sql, CONFIG, "postgres")
    assert not result["allowed"], result
    assert result["fixed"] is None or '"Orders"' not in result["fixed"]


def test_restriction_found_regardless_of_case():
    result = verify_sql("SELECT id FROM ORDERS WHERE ACCOUNT_ID = 123", CONFIG, "sqlite")
    assert result["allowed"] and result["errors"] == []


def test_missing_restriction_is_injected_for_upper_case_table():
    result = verify_sql("SELECT ID FROM ORDERS", CONFIG, "sqlite")
    assert not result["allowed"]
    assert "account_id = 123" in result["fixed"]
