"""`table_level_access`: a configured table without `columns` is approved as a whole, so `SELECT *`
is kept. Tables that list `columns` (and every masked table) stay column-level.

Queries the guard lets through (or the `fixed` rewrite) are executed on seeded sqlite data and
fail when a forbidden marker comes back: PW-HIDDEN (an unlisted column of a column-level table),
SSN-REAL (a masked value) or OTHER (a row outside a restriction).
"""

import sqlite3

import pytest

from conftest import verify_sql_test
from sql_data_guard import verify_sql, validate_config
from sql_data_guard.config_suggestion import merge_config, suggest_config

CONFIG = {
    "table_level_access": True,
    "tables": [
        {
            "table_name": "orders",
            "restrictions": [{"column": "region", "value": "eu"}],
        },
        {"table_name": "items"},
        {
            "table_name": "users",
            "columns": ["id", "name", "ssn"],
            "column_masks": [{"column": "ssn", "policy": "redact"}],
        },
    ],
}

_MARKERS = ("PW-HIDDEN", "SSN-REAL", "OTHER")


@pytest.fixture(scope="module")
def cnn():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE orders (id INT, total INT, region TEXT);
        INSERT INTO orders VALUES (1, 10, 'eu'), (2, 20, 'eu'), (3, 30, 'OTHER-us');
        CREATE TABLE items (id INT, order_id INT, sku TEXT);
        INSERT INTO items VALUES (1, 1, 'a'), (2, 2, 'b');
        CREATE TABLE users (id INT, name TEXT, ssn TEXT, pw TEXT);
        INSERT INTO users VALUES (1, 'ann', 'SSN-REAL-1', 'PW-HIDDEN-1');
        """
    )
    return conn


def _run(cnn, sql, config=CONFIG):
    """Verifies sql, runs what the guard lets through and fails if a forbidden marker comes back."""
    result = verify_sql(sql, config, "sqlite")
    assert not [e for e in result["errors"] if e.startswith("Internal error")], result
    to_run = sql if result["allowed"] else result["fixed"]
    if to_run is None:
        return result, None
    rows = cnn.execute(to_run).fetchall()
    for row in rows:
        for value in row:
            for marker in _MARKERS:
                assert marker not in str(value), f"LEAK {marker}: {to_run} -> {rows}"
    return result, rows


def test_select_star_is_kept(cnn):
    verify_sql_test("SELECT * FROM items", CONFIG, cnn=cnn, data=[(1, 1, "a"), (2, 2, "b")])


def test_any_column_of_a_table_level_table_is_allowed():
    verify_sql_test("SELECT id, sku FROM items WHERE order_id = 1 ORDER BY sku", CONFIG)
    verify_sql_test("SELECT i.* FROM items i", CONFIG)


def test_restrictions_still_apply_to_star(cnn):
    result, rows = _run(cnn, "SELECT * FROM orders")
    assert result["allowed"] is False
    assert result["fixed"] == "SELECT * FROM orders WHERE region = 'eu'"
    assert rows == [(1, 10, "eu"), (2, 20, "eu")]


@pytest.mark.parametrize(
    "sql",
    [
        "WITH c AS (SELECT * FROM items) SELECT * FROM c",
        "WITH c AS (SELECT * FROM items) SELECT sku FROM c",
        "SELECT x.sku FROM (SELECT * FROM items) x",
        "SELECT * FROM (SELECT * FROM items) x",
        "SELECT * FROM items UNION SELECT * FROM items",
        "SELECT * FROM items i JOIN items j ON i.id = j.id",
        "SELECT * FROM items WHERE order_id IN (SELECT id FROM items)",
        "SELECT COUNT(*) FROM items",
    ],
)
def test_star_over_cte_subquery_join_and_union(cnn, sql):
    result, rows = _run(cnn, sql)
    assert result["allowed"] and rows is not None


def test_star_over_a_column_level_table_is_still_expanded_with_masks(cnn):
    verify_sql_test(
        "SELECT * FROM users",
        CONFIG,
        errors={"SELECT * is not allowed"},
        fix="SELECT id, name, '****' AS ssn FROM users",
        cnn=cnn,
        data=[(1, "ann", "****")],
    )


def test_column_level_table_still_rejects_unlisted_columns():
    verify_sql_test(
        "SELECT id, pw FROM users",
        CONFIG,
        errors={"Column pw is not allowed. Column removed from SELECT clause"},
        fix="SELECT id FROM users",
    )
    verify_sql_test(
        "SELECT id FROM users WHERE pw = 'x'", CONFIG, errors={"Column pw is not allowed"}
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT u.ssn FROM users u JOIN items i ON i.id = u.id",
        "SELECT ssn FROM users u JOIN items i ON i.id = u.id",
        "SELECT * FROM users u JOIN items i ON i.id = u.id",
        "SELECT i.*, u.ssn FROM users u JOIN items i ON i.id = u.id",
        "SELECT u.* FROM users u JOIN items i ON i.id = u.id",
        "SELECT * FROM items i JOIN users u ON i.id = u.id",
        "SELECT x.pw FROM (SELECT * FROM users) x",
    ],
)
def test_a_column_level_table_is_not_widened_by_a_table_level_one(cnn, sql):
    result, rows = _run(cnn, sql)  # no marker may come back
    assert result["allowed"] is False


def test_star_in_a_mixed_query_is_rewritten_table_by_table(cnn):
    verify_sql_test(
        "SELECT * FROM items i JOIN users u ON i.id = u.id",
        CONFIG,
        errors={"SELECT * is not allowed"},
        fix="SELECT i.*, u.id, u.name, '****' AS ssn FROM items AS i JOIN users AS u ON i.id = u.id",
        cnn=cnn,
        data=[(1, 1, "a", 1, "ann", "****")],
    )


def test_unknown_table_is_rejected():
    verify_sql_test("SELECT * FROM other", CONFIG, errors={"Table other is not allowed"})


def test_without_the_option_columns_are_required():
    config = {"tables": [{"table_name": "items"}]}
    assert validate_config(config) == [
        "Each table must have a 'columns' key with valid column definitions."
    ]


def test_default_is_column_level():
    config = {"tables": [{"table_name": "items", "columns": ["id", "sku"]}]}
    verify_sql_test(
        "SELECT * FROM items",
        config,
        errors={"SELECT * is not allowed"},
        fix="SELECT id, sku FROM items",
    )


@pytest.mark.parametrize("value", ["yes", 1, None, []])
def test_option_must_be_a_bool(value):
    assert validate_config({**CONFIG, "table_level_access": value}) != []


def test_listed_columns_stay_enforced_with_the_option_on():
    config = {
        "table_level_access": True,
        "tables": [{"table_name": "items", "columns": ["id"]}],
    }
    verify_sql_test(
        "SELECT * FROM items",
        config,
        errors={"SELECT * is not allowed"},
        fix="SELECT id FROM items",
    )
    verify_sql_test(
        "SELECT sku FROM items",
        config,
        errors={
            "No legal elements in SELECT clause",
            "Column sku is not allowed. Column removed from SELECT clause",
        },
    )


def test_empty_columns_are_still_invalid_with_the_option_on():
    config = {"table_level_access": True, "tables": [{"table_name": "items", "columns": []}]}
    assert validate_config(config) != []


def test_a_mask_needs_columns_with_the_option_on():
    config = {
        "table_level_access": True,
        "tables": [
            {"table_name": "users", "column_masks": [{"column": "ssn", "policy": "redact"}]}
        ],
    }
    assert validate_config(config) != []


# ---------------------------------------------------------------- suggest_config / merge_config
def test_suggestion_for_an_unknown_table_has_no_columns():
    config = {"table_level_access": True, "tables": [{"table_name": "items"}]}
    delta = suggest_config("SELECT * FROM orders", config, "sqlite")
    assert delta == {"tables": [{"table_name": "orders"}]}
    assert verify_sql("SELECT * FROM orders", merge_config(config, delta), "sqlite")["allowed"]


def test_suggestion_adds_columns_only_to_column_level_tables():
    config = {
        "table_level_access": True,
        "tables": [{"table_name": "items"}, {"table_name": "users", "columns": ["id"]}],
    }
    delta = suggest_config("SELECT id, ssn, name FROM users", config, "sqlite")
    assert delta == {"tables": [{"table_name": "users", "columns": ["ssn", "name"]}]}


def test_suggestion_without_the_option_still_needs_columns():
    config = {"tables": [{"table_name": "items", "columns": ["id"]}]}
    assert suggest_config("SELECT * FROM orders", config, "sqlite") is None


def test_merge_accepts_a_table_without_columns_only_with_the_option():
    with pytest.raises(ValueError):
        merge_config(
            {"tables": [{"table_name": "a", "columns": ["x"]}]}, {"tables": [{"table_name": "b"}]}
        )
    merged = merge_config(CONFIG, {"tables": [{"table_name": "b"}]})
    assert {"table_name": "b"} in merged["tables"]
