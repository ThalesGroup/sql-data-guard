"""`qualify_tables`: unqualified names of tables configured with a database_name are qualified."""

import sqlite3

import pytest
from conftest import verify_sql_test

from sql_data_guard import verify_sql


def _config(qualify=True, **extra):
    config = {
        "tables": [
            {"table_name": "reviews", "database_name": "hr", "columns": ["id", "title"]},
            {"table_name": "orders", "columns": ["id"]},
        ],
        **extra,
    }
    if qualify is not None:
        config["qualify_tables"] = qualify
    return config


@pytest.fixture(scope="module")
def cnn():
    with sqlite3.connect(":memory:") as conn:
        conn.execute("ATTACH DATABASE ':memory:' AS hr")
        conn.execute("CREATE TABLE hr.reviews (id INT, title TEXT)")
        conn.execute("INSERT INTO hr.reviews VALUES (1, 'hr-review')")
        # same name in the default schema
        conn.execute("CREATE TABLE reviews (id INT, title TEXT)")
        conn.execute("INSERT INTO reviews VALUES (1, 'main-review')")
        conn.execute("CREATE TABLE orders (id INT)")
        conn.execute("INSERT INTO orders VALUES (7)")
        yield conn


_QUALIFIED = "Table reviews qualified with its database: hr.reviews"


def test_unqualified_table_is_qualified(cnn):
    verify_sql_test(
        "SELECT title FROM reviews",
        _config(),
        errors={_QUALIFIED},
        fix="SELECT title FROM hr.reviews",
        cnn=cnn,
        data=[("hr-review",)],
    )


def test_alias_and_qualified_columns_are_kept(cnn):
    verify_sql_test(
        "SELECT r.title, reviews.id FROM reviews AS r JOIN reviews ON reviews.id = r.id",
        _config(),
        errors={_QUALIFIED},
        fix="SELECT r.title, reviews.id FROM hr.reviews AS r JOIN hr.reviews ON reviews.id = r.id",
        cnn=cnn,
        data=[("hr-review", 1)],
    )


def test_qualified_in_sub_queries_and_unions(cnn):
    verify_sql_test(
        "SELECT title FROM (SELECT title FROM reviews) t UNION SELECT title FROM hr.reviews",
        _config(),
        errors={_QUALIFIED},
        fix="SELECT title FROM (SELECT title FROM hr.reviews) AS t UNION SELECT title FROM hr.reviews",
        cnn=cnn,
        data=[("hr-review",)],
    )


def test_already_qualified_and_tables_without_database_are_unchanged(cnn):
    verify_sql_test(
        "SELECT r.title, o.id FROM hr.reviews r, orders o", _config(), cnn=cnn
    )


def test_cte_with_the_same_name_is_not_qualified(cnn):
    verify_sql_test(
        "WITH reviews AS (SELECT 1 AS id, 'cte' AS title) SELECT title FROM reviews",
        _config(),
        cnn=cnn,
        data=[("cte",)],
    )


def test_table_also_configured_without_database_is_unchanged():
    config = _config()
    config["tables"].append({"table_name": "reviews", "columns": ["id", "title"]})
    result = verify_sql("SELECT title FROM reviews", config)
    assert result["allowed"], result


def test_table_configured_in_two_databases_is_ambiguous():
    config = _config()
    config["tables"].append(
        {"table_name": "reviews", "database_name": "hr2", "columns": ["id", "title"]}
    )
    result = verify_sql("SELECT title FROM reviews", config)
    assert result == {
        "allowed": False,
        "errors": ["Table reviews is ambiguous: qualify it with its database"],
        "fixed": None,
        "risk": 0.7,
    }


@pytest.mark.parametrize("qualify", [None, False])
def test_off_by_default(qualify):
    # without the option the name is used as it is (backward compatible)
    result = verify_sql("SELECT title FROM reviews", _config(qualify))
    assert result["allowed"] and result["fixed"] is None, result


@pytest.mark.parametrize("value", ["yes", 1])
def test_invalid_value_is_a_config_error(value):
    result = verify_sql("SELECT title FROM reviews", _config(value))
    assert result["errors"] == [
        f"Invalid 'qualify_tables'. Expected true or false. Received: {value!r}"
    ]


def test_dialect_quoting():
    config = _config()
    config["tables"][0]["database_name"] = "Human Resources"
    result = verify_sql("SELECT title FROM reviews", config, "mysql")
    assert result["fixed"] == "SELECT title FROM `Human Resources`.reviews", result
