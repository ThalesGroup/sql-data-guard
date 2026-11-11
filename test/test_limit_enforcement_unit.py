"""force_limit: the outermost query may not return more rows than the configured cap."""

import sqlite3

import pytest

from sql_data_guard import verify_sql


def _config(cap):
    return {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id", "product_name", "account_id"],
                "restrictions": [{"column": "account_id", "value": 123}],
            }
        ],
        "force_limit": cap,
    }


@pytest.fixture(scope="module")
def cnn():
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE orders (id INT, product_name TEXT, account_id INT)")
        for i in range(20):
            conn.execute("INSERT INTO orders VALUES (?, 'p', 123)", (i,))
        yield conn


def _rows(cnn, result, sql):
    run = sql if result["allowed"] else result["fixed"]
    return cnn.execute(run).fetchall()


def test_limit_is_added(cnn):
    sql = "SELECT id FROM orders WHERE account_id = 123"
    result = verify_sql(sql, _config(5), "sqlite")
    assert not result["allowed"]
    assert result["fixed"] == "SELECT id FROM orders WHERE account_id = 123 LIMIT 5"
    assert result["errors"] == ["Row limit enforced: LIMIT 5"]
    assert len(_rows(cnn, result, sql)) == 5


def test_larger_limit_is_clamped():
    result = verify_sql("SELECT id FROM orders WHERE account_id = 123 LIMIT 1000", _config(5), "sqlite")
    assert result["fixed"] == "SELECT id FROM orders WHERE account_id = 123 LIMIT 5"


@pytest.mark.parametrize("limit", ["5", "3", "0"])
def test_limit_within_cap_is_untouched(limit):
    result = verify_sql(f"SELECT id FROM orders WHERE account_id = 123 LIMIT {limit}", _config(5), "sqlite")
    assert result["allowed"], result
    assert result["fixed"] is None


def test_offset_is_kept():
    result = verify_sql("SELECT id FROM orders WHERE account_id = 123 LIMIT 100 OFFSET 7", _config(5), "sqlite")
    assert result["fixed"] == "SELECT id FROM orders WHERE account_id = 123 LIMIT 5 OFFSET 7"


@pytest.mark.parametrize(
    "limit,dialect",
    [
        ("LIMIT -1", "sqlite"),  # unlimited in SQLite
        ("LIMIT NULL", "sqlite"),
        ("LIMIT ALL", "postgres"),  # unlimited in PostgreSQL
        ("LIMIT 1 + 1000000", "sqlite"),
        ("LIMIT (SELECT 1000000)", "postgres"),
        ("LIMIT 100 * 100", "mysql"),
        ("FETCH FIRST 1000 ROWS ONLY", "postgres"),
    ],
)
def test_limits_that_are_not_a_plain_integer_are_replaced(limit, dialect):
    result = verify_sql(f"SELECT id FROM orders WHERE account_id = 123 {limit}", _config(5), dialect)
    assert not result["allowed"], result
    assert result["fixed"] is not None and "LIMIT 5" in result["fixed"], result
    assert "1000" not in result["fixed"] and "ALL" not in result["fixed"] and "-1" not in result["fixed"]


def test_tsql_top_is_clamped():
    result = verify_sql("SELECT TOP 1000 id FROM orders WHERE account_id = 123", _config(5), "tsql")
    assert result["fixed"] == "SELECT TOP 5 id FROM orders WHERE account_id = 123"


def test_union_is_capped_as_a_whole(cnn):
    sql = "SELECT id FROM orders WHERE account_id = 123 UNION SELECT id FROM orders WHERE account_id = 123"
    result = verify_sql(sql, _config(3), "sqlite")
    assert result["fixed"].endswith("LIMIT 3")
    assert len(_rows(cnn, result, sql)) == 3


def test_only_the_outermost_query_is_capped():
    sql = "SELECT id FROM (SELECT id FROM orders WHERE account_id = 123) t"
    result = verify_sql(sql, _config(5), "sqlite")
    assert result["fixed"] == "SELECT id FROM (SELECT id FROM orders WHERE account_id = 123) AS t LIMIT 5"


def test_limit_is_combined_with_other_fixes():
    result = verify_sql("SELECT id FROM orders", _config(5), "sqlite")
    assert result["fixed"] == "SELECT id FROM orders WHERE account_id = 123 LIMIT 5"
    assert len(result["errors"]) == 2


def test_not_applied_when_the_query_is_rejected():
    result = verify_sql("SELECT id FROM orders WHERE account_id = 123; DROP TABLE orders", _config(5), "sqlite")
    assert not result["allowed"] and result["fixed"] is None


def test_no_force_limit_means_no_change():
    config = _config(5)
    del config["force_limit"]
    result = verify_sql("SELECT id FROM orders WHERE account_id = 123", config, "sqlite")
    assert result["allowed"] and result["fixed"] is None


@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "10", None, [5]])
def test_invalid_force_limit_is_a_config_error(bad):
    result = verify_sql("SELECT id FROM orders WHERE account_id = 123", _config(bad), "sqlite")
    assert not result["allowed"] and result["fixed"] is None
    assert "force_limit" in result["errors"][0]


SQLITE_LIMIT_SHAPES = [
    "",
    "LIMIT -1",
    "LIMIT NULL",
    "LIMIT 1000000",
    "LIMIT 9223372036854775807",
    "LIMIT 5 + 1000",
    "LIMIT (SELECT 1000000)",
    "LIMIT 1000 OFFSET 0",
    "LIMIT 5 OFFSET -1",
    "LIMIT 2, 1000",
    "LIMIT 1e9",
    "LIMIT 0x3E8",
    "LIMIT '1000'",
    "LIMIT CAST(1000 AS INT)",
    "limit  /* c */ 1000",
]


@pytest.mark.parametrize("tail", SQLITE_LIMIT_SHAPES)
@pytest.mark.parametrize(
    "template",
    [
        "SELECT id FROM orders WHERE account_id = 123 {tail}",
        "SELECT id FROM orders WHERE account_id = 123 ORDER BY id {tail}",
        "SELECT id FROM orders WHERE account_id = 123 UNION ALL SELECT id FROM orders WHERE account_id = 123 {tail}",
        "WITH c AS (SELECT id FROM orders WHERE account_id = 123) SELECT id FROM c {tail}",
        "SELECT id FROM (SELECT id FROM orders WHERE account_id = 123 LIMIT 1000) t {tail}",
        "SELECT id FROM orders WHERE account_id = 123 AND id IN (SELECT id FROM orders WHERE account_id = 123 LIMIT 1000) {tail}",
    ],
)
def test_executed_row_count_never_exceeds_the_cap(cnn, template, tail):
    cap = 4
    sql = template.format(tail=tail)
    result = verify_sql(sql, _config(cap), "sqlite")
    if not result["allowed"] and result["fixed"] is None:
        return  # rejected outright
    run = sql if result["allowed"] else result["fixed"]
    try:
        rows = cnn.execute(run).fetchall()
    except sqlite3.Error:
        return  # does not run on this engine
    assert len(rows) <= cap, f"{len(rows)} rows from: {run}"
