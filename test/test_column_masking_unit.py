"""Column masking: a masked column is returned masked, and can be used in no other way."""

import sqlite3

import pytest

from sql_data_guard import verify_sql

RAW = "RAW-SSN"


def _config(**mask):
    mask = dict({"column": "ssn", "policy": "redact"}, **mask)
    return {
        "tables": [
            {
                "table_name": "users",
                "columns": ["id", "name", "ssn"],
                "restrictions": [{"column": "id", "value": 1}],
                "column_masks": [mask],
            },
            {
                "table_name": "orders",
                "columns": ["id", "user_id", "ssn"],  # same column name, NOT masked here
                "restrictions": [{"column": "id", "value": 1}],
            },
        ]
    }


@pytest.fixture(scope="module")
def cnn():
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE users (id INT, name TEXT, ssn TEXT)")
        conn.execute(f"INSERT INTO users VALUES (1, 'ann', '{RAW}-1')")
        conn.execute("CREATE TABLE orders (id INT, user_id INT, ssn TEXT)")
        conn.execute("INSERT INTO orders VALUES (1, 1, 'ORDER-SSN')")
        yield conn


def _verify(sql, config=None, dialect="sqlite"):
    return verify_sql(sql, config or _config(), dialect)


def _run(cnn, sql, config=None, dialect="sqlite"):
    """Runs what the caller would run: the query itself if allowed, else the fixed one."""
    result = _verify(sql, config, dialect)
    run = sql if result["allowed"] else result["fixed"]
    return result, (cnn.execute(run).fetchall() if run else None)


# ---------------------------------------------------------------- selecting a masked column
@pytest.mark.parametrize(
    "sql,fixed",
    [
        ("SELECT id, ssn FROM users WHERE id = 1", "SELECT id, '****' AS ssn FROM users WHERE id = 1"),
        ("SELECT u.ssn FROM users u WHERE u.id = 1", "SELECT '****' AS ssn FROM users AS u WHERE u.id = 1"),
        ("SELECT users.ssn FROM users WHERE id = 1", "SELECT '****' AS ssn FROM users WHERE id = 1"),
        ("SELECT ssn AS social FROM users WHERE id = 1", "SELECT '****' AS social FROM users WHERE id = 1"),
        ("SELECT SSN FROM users WHERE id = 1", "SELECT '****' AS SSN FROM users WHERE id = 1"),
        ("SELECT * FROM users WHERE id = 1", "SELECT id, name, '****' AS ssn FROM users WHERE id = 1"),
    ],
)
def test_selected_masked_column_is_rewritten_and_keeps_its_name(cnn, sql, fixed):
    result, rows = _run(cnn, sql)
    assert not result["allowed"]
    assert result["fixed"] == fixed
    assert rows and all(RAW not in str(v) for row in rows for v in row)


def test_result_shape_is_preserved(cnn):
    result = _verify("SELECT id, name, ssn FROM users WHERE id = 1")
    cursor = cnn.execute(result["fixed"])
    assert [d[0] for d in cursor.description] == ["id", "name", "ssn"]
    assert cursor.fetchall() == [(1, "ann", "****")]


def test_custom_replacement_and_escaping(cnn):
    result = _verify("SELECT ssn FROM users WHERE id = 1", _config(replacement="o'brien"))
    assert result["fixed"] == "SELECT 'o''brien' AS ssn FROM users WHERE id = 1"
    assert cnn.execute(result["fixed"]).fetchall() == [("o'brien",)]


def test_fixed_query_is_accepted_when_verified_again():
    fixed = _verify("SELECT id, ssn FROM users WHERE id = 1")["fixed"]
    assert _verify(fixed)["allowed"]


def test_column_that_is_not_masked_is_untouched():
    assert _verify("SELECT id, name FROM users WHERE id = 1")["allowed"]


def test_same_column_name_on_a_table_without_a_mask_is_not_masked(cnn):
    result, rows = _run(cnn, "SELECT o.ssn FROM orders o WHERE o.id = 1")
    assert result["allowed"] and rows == [("ORDER-SSN",)]


def test_qualifier_decides_which_table_is_meant(cnn):
    sql = "SELECT u.ssn AS a, o.ssn AS b FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = 1 AND o.id = 1"
    result, rows = _run(cnn, sql)
    assert rows == [("****", "ORDER-SSN")]


def test_unqualified_ambiguous_column_is_masked(cnn):
    sql = "SELECT u.id, ssn FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = 1 AND o.id = 1"
    result = _verify(sql)
    assert "'****' AS ssn" in result["fixed"]


# ---------------------------------------------------------------- derived tables, CTEs, UNION
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT t.ssn FROM (SELECT ssn FROM users WHERE id = 1) t",
        "SELECT ssn FROM (SELECT ssn FROM users WHERE id = 1)",
        "WITH c AS (SELECT ssn FROM users WHERE id = 1) SELECT ssn FROM c",
        "WITH c AS (SELECT * FROM users WHERE id = 1) SELECT ssn FROM c",
        "SELECT (SELECT ssn FROM users WHERE id = 1) AS x",
        "SELECT ssn FROM users WHERE id = 1 UNION ALL SELECT ssn FROM users WHERE id = 1",
        "SELECT id FROM users WHERE id = 1 AND id IN (SELECT id FROM (SELECT id, ssn FROM users WHERE id = 1) t)",
    ],
)
def test_masking_is_applied_where_the_base_table_is_selected(cnn, sql):
    result, rows = _run(cnn, sql)
    assert rows is not None
    assert all(RAW not in str(v) for row in rows for v in row), (sql, rows, result)


# ---------------------------------------------------------------- every other use is rejected
REJECTED = [
    "SELECT lower(ssn) FROM users WHERE id = 1",
    "SELECT upper(u.ssn) FROM users u WHERE u.id = 1",
    "SELECT substr(ssn, 1, 3) FROM users WHERE id = 1",
    "SELECT length(ssn) FROM users WHERE id = 1",
    "SELECT ssn || 'x' FROM users WHERE id = 1",
    "SELECT CAST(ssn AS TEXT) FROM users WHERE id = 1",
    "SELECT CASE WHEN ssn = 'x' THEN 1 ELSE 0 END FROM users WHERE id = 1",
    "SELECT coalesce(ssn, 'n') FROM users WHERE id = 1",
    "SELECT count(ssn) FROM users WHERE id = 1",
    "SELECT max(ssn) FROM users WHERE id = 1",
    "SELECT (ssn) FROM users WHERE id = 1",
    "SELECT ssn, ssn || '' FROM users WHERE id = 1",
    "SELECT row_number() OVER (PARTITION BY ssn) FROM users WHERE id = 1",
    "SELECT id FROM users WHERE id = 1 AND ssn LIKE '%'",
    "SELECT id FROM users WHERE id = 1 AND ssn = 'RAW-SSN-1'",
    "SELECT id FROM users WHERE id = 1 AND substr(ssn, 1, 1) = 'R'",
    "SELECT id FROM users WHERE id = 1 AND ssn IS NOT NULL",
    "SELECT id FROM users WHERE id = 1 ORDER BY ssn",
    "SELECT id FROM users WHERE id = 1 GROUP BY id, ssn",
    "SELECT id FROM users WHERE id = 1 GROUP BY id HAVING max(ssn) > 'A'",
    "SELECT u.id FROM users u JOIN orders o ON o.ssn = u.ssn WHERE u.id = 1 AND o.id = 1",
    "SELECT u.id FROM users u JOIN orders o USING (ssn) WHERE u.id = 1 AND o.id = 1",
    "SELECT id FROM users WHERE id = 1 AND EXISTS (SELECT 1 FROM orders o WHERE o.id = 1 AND users.ssn LIKE 'R%')",
    "SELECT u.id FROM users u WHERE u.id = 1 AND EXISTS (SELECT 1 FROM orders o WHERE o.id = 1 AND u.ssn LIKE 'R%')",
    "SELECT u.id FROM users u WHERE u.id = 1 AND (SELECT count(*) FROM orders o WHERE o.id = 1 AND u.ssn = 'x') = 1",
    "SELECT id FROM users WHERE id = 1 AND id IN (SELECT id FROM users WHERE id = 1 AND ssn LIKE 'R%')",
    "WITH c AS (SELECT id FROM users WHERE id = 1 AND ssn LIKE 'R%') SELECT id FROM c",
    "SELECT t.id FROM (SELECT id FROM users WHERE id = 1 AND ssn LIKE 'R%') t",
    "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM users WHERE id = 1 AND ssn LIKE 'R%'",
    "SELECT id FROM users WHERE id = 1 AND SSN LIKE 'R%'",
]


@pytest.mark.parametrize("sql", REJECTED)
def test_masked_column_used_any_other_way_is_rejected(cnn, sql):
    result = _verify(sql)
    assert not result["allowed"], result
    assert result["fixed"] is None, result
    assert any("masked" in e for e in result["errors"]), result


# ---------------------------------------------------------------- the raw value never comes back
SHAPES = REJECTED + [
    "SELECT ssn FROM users WHERE id = 1",
    "SELECT * FROM users WHERE id = 1",
    "SELECT u.* FROM users u WHERE u.id = 1",
    "SELECT users.* FROM users WHERE id = 1",
    "SELECT ssn AS id FROM users WHERE id = 1",
    "SELECT ssn AS ssn FROM users WHERE id = 1",
    "SELECT ssn, ssn AS other FROM users WHERE id = 1",
    "SELECT DISTINCT ssn FROM users WHERE id = 1",
    "SELECT ssn FROM users WHERE id = 1 ORDER BY 1",
    "SELECT ssn FROM users u2 WHERE u2.id = 1",
    "SELECT main.users.ssn FROM users WHERE id = 1",
    'SELECT "ssn" FROM users WHERE id = 1',
    "SELECT json_object('s', ssn) FROM users WHERE id = 1",
    "SELECT group_concat(ssn) FROM users WHERE id = 1",
    "SELECT x.ssn FROM (SELECT * FROM users WHERE id = 1) x",
    "SELECT ssn FROM users WHERE id = 1 EXCEPT SELECT 'a'",
    "SELECT (SELECT u.ssn FROM orders o WHERE o.id = 1) FROM users u WHERE u.id = 1",
    "SELECT u.id, (SELECT o.ssn FROM orders o WHERE o.id = 1) FROM users u WHERE u.id = 1",
]


@pytest.mark.parametrize("sql", SHAPES)
def test_raw_value_is_never_returned(cnn, sql):
    result = _verify(sql)
    run = sql if result["allowed"] else result["fixed"]
    if run is None:
        return  # rejected
    try:
        rows = cnn.execute(run).fetchall()
    except sqlite3.Error:
        return
    assert all(RAW not in str(v) for row in rows for v in row), (sql, run, rows)


# ---------------------------------------------------------------- configuration
@pytest.mark.parametrize(
    "masks",
    [
        "ssn",
        [{"policy": "redact"}],
        [{"column": "", "policy": "redact"}],
        [{"column": "nope", "policy": "redact"}],  # not in columns
        [{"column": "ssn", "policy": "hash"}],  # not supported (yet)
        [{"column": "ssn"}],
        [{"column": "ssn", "policy": "redact", "replacement": 5}],
        [{"column": "ssn", "policy": "redact"}, {"column": "SSN", "policy": "redact"}],  # twice
        ["ssn"],
    ],
)
def test_invalid_mask_is_a_config_error(masks):
    config = _config()
    config["tables"][0]["column_masks"] = masks
    result = verify_sql("SELECT id FROM users WHERE id = 1", config, "sqlite")
    assert not result["allowed"] and result["fixed"] is None
    assert result["errors"]


def test_masked_column_cannot_also_be_restricted():
    config = _config()
    config["tables"][0]["restrictions"] = [{"column": "ssn", "value": "x"}]
    result = verify_sql("SELECT id FROM users", config, "sqlite")
    assert not result["allowed"] and "masked and restricted" in result["errors"][0]


def test_no_masks_means_no_change():
    config = _config()
    del config["tables"][0]["column_masks"]
    assert verify_sql("SELECT id, ssn FROM users WHERE id = 1", config, "sqlite")["allowed"]
