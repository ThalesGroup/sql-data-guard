"""Database names: the `databases` allow-list, `strict_database`, `catalog_name`, validate_config."""

import copy

import duckdb
import pytest

from sql_data_guard import merge_config, suggest_config, validate_config, verify_sql

STRICT = {
    "tables": [
        {"table_name": "orders", "database_name": "sales", "columns": ["id", "amount"]},
        {"table_name": "people", "database_name": "hr", "columns": ["id", "name"]},
    ],
    "databases": ["sales", "hr"],
    "strict_database": True,
    "catalog_name": "proj",
}


def _config(**changes):
    config = copy.deepcopy(STRICT)
    for key, value in changes.items():
        if value is None:
            config.pop(key, None)
        else:
            config[key] = value
    return config


def _allowed(sql, config=STRICT, dialect="bigquery"):
    result = verify_sql(sql, config, dialect)
    return result["allowed"], result["errors"]


# ----------------------------------------------------------------------- validate_config
def test_valid_config_has_no_errors():
    assert validate_config(STRICT) == []


@pytest.mark.parametrize(
    "config,error",
    [
        (
            _config(databases=["sales"]),
            "The database 'hr' of table 'people' is not in 'databases'.",
        ),
        (
            _config(databases="sales"),
            "Invalid 'databases'. Expected a list of database names. Received: 'sales'",
        ),
        (
            _config(databases=["sales", ""]),
            "Invalid 'databases'. Expected a list of database names. Received: ['sales', '']",
        ),
        (
            _config(strict_database="yes"),
            "Invalid 'strict_database'. Expected true or false. Received: 'yes'",
        ),
        (
            _config(catalog_name=""),
            "Invalid 'catalog_name'. Expected a non-empty string. Received: ''",
        ),
        (
            {**_config(), "tables": [{"table_name": "orders", "columns": ["id"]}]},
            "Table 'orders' must have a 'database_name' when 'strict_database' is set.",
        ),
        (
            {
                **_config(),
                "tables": [
                    {
                        "table_name": "orders",
                        "database_name": "sales",
                        "catalog_name": 5,
                        "columns": ["id"],
                    }
                ],
            },
            "The 'catalog_name' of table 'orders' must be a non-empty string.",
        ),
        (None, "Invalid configuration provided. The configuration must include 'tables'."),
        ([], "Invalid configuration provided. The configuration must include 'tables'."),
        ({"tables": [None]}, "Each table must be an object."),
    ],
)
def test_invalid_config(config, error):
    assert validate_config(config) == [error]
    # verify_sql reports the same problem as a rejection
    result = verify_sql("SELECT id FROM sales.orders", config)
    assert result == {"allowed": False, "errors": [error], "fixed": None, "risk": 1.0}


def test_validate_config_never_raises():
    class Weird(dict):
        def get(self, *args):
            raise RuntimeError("boom")

    assert validate_config(Weird(tables=[])) == ["Invalid configuration (RuntimeError)"]


def test_validate_config_does_not_modify_the_config():
    config = _config()
    validate_config(config)
    verify_sql("SELECT id FROM orders", config)
    assert config == STRICT


# ----------------------------------------------------------------------- databases
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM sales.orders",
        "SELECT id FROM SALES.orders",
        "SELECT o.id, p.name FROM sales.orders o JOIN hr.people p ON p.id = o.id",
    ],
)
def test_listed_database_is_allowed(sql):
    assert _allowed(sql) == (True, [])


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM finance.orders",
        "SELECT id FROM sales.orders UNION ALL SELECT id FROM finance.orders",
        "SELECT id FROM sales.orders WHERE id IN (SELECT id FROM finance.orders)",
    ],
)
def test_unlisted_database_is_rejected(sql):
    allowed, errors = _allowed(sql)
    assert not allowed
    assert "Database finance is not allowed" in errors


def test_databases_apply_to_tables_without_database_name():
    # without strict mode a table may be configured without a database; it then matches any
    # qualifier, but only the listed databases
    config = {
        "tables": [{"table_name": "orders", "columns": ["id"]}],
        "databases": ["sales"],
    }
    assert _allowed("SELECT id FROM sales.orders", config) == (True, [])
    assert _allowed("SELECT id FROM orders", config) == (True, [])
    assert _allowed("SELECT id FROM finance.orders", config) == (
        False,
        ["Database finance is not allowed"],
    )


# ----------------------------------------------------------------------- strict_database
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM orders",
        "SELECT o.id FROM sales.orders o JOIN people p ON p.id = o.id",
        "SELECT id FROM sales.orders UNION ALL SELECT id FROM people",
        "SELECT id FROM (SELECT id FROM orders) t",
        "SELECT id FROM sales.orders WHERE id IN (SELECT id FROM orders)",
    ],
)
def test_strict_rejects_unqualified_tables(sql):
    allowed, errors = _allowed(sql)
    assert not allowed
    assert any(e.endswith("must be qualified with its database") for e in errors), errors
    assert verify_sql(sql, STRICT, "bigquery")["fixed"] is None


def test_strict_allows_cte_names():
    sql = "WITH orders AS (SELECT id FROM sales.orders) SELECT id FROM orders"
    assert _allowed(sql) == (True, [])


def test_without_strict_unqualified_still_matches():
    assert _allowed("SELECT id FROM orders", _config(strict_database=None)) == (True, [])


# ----------------------------------------------------------------------- catalog
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM proj.sales.orders",
        "SELECT id FROM `proj.sales.orders`",
        "SELECT id FROM `proj`.sales.orders",
        "SELECT id FROM PROJ.sales.orders",
        "SELECT id FROM sales.orders",  # no catalog: the connection's own
    ],
)
def test_matching_catalog_is_allowed(sql):
    assert _allowed(sql) == (True, [])


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM otherproj.sales.orders",
        "SELECT id FROM `otherproj.sales.orders`",
        "SELECT id FROM proj.sales.orders, otherproj.sales.orders",
        "SELECT id FROM proj.sales.orders UNION ALL SELECT id FROM otherproj.sales.orders",
    ],
)
def test_other_catalog_is_rejected(sql):
    allowed, errors = _allowed(sql)
    assert not allowed
    assert "Table orders is not allowed: catalog otherproj is not allowed" in errors


def test_other_catalog_for_an_unknown_table_is_rejected():
    assert _allowed("SELECT x FROM otherproj.sales.newtable") == (
        False,
        ["Table newtable is not allowed: catalog otherproj is not allowed"],
    )


def test_per_table_catalog_overrides_the_default():
    config = _config()
    config["tables"][1]["catalog_name"] = "hrproj"
    assert _allowed("SELECT name FROM hrproj.hr.people", config) == (True, [])
    assert not _allowed("SELECT name FROM proj.hr.people", config)[0]
    assert _allowed("SELECT id FROM proj.sales.orders", config) == (True, [])


def test_catalog_is_ignored_when_not_configured():
    # backward compatible: no catalog_name, no catalog check
    assert _allowed("SELECT id FROM anyproj.sales.orders", _config(catalog_name=None)) == (
        True,
        [],
    )


@pytest.mark.parametrize("dialect", ["tsql", "bigquery", "snowflake", "postgres", None])
def test_names_with_more_than_three_parts_are_rejected(dialect):
    # catalog/db/name would be proj/sales/orders: the `evil` schema would be ignored
    result = verify_sql(
        "SELECT id FROM proj.sales.evil.orders", _config(catalog_name=None), dialect
    )
    assert result["errors"] == [
        "Table proj.sales.evil.orders is not allowed: "
        "names with more than three parts are not supported"
    ]
    assert result["fixed"] is None


# ----------------------------------------------------------------------- suggest / merge
def test_suggest_adds_the_missing_database():
    sql = "SELECT name, salary FROM finance.payroll"
    delta = suggest_config(sql, STRICT, "bigquery")
    assert delta == {
        "tables": [
            {"table_name": "payroll", "database_name": "finance", "columns": ["name", "salary"]}
        ],
        "databases": ["finance"],
    }
    merged = merge_config(STRICT, delta)
    assert merged["databases"] == ["sales", "hr", "finance"]
    assert validate_config(merged) == []
    assert verify_sql(sql, merged, "bigquery")["allowed"]


def test_suggest_adds_a_database_for_a_known_table_name():
    delta = suggest_config("SELECT id FROM finance.orders", STRICT, "bigquery")
    assert delta == {
        "tables": [{"table_name": "orders", "columns": ["id"], "database_name": "finance"}],
        "databases": ["finance"],
    }


def test_suggest_without_databases_list_does_not_add_one():
    config = _config(databases=None, strict_database=None)
    delta = suggest_config("SELECT x FROM finance.payroll", config, "bigquery")
    assert delta == {
        "tables": [{"table_name": "payroll", "database_name": "finance", "columns": ["x"]}]
    }


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM otherproj.sales.orders",  # catalog mismatch
        "SELECT x FROM otherproj.finance.payroll",
        "SELECT id FROM orders",  # unqualified in strict mode
        "SELECT x FROM payroll",
        "SELECT id FROM proj.sales.evil.orders",
    ],
)
def test_suggest_returns_none_when_widening_cannot_help(sql):
    assert suggest_config(sql, STRICT, "bigquery") is None


def test_merge_databases():
    merged = merge_config(STRICT, {"tables": [], "databases": ["HR", "finance"]})
    assert merged["databases"] == ["sales", "hr", "finance"]
    created = merge_config({"tables": []}, {"tables": [], "databases": ["sales"]})
    assert created["databases"] == ["sales"]
    assert STRICT["databases"] == ["sales", "hr"]  # not modified


@pytest.mark.parametrize(
    "delta",
    [
        {"databases": ["x"]},
        {"tables": [], "databases": "x"},
        {"tables": [], "databases": [""]},
        {"tables": [], "databases": [1]},
        {"tables": [], "catalog_name": "x"},
        {"tables": [], "strict_database": False},
    ],
)
def test_merge_rejects_malformed_delta(delta):
    with pytest.raises(ValueError):
        merge_config(STRICT, delta)


# ----------------------------------------------------------------------- executed on duckdb
@pytest.fixture(scope="module")
def two_catalogs():
    conn = duckdb.connect()
    for catalog, marker in [("proj", "mine"), ("otherproj", "OTHER")]:
        conn.execute(f"ATTACH ':memory:' AS {catalog}")
        conn.execute(f"CREATE SCHEMA {catalog}.sales")
        conn.execute(f"CREATE TABLE {catalog}.sales.orders (id INT, amount TEXT)")
        conn.execute(f"INSERT INTO {catalog}.sales.orders VALUES (1, '{marker}')")
    yield conn
    conn.close()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT amount FROM proj.sales.orders",
        "SELECT amount FROM otherproj.sales.orders",
        "SELECT amount FROM proj.sales.orders UNION ALL SELECT amount FROM otherproj.sales.orders",
        "SELECT o.amount FROM otherproj.sales.orders o",
        'SELECT amount FROM "otherproj"."sales"."orders"',
        "SELECT amount FROM (SELECT amount FROM otherproj.sales.orders) t",
        "WITH c AS (SELECT amount FROM otherproj.sales.orders) SELECT amount FROM c",
    ],
)
def test_other_catalog_data_never_comes_back(two_catalogs, sql):
    result = verify_sql(sql, STRICT, "duckdb")
    to_run = sql if result["allowed"] else result["fixed"]
    if to_run is None:
        return
    rows = two_catalogs.execute(to_run).fetchall()
    assert all("OTHER" not in str(r) for r in rows), (to_run, rows)
    assert rows == [("mine",)]
