"""SHOW / DESCRIBE metadata statements, allowed with `allow_metadata`."""

import copy

import duckdb
import pytest

from sql_data_guard import suggest_config, validate_config, verify_sql

CONFIG = {
    "tables": [
        {"table_name": "orders", "database_name": "sales", "columns": ["id"]},
        {"table_name": "v_orders", "database_name": "sales", "columns": ["id"]},
    ],
    "databases": ["sales"],
    "catalog_name": "proj",
    "allow_metadata": True,
}
WITHOUT = {k: v for k, v in CONFIG.items() if k != "allow_metadata"}
STRICT = {**CONFIG, "strict_database": True}

DIALECTS = [
    "mysql", "trino", "athena", "snowflake", "databricks", "duckdb", "postgres", "bigquery",
    "hive", "spark", None,
]

ALLOWED = [
    "SHOW TABLES",
    "SHOW TABLES FROM sales",
    "show tables in sales;",
    "SHOW FULL TABLES IN sales LIKE 'o%'",
    "SHOW TABLES FROM sales -- a comment\n",
    "SHOW VIEWS FROM proj.sales",
    "SHOW SCHEMAS",
    "SHOW DATABASES",
    "SHOW SCHEMAS FROM proj",
    "SHOW SCHEMAS IN DATABASE proj",
    "SHOW CATALOGS",
    "SHOW CATALOGS LIKE 'p%'",
    "SHOW COLUMNS FROM sales.orders",
    "SHOW COLUMNS IN orders FROM sales",
    "SHOW COLUMNS FROM proj.sales.orders",
    "SHOW COLUMNS FROM orders",
    "SHOW CREATE TABLE sales.orders",
    "SHOW CREATE VIEW sales.v_orders",
    "SHOW PARTITIONS sales.orders",
    "SHOW TBLPROPERTIES sales.orders",
    "DESCRIBE sales.orders",
    "DESC sales.orders",
    "DESCRIBE TABLE sales.orders",
    "DESCRIBE EXTENDED sales.orders",
    "DESCRIBE FORMATTED sales.orders",
    "DESCRIBE orders",
    "DESCRIBE DATABASE sales",
]

REJECTED = {
    # targets outside the policy
    "SHOW TABLES FROM finance": "Database finance is not allowed",
    "SHOW TABLES FROM other.sales": "Catalog other is not allowed",
    "SHOW SCHEMAS FROM other": "Catalog other is not allowed",
    "SHOW COLUMNS FROM sales.payroll": "Table payroll is not allowed",
    "SHOW COLUMNS FROM otherproj.sales.orders": "Table orders is not allowed: catalog otherproj is not allowed",
    "SHOW CREATE TABLE sales.payroll": "Table payroll is not allowed",
    "SHOW COLUMNS FROM a.b.c.orders": "SHOW COLUMNS is not a supported metadata statement",
    "DESCRIBE sales.payroll": "Table payroll is not allowed",
    "DESCRIBE DATABASE finance": "Database finance is not allowed",
    "SHOW TABLES /* FROM sales */ FROM finance": "Database finance is not allowed",
    # unsupported forms
    "SHOW GRANTS": "SHOW GRANTS is not a supported metadata statement",
    "SHOW VARIABLES": "SHOW VARIABLES is not a supported metadata statement",
    "SHOW PROCESSLIST": "SHOW PROCESSLIST is not a supported metadata statement",
    "SHOW CREATE FUNCTION f": "SHOW CREATE is not a supported metadata statement",
    "SHOW": "SHOW is not a supported metadata statement",
    "SHOW TABLES FROM": "SHOW TABLES is not a supported metadata statement",
    "SHOW TABLES LIKE (SELECT 1)": "SHOW TABLES is not a supported metadata statement",
    "SHOW TABLES FROM 'sales'": "SHOW TABLES is not a supported metadata statement",
    "SHOW TABLES FROM sales x": "SHOW TABLES is not a supported metadata statement",
    "SHOW TABLES FROM sales LIKE 'x' LIKE 'y'": "SHOW TABLES is not a supported metadata statement",
    "SHOW COLUMNS sales.orders": "SHOW COLUMNS is not a supported metadata statement",
    "SHOW COLUMNS FROM sales.orders AS x": "SHOW COLUMNS is not a supported metadata statement",
    "SHOW COLUMNS FROM sales.orders FROM sales": "SHOW COLUMNS is not a supported metadata statement",
    "SHOW CREATE TABLE sales.orders x": "SHOW CREATE is not a supported metadata statement",
    "SHOW TBLPROPERTIES sales.orders ('k')": "SHOW TBLPROPERTIES is not a supported metadata statement",
    # a second statement
    "SHOW TABLES; DROP TABLE x": "SHOW TABLES is not a supported metadata statement",
    "SHOW TABLES FROM sales; SELECT 1": "SHOW TABLES is not a supported metadata statement",
}

DESCRIBE_REJECTED = [
    "DESCRIBE SELECT * FROM payroll",
    "DESCRIBE (SELECT 1)",
    "DESCRIBE FUNCTION upper",
    "DESCRIBE HISTORY sales.orders",
]


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("sql", ALLOWED)
def test_allowed(sql, dialect):
    assert verify_sql(sql, CONFIG, dialect) == {
        "allowed": True,
        "errors": [],
        "fixed": None,
        "risk": 0,
    }


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("sql,error", REJECTED.items())
def test_rejected(sql, error, dialect):
    result = verify_sql(sql, CONFIG, dialect)
    assert not result["allowed"] and result["fixed"] is None
    assert error in result["errors"], result


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("sql", DESCRIBE_REJECTED)
def test_describe_rejected(sql, dialect):
    result = verify_sql(sql, CONFIG, dialect)
    assert not result["allowed"] and result["fixed"] is None, result
    assert not any(e.startswith("Internal error") for e in result["errors"]), result


@pytest.mark.parametrize(
    "dialect,sql,error",
    [
        ("bigquery", "SHOW COLUMNS FROM `sales.payroll`", "Table payroll is not allowed"),
        ("bigquery", "DESCRIBE `sales.payroll`", "Table payroll is not allowed"),
        ("mysql", "SHOW COLUMNS FROM `orders` FROM `finance`", "Database finance is not allowed"),
    ],
)
def test_quoted_names_are_split_as_in_queries(dialect, sql, error):
    result = verify_sql(sql, CONFIG, dialect)
    assert not result["allowed"] and error in result["errors"], result


def test_bigquery_dotted_name_of_a_configured_table():
    assert verify_sql("SHOW COLUMNS FROM `sales.orders`", CONFIG, "bigquery")["allowed"]


@pytest.mark.parametrize(
    "sql,error",
    [
        ("SHOW TABLES", "SHOW TABLES must name its database (strict_database)"),
        ("SHOW VIEWS", "SHOW VIEWS must name its database (strict_database)"),
        ("DESCRIBE orders", "Table orders must be qualified with its database"),
        ("SHOW COLUMNS FROM orders", "Table orders must be qualified with its database"),
    ],
)
def test_strict_database(sql, error):
    assert verify_sql(sql, STRICT, "trino")["errors"] == [error]


@pytest.mark.parametrize(
    "sql", ["SHOW TABLES FROM sales", "DESCRIBE sales.orders", "SHOW SCHEMAS"]
)
def test_strict_database_allows_qualified(sql):
    assert verify_sql(sql, STRICT, "trino")["allowed"]


def test_database_without_databases_list_must_be_a_configured_one():
    config = {k: v for k, v in CONFIG.items() if k != "databases"}
    assert verify_sql("SHOW TABLES FROM sales", config)["allowed"]
    assert verify_sql("SHOW TABLES FROM finance", config)["errors"] == [
        "Database finance is not allowed"
    ]
    # tables configured without any database: the database cannot be checked
    loose = {"tables": [{"table_name": "orders", "columns": ["id"]}], "allow_metadata": True}
    assert verify_sql("SHOW TABLES FROM finance", loose)["allowed"]


@pytest.mark.parametrize(
    "sql,error",
    [
        ("SHOW TABLES", "SHOW statement is not allowed"),
        ("SHOW COLUMNS FROM sales.orders", "SHOW statement is not allowed"),
        ("SHOW CREATE TABLE sales.orders", "SHOW statement is not allowed"),
        ("DESCRIBE sales.orders", "Could not find a query statement"),
    ],
)
def test_option_off_keeps_the_rejection(sql, error):
    assert verify_sql(sql, WITHOUT, "trino")["errors"] == [error]


def test_selects_are_unaffected():
    assert verify_sql("SELECT id FROM sales.orders", CONFIG, "trino")["allowed"]
    assert not verify_sql("SELECT secret FROM sales.payroll", CONFIG, "trino")["allowed"]


@pytest.mark.parametrize("value", ["yes", 1, None])
def test_invalid_option(value):
    config = {**CONFIG, "allow_metadata": value}
    assert validate_config(config) == [
        f"Invalid 'allow_metadata'. Expected true or false. Received: {value!r}"
    ]


def test_suggest_config_does_not_suggest_tables_for_describe():
    # DESCRIBE names no column, and a configured table needs columns
    assert suggest_config("DESCRIBE sales.payroll", CONFIG, "trino") is None


def test_suggest_config_adds_a_database_for_show_tables():
    assert suggest_config("SHOW TABLES FROM finance", CONFIG, "trino") == {
        "tables": [],
        "databases": ["finance"],
    }


def test_config_is_not_modified():
    config = copy.deepcopy(CONFIG)
    verify_sql("SHOW COLUMNS FROM sales.orders", config, "mysql")
    assert config == CONFIG


@pytest.fixture(scope="module")
def duck():
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA sales")
    conn.execute("CREATE TABLE sales.orders (id INT, secret TEXT)")
    yield conn
    conn.close()


@pytest.mark.parametrize("sql", ["DESCRIBE sales.orders", "SHOW TABLES", "SHOW DATABASES"])
def test_allowed_statements_run_on_duckdb(duck, sql):
    config = {k: v for k, v in CONFIG.items() if k != "catalog_name"}
    assert verify_sql(sql, config, "duckdb")["allowed"]
    duck.execute(sql).fetchall()
