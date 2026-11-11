"""
Correlated sub-queries: a column qualified with the alias of an enclosing query's table must be
allowed on *that* table, not on whichever table the sub-query selects from.
"""

import sqlite3

import pytest

from sql_data_guard import verify_sql

CONFIG = {
    "tables": [
        {
            "table_name": "orders",
            "columns": ["id", "account_id"],
            "restrictions": [{"column": "account_id", "value": 123}],
        },
        {
            # `notes` is allowed here but NOT on orders, although both tables have the column
            "table_name": "customers",
            "columns": ["id", "notes"],
            "restrictions": [{"column": "id", "value": 123}],
        },
    ]
}


@pytest.fixture(scope="module")
def cnn():
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE orders (id INT, account_id INT, notes TEXT)")
        conn.execute("INSERT INTO orders VALUES (1, 123, 'SECRET-NOTE')")
        conn.execute("CREATE TABLE customers (id INT, notes TEXT)")
        conn.execute("INSERT INTO customers VALUES (123, 'public')")
        yield conn


FORBIDDEN_CORRELATED = [
    # oracle on the outer table's forbidden column
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND EXISTS (SELECT 1 FROM customers c WHERE c.id = 123 AND o.notes LIKE 'SECRET%')",
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND (SELECT count(*) FROM customers c WHERE c.id = 123 AND o.notes = 'SECRET-NOTE') = 1",
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND o.id IN (SELECT c.id FROM customers c WHERE c.id = 123 AND o.notes IS NOT NULL)",
    # two levels deep, referring to the outermost query
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND EXISTS (SELECT 1 FROM customers c WHERE c.id = 123 AND EXISTS (SELECT 1 FROM customers c2 WHERE c2.id = 123 AND o.notes LIKE 'S%'))",
    # outer table referenced by its name (no alias)
    "SELECT orders.id FROM orders WHERE orders.account_id = 123 AND EXISTS (SELECT 1 FROM customers c WHERE c.id = 123 AND orders.notes LIKE 'S%')",
    # in the select list of the sub-query
    "SELECT o.id, (SELECT o.notes FROM customers c WHERE c.id = 123) AS n FROM orders o WHERE o.account_id = 123",
    # in other clauses of the sub-query
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND EXISTS (SELECT 1 FROM customers c WHERE c.id = 123 ORDER BY o.notes)",
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND EXISTS (SELECT 1 FROM customers c JOIN customers d ON d.notes = o.notes WHERE c.id = 123)",
    # CTE / derived table body referring to an outer alias
    "SELECT o.id FROM orders o, (SELECT c.id FROM customers c WHERE c.id = 123 AND o.notes LIKE 'S%') d WHERE o.account_id = 123",
]


@pytest.mark.parametrize("sql", FORBIDDEN_CORRELATED)
def test_outer_forbidden_column_through_a_subquery_is_rejected(cnn, sql):
    result = verify_sql(sql, CONFIG, "sqlite")
    assert not result["allowed"], result
    assert result["fixed"] is None or "notes" not in result["fixed"], result


ALLOWED_CORRELATED = [
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND EXISTS (SELECT 1 FROM customers c WHERE c.id = 123 AND o.id = c.id)",
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND EXISTS (SELECT 1 FROM customers c WHERE c.id = 123 AND c.notes = 'x')",
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND o.id IN (SELECT c.id FROM customers c WHERE c.id = 123)",
    "SELECT orders.id FROM orders WHERE orders.account_id = 123 AND EXISTS (SELECT 1 FROM customers c WHERE c.id = 123 AND orders.id = c.id)",
    # an inner alias shadows an outer one: `o` is the customers table here
    "SELECT o.id FROM orders o WHERE o.account_id = 123 AND EXISTS (SELECT 1 FROM customers o WHERE o.id = 123 AND o.notes = 'x')",
]


@pytest.mark.parametrize("sql", ALLOWED_CORRELATED)
def test_legitimate_correlated_subqueries_still_work(cnn, sql):
    result = verify_sql(sql, CONFIG, "sqlite")
    assert result["allowed"], result
    cnn.execute(sql).fetchall()  # and they run


@pytest.mark.parametrize(
    "sql,dialect",
    [
        # LATERAL derived tables see the other tables of the FROM clause
        ("SELECT o.id FROM orders o CROSS JOIN LATERAL (SELECT 1 AS x FROM customers c WHERE c.id = 123 AND o.notes LIKE 'S%') d WHERE o.account_id = 123", "postgres"),
        ("SELECT o.id FROM orders o, LATERAL (SELECT 1 AS x FROM customers c WHERE c.id = 123 AND o.notes LIKE 'S%') d WHERE o.account_id = 123", "duckdb"),
        ("SELECT o.id FROM orders o LEFT JOIN LATERAL (SELECT c.id FROM customers c WHERE c.id = 123 AND o.notes = 'x') d ON TRUE WHERE o.account_id = 123", "mysql"),
        # same shape without the LATERAL keyword (databases that allow it, and the guard must not rely on that)
        ("SELECT o.id FROM orders o, (SELECT c.id FROM customers c WHERE c.id = 123 AND o.notes LIKE 'S%') d WHERE o.account_id = 123", "sqlite"),
    ],
)
def test_lateral_subquery_cannot_read_a_forbidden_column_of_a_sibling_table(sql, dialect):
    result = verify_sql(sql, CONFIG, dialect)
    assert not result["allowed"], result
    assert result["fixed"] is None or "notes" not in result["fixed"], result
