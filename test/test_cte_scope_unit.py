"""CTE scoping: a WITH name is visible only to the query it belongs to (and its children)."""

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


def _verify(sql, dialect="sqlite"):
    return verify_sql(sql, CONFIG, dialect)


@pytest.mark.parametrize(
    "sql",
    [
        # a CTE defined in a sibling subquery must not make the real table usable elsewhere
        "SELECT p.salary FROM (WITH payroll AS (SELECT 1 AS id) SELECT id FROM payroll) a, payroll p",
        "SELECT a.id FROM (WITH payroll AS (SELECT 1 AS id) SELECT id FROM payroll) a, payroll p, orders o WHERE o.account_id = 123",
        # a non-recursive CTE cannot read the real table of the same name
        "WITH payroll AS (SELECT product_name FROM payroll) SELECT product_name FROM payroll",
        # CTE body on a set operation is verified
        "WITH x AS (SELECT salary FROM payroll) SELECT id FROM orders WHERE account_id = 123 UNION SELECT id FROM orders WHERE account_id = 123",
        # data-modifying CTEs
        "WITH d AS (DELETE FROM orders RETURNING id) SELECT id FROM d",
        "WITH d AS (INSERT INTO orders VALUES (1, 'a', 123) RETURNING id) SELECT id FROM d",
        "WITH d AS (UPDATE orders SET id = 1 RETURNING id) SELECT id FROM d",
    ],
)
def test_cte_scope_bypass_is_rejected(sql):
    result = _verify(sql, "postgres")
    assert not result["allowed"], result
    assert result["fixed"] is None or "payroll" not in result["fixed"]


@pytest.mark.parametrize(
    "sql",
    [
        "WITH a AS (SELECT id FROM orders WHERE account_id = 123), b AS (SELECT id FROM a) SELECT id FROM b",
        "WITH x AS (SELECT id FROM orders WHERE account_id = 123) SELECT id FROM x UNION SELECT id FROM x",
        "WITH x AS (SELECT id FROM orders WHERE account_id = 123) SELECT y.id FROM x y",
        "SELECT id FROM orders WHERE account_id = 123 AND id IN (WITH c AS (SELECT id FROM orders WHERE account_id = 123) SELECT id FROM c)",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
    ],
)
def test_legitimate_ctes_still_work(sql):
    result = _verify(sql)
    assert result["allowed"], result


def test_recursive_cte_cannot_authorize_forbidden_column():
    sql = (
        "WITH RECURSIVE r(secret) AS (SELECT secret FROM orders WHERE account_id = 123 "
        "UNION ALL SELECT secret FROM r) SELECT secret FROM r"
    )
    result = _verify(sql)
    assert not result["allowed"], result
