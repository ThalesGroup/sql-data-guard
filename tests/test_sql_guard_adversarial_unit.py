import sqlite3
import pytest
from conftest import verify_sql_test
from sql_data_guard import verify_sql


@pytest.fixture(scope="class")
def config() -> dict:
    return {
        "tables": [
            {
                "table_name": "orders",
                "database_name": "orders_db",
                "columns": ["id", "product_name", "account_id", "day"],
                "restrictions": [{"column": "id", "value": 123}],
            }
        ]
    }


@pytest.fixture(scope="class")
def cnn():
    with sqlite3.connect(":memory:") as conn:
        conn.execute("ATTACH DATABASE ':memory:' AS orders_db")
        conn.execute(
            "CREATE TABLE orders_db.orders (id INT, product_name TEXT, account_id INT, day TEXT)"
        )
        conn.execute(
            "INSERT INTO orders VALUES (123, 'product1', 123, '2025-01-01')"
        )
        conn.execute(
            "INSERT INTO orders VALUES (124, 'product2', 124, '2025-01-02')"
        )
        yield conn


class TestSQLAdversarial:
    def test_setup_works(self, config, cnn):
        result = verify_sql("SELECT id FROM orders WHERE id = 123", config)
        assert result["allowed"]

    def test_stacked_query_rejection(self, config):
        # These should be REJECTED as stacked queries
        queries_rejected = [
            "SELECT id FROM orders WHERE id = 123; DROP TABLE orders;",
            "SELECT id FROM orders WHERE id = 123; SELECT product_name FROM orders;",
        ]
        for q in queries_rejected:
            result = verify_sql(q, config)
            assert not result["allowed"], f"Should have rejected stacked query: {q}"
            assert any("Stacked" in err for err in result["errors"]), f"Unexpected error for {q}: {result['errors']}"

        # These should be ALLOWED because they are single queries with optional comments or trailing semicolons
        queries_allowed = [
            "SELECT id FROM orders WHERE id = 123;",
            "SELECT id FROM orders WHERE id = 123; -- some comment",
        ]
        for q in queries_allowed:
            result = verify_sql(q, config)
            assert result["allowed"], f"Should have allowed query: {q}. Errors: {result['errors']}"

    def test_comment_and_whitespace_obfuscation(self, config):
        # Comments used as whitespace to confuse simplistic parsers
        valid_obfuscated_queries = [
            "SELECT/**/id/**/FROM/**/orders/**/WHERE/**/id=123",
            "SELECT id FROM orders WHERE -- comment\nid = 123",
            "SELECT id FROM orders WHERE /* block comment */ id = 123",
            "SELECT id FROM orders WHERE id = 123 /* end comment */",
        ]
        for q in valid_obfuscated_queries:
            result = verify_sql(q, config)
            assert result["allowed"], f"Should have parsed and allowed: {q}. Errors: {result['errors']}"

        # Ensure that restrictions are still enforced even if obfuscated
        invalid_obfuscated_queries = [
            "SELECT/**/not_allowed/**/FROM/**/orders/**/WHERE/**/id=123",
            "SELECT id FROM orders WHERE -- comment\nid = 234",
            "SELECT id FROM orders WHERE /* block comment */ id = 234",
            "SELECT id FROM orders WHERE id = 234 /* end comment */",
        ]
        for q in invalid_obfuscated_queries:
            result = verify_sql(q, config)
            assert not result["allowed"], f"Should have rejected/blocked query: {q}"

    def test_cte_and_subquery_restrictions(self, config):
        # 1. Valid CTE that includes correct restriction
        q_valid_cte = "WITH cte AS (SELECT id, account_id FROM orders WHERE id = 123) SELECT id FROM cte"
        result = verify_sql(q_valid_cte, config)
        assert result["allowed"]

        # 2. CTE attempting to bypass column validation with unauthorized column
        q_bypass_col_cte = "WITH cte AS (SELECT not_allowed FROM orders WHERE id = 123) SELECT not_allowed FROM cte"
        result = verify_sql(q_bypass_col_cte, config)
        assert not result["allowed"]
        assert any("not_allowed" in err for err in result["errors"])

        # 3. CTE attempting to bypass row restriction
        q_missing_restriction_cte = "WITH cte AS (SELECT id, account_id FROM orders) SELECT id FROM cte"
        result = verify_sql(q_missing_restriction_cte, config)
        assert not result["allowed"]
        assert "Missing restriction for table: orders column: id value: 123" in result["errors"]
        assert result["fixed"] == "WITH cte AS (SELECT id, account_id FROM orders WHERE id = 123) SELECT id FROM cte"

        # 4. Nested subquery with unauthorized column
        q_unauthorized_col_subquery = "SELECT id FROM (SELECT id, not_allowed FROM orders) WHERE id = 123"
        result = verify_sql(q_unauthorized_col_subquery, config)
        assert not result["allowed"]
        assert any("not_allowed" in err for err in result["errors"])
        assert result["fixed"] == "SELECT id FROM (SELECT id FROM orders WHERE id = 123) WHERE id = 123"

        # 5. Nested subquery with missing row restriction
        q_missing_restriction_subquery = "SELECT id FROM (SELECT id FROM orders) WHERE id = 123"
        result = verify_sql(q_missing_restriction_subquery, config)
        assert not result["allowed"]
        assert "Missing restriction for table: orders column: id value: 123" in result["errors"]
        assert result["fixed"] == "SELECT id FROM (SELECT id FROM orders WHERE id = 123) WHERE id = 123"

    def test_query_limits_and_extremes(self, config):
        # 1. Length limits (max_length)
        length_limited_config = {
            "max_length": 50,
            "tables": [
                {
                    "table_name": "orders",
                    "database_name": "orders_db",
                    "columns": ["id"],
                    "restrictions": [{"column": "id", "value": 123}],
                }
            ]
        }
        long_query = "SELECT id FROM orders WHERE id = 123 AND product_name = 'some_very_very_long_string_literal'"
        result = verify_sql(long_query, length_limited_config)
        assert not result["allowed"]
        assert "SQL exceeds maximum length of 50 characters." in result["errors"]

        # 2. Deeply nested queries (recursion safety)
        depth = 150
        nested_query = (
            "SELECT id FROM (" * depth
            + "SELECT id FROM orders WHERE id = 123"
            + ") AS sub"
            + ") AS sub" * (depth - 1)
        )
        result = verify_sql(nested_query, config)
        assert not result["allowed"]
        assert any("recursion" in err or "depth" in err or "parsing" in err for err in result["errors"])
