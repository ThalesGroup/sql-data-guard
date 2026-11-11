"""Phase 0 probes: each test asserts the *secure* behavior.

A failing test confirms the corresponding finding is real.
"""

import pytest

from sql_data_guard import verify_sql

CONFIG = {
    "tables": [
        {
            "table_name": "orders",
            "columns": ["id", "product_name", "account_id"],
            "restrictions": [{"column": "id", "value": 123}],
        },
        {
            "table_name": "customers",
            "columns": ["id", "name"],
            "restrictions": [{"column": "id", "value": 123}],
        },
    ]
}


def _blocked(sql: str, config: dict = CONFIG, dialect: str = "sqlite") -> bool:
    result = verify_sql(sql, config, dialect)
    # Blocked means rejected outright, or rewritten so the original is not run.
    return (not result["allowed"]) and result["fixed"] is None


def _blocked_or_fixed(sql: str, config: dict = CONFIG, dialect: str = "sqlite"):
    result = verify_sql(sql, config, dialect)
    return result


class TestMultiStatement:
    def test_second_statement_is_rejected(self):
        sql = "SELECT id FROM orders WHERE id = 123; DROP TABLE orders"
        assert _blocked(sql)


class TestColumnClauses:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT id FROM orders WHERE id = 123 AND secret LIKE 'a%'",
            "SELECT id FROM orders WHERE id = 123 ORDER BY secret",
            "SELECT id FROM orders WHERE id = 123 GROUP BY secret",
            "SELECT id FROM orders WHERE id = 123 GROUP BY id HAVING max(secret) > 1",
            "SELECT o.id FROM orders o JOIN customers c ON o.secret = c.id "
            "WHERE o.id = 123 AND c.id = 123",
        ],
    )
    def test_disallowed_column_outside_select(self, sql):
        result = verify_sql(sql, CONFIG, "sqlite")
        assert not result["allowed"]
        # a "fixed" query is only acceptable if it no longer references the column
        assert result["fixed"] is None or "secret" not in result["fixed"]


class TestScoping:
    def test_sub_select_qualifier_does_not_bypass_columns(self):
        sql = "SELECT sub_select.secret FROM orders sub_select WHERE id = 123"
        result = verify_sql(sql, CONFIG, "sqlite")
        assert not result["allowed"]
        assert result["fixed"] is None or "secret" not in result["fixed"]

    def test_alias_shadowing_real_table_name(self):
        # A subquery alias named like a real, but unlisted, table name
        sql = "SELECT x.id FROM (SELECT id FROM orders WHERE id = 123) AS salaries, salaries x"
        assert _blocked(sql)

    def test_cte_name_shadowing_does_not_allow_other_table(self):
        sql = (
            "WITH secrets AS (SELECT id FROM orders WHERE id = 123) "
            "SELECT id FROM secrets, payroll"
        )
        assert _blocked(sql)

    def test_nested_where_is_not_used_for_outer_restriction(self):
        # Outer query has no restriction; only the nested one does.
        sql = "SELECT id FROM orders WHERE product_name IN (SELECT name FROM customers WHERE id = 123)"
        result = verify_sql(sql, CONFIG, "sqlite")
        assert not result["allowed"]
        assert "id = 123" in (result["fixed"] or "")
        # restriction must be on the outer query, not only the inner one
        assert result["fixed"].count("id = 123") >= 2

    def test_nested_from_is_not_used_for_outer_query(self):
        sql = "SELECT id FROM (SELECT id FROM orders WHERE id = 123) AS t"
        result = verify_sql(sql, CONFIG, "sqlite")
        assert result["allowed"]


class TestTableIdentity:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT id FROM other_schema.orders WHERE id = 123",
            "SELECT id FROM ORDERS WHERE id = 123",
            'SELECT id FROM "orders" WHERE id = 123',
        ],
    )
    def test_table_qualifier_and_case_are_handled_consistently(self, sql):
        # Qualified names from a different schema must not silently match;
        # case/quoting variants must not crash. Either result is acceptable
        # as long as it is deterministic and does not raise.
        result = verify_sql(sql, CONFIG, "postgres")
        assert isinstance(result["allowed"], bool)

    def test_foreign_schema_is_rejected_when_config_has_database(self):
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "database_name": "orders_db",
                    "columns": ["id"],
                    "restrictions": [{"column": "id", "value": 123}],
                }
            ]
        }
        assert _blocked("SELECT id FROM evil_db.orders WHERE id = 123", config)


class TestFunctions:
    @pytest.mark.parametrize(
        "sql,dialect",
        [
            ("SELECT pg_read_file('/etc/passwd') FROM orders WHERE id = 123", "postgres"),
            ("SELECT load_extension('x') FROM orders WHERE id = 123", "sqlite"),
            ("SELECT id FROM orders WHERE id = 123 AND pg_sleep(10) IS NOT NULL", "postgres"),
        ],
    )
    def test_dangerous_functions_are_rejected(self, sql, dialect):
        result = verify_sql(sql, CONFIG, dialect)
        assert not result["allowed"]


class TestRestrictionComparison:
    def test_numeric_comparison_not_string_comparison(self):
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": ["id", "qty"],
                    "restrictions": [{"column": "qty", "value": 10, "operation": ">"}],
                }
            ]
        }
        # 9 > 10 is false numerically, but "9" > "10" is true as strings
        result = verify_sql("SELECT id FROM orders WHERE qty > 9", config, "sqlite")
        assert not result["allowed"] or "qty > 10" in (result["fixed"] or "")

    def test_between_with_non_numeric_does_not_raise(self):
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": ["id", "day"],
                    "restrictions": [{"column": "id", "values": [1, 5], "operation": "BETWEEN"}],
                }
            ]
        }
        result = verify_sql(
            "SELECT id FROM orders WHERE id BETWEEN 'a' AND 'b'", config, "sqlite"
        )
        assert not result["allowed"]


class TestConfigValueQuoting:
    def test_string_restriction_value_is_escaped(self):
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": ["id", "region"],
                    "restrictions": [{"column": "region", "value": "o'brien"}],
                }
            ]
        }
        result = verify_sql("SELECT id FROM orders", config, "sqlite")
        assert not result["allowed"]
        # must be parseable and keep the quote inside the literal
        import sqlglot

        sqlglot.parse_one(result["fixed"], dialect="sqlite")
        assert "'o''brien'" in result["fixed"] or "o\\'brien" in result["fixed"]


class TestExceptions:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 'unterminated FROM orders",
            "SELECT id FROM orders WHERE id = 123 /* unterminated",
            "SELECT \"unterminated FROM orders",
        ],
    )
    def test_tokenizer_errors_do_not_raise(self, sql):
        result = verify_sql(sql, CONFIG, "sqlite")
        assert not result["allowed"]

    def test_errors_are_deterministic_list(self):
        sql = "SELECT secret, other, id FROM orders"
        first = verify_sql(sql, CONFIG, "sqlite")["errors"]
        assert isinstance(first, list)
        assert first == sorted(first)


class TestValidator:
    def test_in_with_string_values_is_accepted(self):
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": ["id", "region"],
                    "restrictions": [
                        {"column": "region", "values": ["eu", "us", "il"], "operation": "IN"}
                    ],
                }
            ]
        }
        result = verify_sql("SELECT id FROM orders", config, "sqlite")
        # config must be valid: error list should not be a config-validation error
        assert not any("restriction" in e.lower() and "unsupported" in e.lower()
                       for e in result["errors"])


class TestLogging:
    def test_parse_error_does_not_log_full_sql(self, caplog):
        secret = "SELECT 'super-secret-literal' FROM FROM"
        with caplog.at_level("DEBUG"):
            verify_sql(secret, CONFIG, "sqlite")
        assert "super-secret-literal" not in caplog.text
