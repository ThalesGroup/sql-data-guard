"""Unit tests for the malicious-payload / SQL-injection detection module (F1).

These tests cover the new ``injection_detection`` module and its integration
with ``verify_sql``:

* Stacked queries (always on)
* Dangerous functions (always on)
* System-catalog probing (always on)
* Comment-based evasion (opt-in via ``detect_comments`` / ``detect_injection``)

They also lock in that the feature is *non-breaking*: a benign query with the
feature disabled is still allowed.
"""

import pytest

from sql_data_guard import verify_sql


def _config(detect_comments: bool = False) -> dict:
    config = {
        "tables": [
            {
                "table_name": "orders",
                "database_name": "orders_db",
                "columns": ["id", "product_name", "account_id"],
                "restrictions": [{"column": "account_id", "value": 123}],
            }
        ]
    }
    if detect_comments:
        config["detect_comments"] = True
    return config


def _errors(sql: str, config: dict, dialect: str = "sqlite") -> list:
    return list(verify_sql(sql, config, dialect)["errors"])


class TestStackedQueries:
    def test_stacked_drop_is_blocked_with_intent_revealing_error(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123; DROP TABLE orders",
            _config(),
            "sqlite",
        )
        assert result["allowed"] is False
        assert (
            "Stacked query detected: multiple statements are not allowed"
            in result["errors"]
        )

    def test_stacked_select_select_is_blocked(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123; "
            "SELECT id FROM orders WHERE account_id = 123",
            _config(),
            "sqlite",
        )
        assert result["allowed"] is False
        assert (
            "Stacked query detected: multiple statements are not allowed"
            in result["errors"]
        )

    def test_single_statement_is_not_flagged_as_stacked(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123", _config(), "sqlite"
        )
        assert result["allowed"] is True
        assert result["errors"] == []


class TestDangerousFunctions:
    @pytest.mark.parametrize(
        "sql,dialect,fragment",
        [
            (
                "SELECT id FROM orders WHERE account_id = 123 AND SLEEP(5)",
                "sqlite",
                "SLEEP",
            ),
            (
                "SELECT id FROM orders WHERE account_id = 123 AND BENCHMARK(1000000, 1)",
                "mysql",
                "BENCHMARK",
            ),
            (
                "SELECT id FROM orders WHERE account_id = 123 AND LOAD_FILE('/etc/passwd')",
                "mysql",
                "LOAD_FILE",
            ),
            ("SELECT pg_sleep(5) FROM orders WHERE account_id = 123", "postgres", "pg_sleep"),
        ],
    )
    def test_dangerous_function_is_detected(self, sql, dialect, fragment):
        errors = _errors(sql, _config(), dialect)
        assert any(
            e.startswith(f"Dangerous function detected: {fragment}") for e in errors
        ), errors

    def test_benign_aggregate_function_is_allowed(self):
        result = verify_sql(
            "SELECT COUNT(id) FROM orders WHERE account_id = 123", _config(), "sqlite"
        )
        assert result["allowed"] is True


class TestSystemCatalogProbing:
    def test_information_schema_is_detected(self):
        errors = _errors(
            "SELECT id FROM information_schema.tables", _config(), "sqlite"
        )
        assert any("System catalog probing detected: information_schema" in e for e in errors)

    def test_sqlite_master_is_detected(self):
        errors = _errors(
            "SELECT id FROM orders WHERE account_id = 123 "
            "UNION SELECT name FROM sqlite_master",
            _config(),
            "sqlite",
        )
        assert any("System catalog probing detected: sqlite_master" in e for e in errors)

    def test_common_name_is_not_a_false_positive(self):
        # 'sys' is a plausible real table name and must NOT be flagged.
        errors = _errors("SELECT id FROM sys", _config(), "sqlite")
        assert not any("System catalog probing" in e for e in errors)


class TestCommentEvasion:
    def test_comment_not_flagged_when_disabled(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123 -- trailing",
            _config(detect_comments=False),
            "sqlite",
        )
        # No injection error; comment detection is opt-in.
        assert not any("Comment-based evasion" in e for e in result["errors"])

    def test_line_comment_flagged_when_enabled(self):
        errors = _errors(
            "SELECT id FROM orders WHERE account_id = 123 -- ; DROP TABLE orders",
            _config(detect_comments=True),
        )
        assert any("Comment-based evasion detected: '--'" in e for e in errors)

    def test_block_comment_flagged_when_enabled(self):
        errors = _errors(
            "SELECT id FROM orders WHERE account_id = 123 /* hide */",
            _config(detect_comments=True),
        )
        assert any("Comment-based evasion detected: '/* */'" in e for e in errors)

    def test_detect_injection_nested_flag(self):
        config = _config()
        config["detect_injection"] = {"comments": True}
        errors = _errors(
            "SELECT id FROM orders WHERE account_id = 123 # hash", config
        )
        assert any("Comment-based evasion detected: '#'" in e for e in errors)


class TestRiskScoring:
    def test_injection_increases_risk(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123 AND SLEEP(5)",
            _config(),
            "sqlite",
        )
        assert result["risk"] > 0
