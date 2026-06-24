"""Tests for column-level masking / redaction."""

import sqlite3

import pytest

from conftest import verify_sql_test
from sql_data_guard import verify_sql
from sql_data_guard.column_masking import validate_column_masks


class TestColumnMaskValidation:
    """Validation of the column_masks section of the config."""

    def test_valid_masks(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "email", "credit_card"],
                    "column_masks": [
                        {"column": "credit_card", "policy": "partial", "show_last": 4},
                        {"column": "email", "policy": "hash"},
                        {"column": "id", "policy": "redact"},
                    ],
                }
            ]
        }
        validate_column_masks(config)  # should not raise

    def test_unsupported_policy(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id"],
                    "column_masks": [{"column": "id", "policy": "encrypt"}],
                }
            ]
        }
        with pytest.raises(ValueError, match="Unsupported mask policy"):
            validate_column_masks(config)

    def test_mask_column_not_in_allow_list(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id"],
                    "column_masks": [{"column": "ssn", "policy": "redact"}],
                }
            ]
        }
        with pytest.raises(ValueError, match="must also appear in the 'columns'"):
            validate_column_masks(config)

    def test_partial_invalid_show_last(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["card"],
                    "column_masks": [
                        {"column": "card", "policy": "partial", "show_last": -1}
                    ],
                }
            ]
        }
        with pytest.raises(ValueError, match="show_last"):
            validate_column_masks(config)

    def test_masks_must_be_list(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id"],
                    "column_masks": {"column": "id", "policy": "redact"},
                }
            ]
        }
        with pytest.raises(ValueError, match="must be a list"):
            validate_column_masks(config)

    def test_invalid_config_surfaces_through_verify_sql(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id"],
                    "column_masks": [{"column": "id", "policy": "encrypt"}],
                }
            ]
        }
        result = verify_sql("SELECT id FROM users", config, "sqlite")
        assert result["allowed"] is False
        assert result["risk"] == 1.0
        assert any("Unsupported mask policy" in e for e in result["errors"])


class TestColumnMaskRewrite:
    """Rewriting of SELECT columns into masking expressions."""

    def test_redact_mask(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "ssn"],
                    "column_masks": [{"column": "ssn", "policy": "redact"}],
                }
            ]
        }
        verify_sql_test(
            "SELECT id, ssn FROM users",
            config,
            errors={"Column ssn is masked (redact)"},
            fix="SELECT id, '****' AS ssn FROM users",
        )

    def test_redact_custom_replacement(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "ssn"],
                    "column_masks": [
                        {"column": "ssn", "policy": "redact", "replacement": "REDACTED"}
                    ],
                }
            ]
        }
        verify_sql_test(
            "SELECT ssn FROM users",
            config,
            errors={"Column ssn is masked (redact)"},
            fix="SELECT 'REDACTED' AS ssn FROM users",
        )

    def test_hash_mask(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "email"],
                    "column_masks": [{"column": "email", "policy": "hash"}],
                }
            ]
        }
        verify_sql_test(
            "SELECT id, email FROM users",
            config,
            errors={"Column email is masked (hash)"},
            fix="SELECT id, MD5(email) AS email FROM users",
        )

    def test_partial_mask(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "credit_card"],
                    "column_masks": [
                        {"column": "credit_card", "policy": "partial", "show_last": 4}
                    ],
                }
            ]
        }
        verify_sql_test(
            "SELECT credit_card FROM users",
            config,
            errors={"Column credit_card is masked (partial)"},
            fix="SELECT '****' || SUBSTRING(CAST(credit_card AS TEXT), -4) AS credit_card FROM users",
        )

    def test_mask_with_restriction(self):
        """Masking and row restrictions compose cleanly."""
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": ["id", "account_id", "card"],
                    "restrictions": [{"column": "account_id", "value": 123}],
                    "column_masks": [{"column": "card", "policy": "redact"}],
                }
            ]
        }
        verify_sql_test(
            "SELECT id, card FROM orders WHERE account_id = 123",
            config,
            errors={"Column card is masked (redact)"},
            fix="SELECT id, '****' AS card FROM orders WHERE account_id = 123",
        )

    def test_unmasked_column_untouched(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "email"],
                    "column_masks": [{"column": "email", "policy": "redact"}],
                }
            ]
        }
        # Selecting only the unmasked column: nothing to fix.
        verify_sql_test("SELECT id FROM users", config)

    def test_no_masks_configured(self):
        config = {
            "tables": [{"table_name": "users", "columns": ["id", "email"]}]
        }
        verify_sql_test("SELECT id, email FROM users", config)

    def test_table_qualified_column_is_masked(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "ssn"],
                    "column_masks": [{"column": "ssn", "policy": "redact"}],
                }
            ]
        }
        verify_sql_test(
            "SELECT u.ssn FROM users AS u",
            config,
            errors={"Column ssn is masked (redact)"},
            fix="SELECT '****' AS ssn FROM users AS u",
        )


class TestColumnMaskExecution:
    """The rewritten (fixed) query must actually run and hide the data."""

    @pytest.fixture(scope="class")
    def cnn(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE users (id INT, email TEXT, credit_card TEXT)"
        )
        conn.execute(
            "INSERT INTO users VALUES (1, 'alice@example.com', '4111111111111234')"
        )
        conn.commit()
        yield conn
        conn.close()

    def test_redact_executes_and_hides(self, cnn):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "credit_card"],
                    "column_masks": [{"column": "credit_card", "policy": "redact"}],
                }
            ]
        }
        verify_sql_test(
            "SELECT id, credit_card FROM users",
            config,
            errors={"Column credit_card is masked (redact)"},
            fix="SELECT id, '****' AS credit_card FROM users",
            cnn=cnn,
            data=[[1, "****"]],
        )

    def test_partial_executes_and_keeps_last_four(self, cnn):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "credit_card"],
                    "column_masks": [
                        {"column": "credit_card", "policy": "partial", "show_last": 4}
                    ],
                }
            ]
        }
        # sqlglot renders SUBSTRING(...) as sqlite-native SUBSTRING/|| so the masked
        # query runs without any custom UDFs.
        sql_to_use = verify_sql_test(
            "SELECT id, credit_card FROM users",
            config,
            errors={"Column credit_card is masked (partial)"},
            fix="SELECT id, '****' || SUBSTRING(CAST(credit_card AS TEXT), -4) AS credit_card FROM users",
        )
        row = cnn.execute(sql_to_use).fetchone()
        assert row == (1, "****1234")
