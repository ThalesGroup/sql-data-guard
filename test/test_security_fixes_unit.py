"""
Regression tests for the security / robustness fixes tracked as C1-C4, S1-S7, Q1-Q4.

Each test pins the corrected behavior for a previously validated bug so future
changes cannot silently reintroduce it. See memory-bank/progress.md for the catalogue.
"""

import pytest

from sql_data_guard import verify_sql
from sql_data_guard.restriction_validation import (
    validate_restrictions,
    UnsupportedRestrictionError,
)


def _orders_config() -> dict:
    return {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id", "product_name", "account_id"],
                "restrictions": [{"column": "account_id", "value": 123}],
            }
        ]
    }


# --------------------------------------------------------------------------- #
# S7 - explicit multi-statement (stacked query) guard
# --------------------------------------------------------------------------- #
class TestS7MultiStatement:
    def test_stacked_drop_is_rejected(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123; DROP TABLE orders",
            _orders_config(),
        )
        assert result["allowed"] is False
        assert (
            "Stacked query detected: multiple statements are not allowed"
            in result["errors"]
        )
        assert result["fixed"] is None

    def test_stacked_select_is_rejected(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123; SELECT * FROM secret",
            _orders_config(),
        )
        assert result["allowed"] is False
        assert (
            "Stacked query detected: multiple statements are not allowed"
            in result["errors"]
        )

    def test_single_statement_with_trailing_semicolon_is_allowed(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123;", _orders_config()
        )
        assert result["allowed"] is True
        assert result["errors"] == []

    def test_single_statement_with_comment_tail_is_allowed(self):
        # The "-- ..." tail is an inert comment, not a second statement.
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123 -- ; DROP TABLE orders",
            _orders_config(),
        )
        assert result["allowed"] is True


# --------------------------------------------------------------------------- #
# S1 - generated restriction values must be safely escaped / quoted
# --------------------------------------------------------------------------- #
class TestS1ValueEscaping:
    def test_scalar_string_with_single_quote_is_escaped(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "name"],
                    "restrictions": [{"column": "name", "value": "O'Brien"}],
                }
            ]
        }
        result = verify_sql("SELECT id FROM users", config)
        # No parse error / crash, and the embedded quote is doubled (escaped).
        assert result["fixed"] == "SELECT id FROM users WHERE name = 'O''Brien'"

    def test_in_string_values_are_quoted(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "role"],
                    "restrictions": [
                        {
                            "column": "role",
                            "operation": "IN",
                            "values": ["admin", "user", "guest"],
                        }
                    ],
                }
            ]
        }
        result = verify_sql("SELECT id FROM users", config)
        assert result["fixed"] == (
            "SELECT id FROM users WHERE role IN ('admin', 'user', 'guest')"
        )


# --------------------------------------------------------------------------- #
# S2 - numeric comparison (not string comparison) for < > <= >=
# --------------------------------------------------------------------------- #
class TestS2NumericComparison:
    def _age_config(self) -> dict:
        return {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["age"],
                    "restrictions": [{"column": "age", "operation": "<", "value": 18}],
                }
            ]
        }

    def test_more_restrictive_query_satisfies_restriction(self):
        # age < 9 is within age < 18; "9" < "18" lexicographically was the bug.
        result = verify_sql("SELECT age FROM users WHERE age < 9", self._age_config())
        assert result["allowed"] is True
        assert result["errors"] == []

    def test_less_restrictive_query_injects_correct_operator(self):
        result = verify_sql("SELECT age FROM users WHERE age < 99", self._age_config())
        assert result["allowed"] is False
        # Injected condition uses the restriction's operator (<), not "=".
        assert result["fixed"] == "SELECT age FROM users WHERE (age < 99) AND age < 18"


# --------------------------------------------------------------------------- #
# S3 - dynamic-table column allow-list bypass
# --------------------------------------------------------------------------- #
class TestS3DynamicTableColumns:
    def _users_config(self) -> dict:
        return {"tables": [{"table_name": "users", "columns": ["id", "public_data"]}]}

    def test_unauthorized_column_via_subquery_is_blocked(self):
        result = verify_sql(
            "SELECT secret_col FROM (SELECT * FROM users) AS sub", self._users_config()
        )
        assert result["allowed"] is False
        assert (
            "Column secret_col is not allowed. Column removed from SELECT clause"
            in result["errors"]
        )
        # The disallowed column must NOT survive into a fixed query.
        assert result["fixed"] is None or "secret_col" not in result["fixed"]

    def test_authorized_column_via_subquery_is_allowed(self):
        result = verify_sql(
            "SELECT id FROM (SELECT * FROM users) AS sub", self._users_config()
        )
        # Inner SELECT * is flagged, but the legitimate outer column survives.
        assert result["fixed"] == "SELECT id FROM (SELECT id, public_data FROM users) AS sub"

    def test_unaliased_subquery_column_still_allowed(self):
        config = _orders_config()
        result = verify_sql("SELECT id FROM (SELECT id FROM orders)", config)
        assert "Column id is not allowed. Column removed from SELECT clause" not in (
            result["errors"]
        )


# --------------------------------------------------------------------------- #
# S4 - EXCEPT / INTERSECT set operations are verified like UNION
# --------------------------------------------------------------------------- #
class TestS4SetOperations:
    def test_valid_except_is_allowed(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123 "
            "EXCEPT SELECT id FROM orders WHERE account_id = 123",
            _orders_config(),
        )
        assert result["allowed"] is True, result

    def test_valid_intersect_is_allowed(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123 "
            "INTERSECT SELECT id FROM orders WHERE account_id = 123",
            _orders_config(),
        )
        assert result["allowed"] is True, result

    def test_except_with_disallowed_column_is_blocked(self):
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123 "
            "EXCEPT SELECT secret FROM orders WHERE account_id = 123",
            _orders_config(),
        )
        assert result["allowed"] is False
        assert any("secret" in e for e in result["errors"])


# --------------------------------------------------------------------------- #
# S5 - opt-in max_risk hard-block
# --------------------------------------------------------------------------- #
class TestS5MaxRisk:
    def test_query_above_threshold_is_hard_blocked(self):
        config = _orders_config()
        config["max_risk"] = 0.2
        result = verify_sql("SELECT id FROM orders WHERE 1 = 1", config)
        assert result["allowed"] is False
        assert result["fixed"] is None
        assert result["risk"] > 0.2

    def test_query_below_threshold_is_still_fixed(self):
        config = _orders_config()
        config["max_risk"] = 0.9
        result = verify_sql("SELECT id FROM orders WHERE 1 = 1", config)
        assert result["fixed"] is not None

    def test_no_threshold_preserves_default_behavior(self):
        result = verify_sql("SELECT id FROM orders WHERE 1 = 1", _orders_config())
        assert result["fixed"] is not None


# --------------------------------------------------------------------------- #
# C1-C4 - config validation robustness (no caller-facing crashes)
# --------------------------------------------------------------------------- #
class TestConfigValidation:
    def test_in_accepts_many_values(self):
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": ["id", "account_id"],
                    "restrictions": [
                        {"column": "account_id", "operation": "IN", "values": [1, 2, 3]}
                    ],
                }
            ]
        }
        # Must not raise; previously crashed with len(values) == 2 requirement.
        validate_restrictions(config)

    def test_in_accepts_string_values(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "role"],
                    "restrictions": [
                        {
                            "column": "role",
                            "operation": "IN",
                            "values": ["admin", "user", "guest"],
                        }
                    ],
                }
            ]
        }
        validate_restrictions(config)

    def test_in_rejects_mixed_types_without_crashing_verify_sql(self):
        config = {
            "tables": [
                {
                    "table_name": "users",
                    "columns": ["id", "role"],
                    "restrictions": [
                        {"column": "role", "operation": "IN", "values": [1, "a"]}
                    ],
                }
            ]
        }
        # C4: verify_sql converts the ValueError into a result dict, not a crash.
        result = verify_sql("SELECT id FROM users", config)
        assert result["allowed"] is False
        assert any("IN" in e for e in result["errors"])

    def test_missing_value_for_scalar_op_is_clean_error(self):
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": ["id", "status"],
                    "restrictions": [{"column": "status", "operation": "="}],
                }
            ]
        }
        result = verify_sql("SELECT id FROM orders", config)
        assert result["allowed"] is False
        assert result["risk"] == 1.0

    def test_lowercase_operations_are_accepted(self):
        config = {
            "tables": [
                {
                    "table_name": "orders",
                    "columns": ["id", "account_id"],
                    "restrictions": [
                        {"column": "account_id", "operation": "between", "values": [1, 5]}
                    ],
                }
            ]
        }
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id BETWEEN 1 AND 5", config
        )
        assert result["allowed"] is True, result

    def test_unsupported_operation_still_rejected(self):
        config = {
            "tables": [
                {
                    "table_name": "products",
                    "columns": ["price"],
                    "restrictions": [
                        {"column": "price", "value": 100, "operation": "NotSupported"}
                    ],
                }
            ]
        }
        with pytest.raises(UnsupportedRestrictionError):
            validate_restrictions(config)


# --------------------------------------------------------------------------- #
# Q1 - errors are an ordered, de-duplicated list
# --------------------------------------------------------------------------- #
class TestQ1ErrorsList:
    def test_errors_is_a_list(self):
        result = verify_sql("SELECT id FROM orders WHERE account_id = 123", _orders_config())
        assert isinstance(result["errors"], list)

    def test_errors_are_deduplicated(self):
        result = verify_sql("SELECT secret FROM (SELECT * FROM users) AS s", {
            "tables": [{"table_name": "users", "columns": ["id"]}]
        })
        assert len(result["errors"]) == len(set(result["errors"]))


# --------------------------------------------------------------------------- #
# Q3 - the MCP wrapper module is import-safe
# --------------------------------------------------------------------------- #
class TestQ3McpImport:
    def test_mcp_wrapper_is_importable(self):
        # Previously raised NameError because module-level globals were only
        # defined inside the __main__ block. (Requires the optional docker dep.)
        pytest.importorskip("docker")
        from sql_data_guard.mcpwrapper import mcp_wrapper

        assert isinstance(mcp_wrapper.errors, dict)
        assert isinstance(mcp_wrapper.config, dict)


# --------------------------------------------------------------------------- #
# S6 - opt-in REST API key authentication
# --------------------------------------------------------------------------- #
class TestS6ApiKey:
    def _request(self):
        from sql_data_guard.rest import sql_data_guard_rest as rest_mod

        return rest_mod

    def test_no_key_configured_allows_request(self, monkeypatch):
        rest_mod = self._request()
        monkeypatch.setattr(rest_mod, "_API_KEY", None)
        result = rest_mod.app.test_client().post(
            "/verify-sql",
            json={"sql": "SELECT id FROM orders WHERE id = 123", "config": {
                "tables": [{"table_name": "orders", "columns": ["id"],
                            "restrictions": [{"column": "id", "value": 123}]}]
            }},
        )
        assert result.status_code == 200

    def test_missing_key_is_unauthorized(self, monkeypatch):
        rest_mod = self._request()
        monkeypatch.setattr(rest_mod, "_API_KEY", "secret")
        result = rest_mod.app.test_client().post("/verify-sql", json={})
        assert result.status_code == 401

    def test_correct_key_is_authorized(self, monkeypatch):
        rest_mod = self._request()
        monkeypatch.setattr(rest_mod, "_API_KEY", "secret")
        result = rest_mod.app.test_client().post(
            "/verify-sql",
            json={"sql": "SELECT id FROM orders WHERE id = 123", "config": {
                "tables": [{"table_name": "orders", "columns": ["id"],
                            "restrictions": [{"column": "id", "value": 123}]}]
            }},
            headers={"X-API-Key": "secret"},
        )
        assert result.status_code == 200
