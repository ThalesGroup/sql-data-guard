"""Unit tests for the function allow-list / deny-list (feature F2).

Two optional, top-level config keys (matched case-insensitively):

* ``blocked_functions`` -- listed functions are blocked.
* ``allowed_functions`` -- only listed functions may be called.

Violations are hard blocks (no auto-fix). The feature is a no-op when neither
key is present, and composes with the always-on dangerous-function deny-list
shipped by F1.
"""

import pytest

from sql_data_guard import verify_sql


def _config(allowed=None, blocked=None) -> dict:
    config = {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id", "product_name", "account_id"],
            }
        ]
    }
    if allowed is not None:
        config["allowed_functions"] = allowed
    if blocked is not None:
        config["blocked_functions"] = blocked
    return config


class TestBlockedFunctions:
    @pytest.mark.parametrize(
        "sql,dialect,fn",
        [
            ("SELECT UPPER(product_name) FROM orders", "sqlite", "UPPER"),
            ("SELECT id FROM orders WHERE LENGTH(product_name) > 3", "sqlite", "LENGTH"),
        ],
    )
    def test_blocked_function_is_rejected(self, sql, dialect, fn):
        result = verify_sql(sql, _config(blocked=[fn]), dialect)
        assert result["allowed"] is False
        assert result["fixed"] is None  # hard block, not auto-fixed
        assert any(f"Function {fn} is not allowed" in e for e in result["errors"])

    def test_block_is_case_insensitive(self):
        # Lower-case call, upper-case deny-list entry: matching is
        # case-insensitive even though sqlglot canonicalises the displayed name.
        result = verify_sql(
            "SELECT upper(product_name) FROM orders", _config(blocked=["UPPER"]), "sqlite"
        )
        assert result["allowed"] is False
        assert any("is not allowed" in e for e in result["errors"])

    def test_unlisted_function_passes_with_deny_list(self):
        result = verify_sql(
            "SELECT COUNT(id) FROM orders", _config(blocked=["UPPER"]), "sqlite"
        )
        assert result["allowed"] is True
        assert result["errors"] == []


class TestAllowedFunctions:
    def test_allowed_function_passes(self):
        result = verify_sql(
            "SELECT COUNT(id) FROM orders", _config(allowed=["COUNT"]), "sqlite"
        )
        assert result["allowed"] is True

    def test_non_allowed_function_is_blocked(self):
        result = verify_sql(
            "SELECT UPPER(product_name) FROM orders",
            _config(allowed=["COUNT"]),
            "sqlite",
        )
        assert result["allowed"] is False
        assert any(
            "Function UPPER is not in the allowed functions list" in e
            for e in result["errors"]
        )


class TestComposesWithF1:
    def test_f1_dangerous_function_still_blocked_without_config(self):
        # LOAD_FILE is always blocked by F1 even with no F2 policy.
        result = verify_sql(
            "SELECT id FROM orders WHERE account_id = 123 AND LOAD_FILE('/etc/passwd')",
            _config(),
            "mysql",
        )
        assert result["allowed"] is False
        assert any("Dangerous function detected: LOAD_FILE" in e for e in result["errors"])


class TestNonBreaking:
    def test_no_policy_is_noop(self):
        result = verify_sql(
            "SELECT COUNT(id) FROM orders", _config(), "sqlite"
        )
        assert result["allowed"] is True
        assert result["fixed"] is None


class TestValidation:
    @pytest.mark.parametrize("key", ["allowed_functions", "blocked_functions"])
    @pytest.mark.parametrize("bad", ["UPPER", [1, 2], {"a": 1}])
    def test_invalid_function_list_returns_graceful_error(self, key, bad):
        config = _config()
        config[key] = bad
        result = verify_sql("SELECT id FROM orders", config, "sqlite")
        assert result["allowed"] is False
        assert any(f"Invalid '{key}'" in e for e in result["errors"])
