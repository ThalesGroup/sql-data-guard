"""Unit tests for mandatory LIMIT / max-rows enforcement (feature F3).

``force_limit`` caps the number of rows the outermost query may return:

* injected when no ``LIMIT`` is present,
* clamped when an existing ``LIMIT`` exceeds the cap,
* left untouched when the existing ``LIMIT`` is within the cap,
* a no-op when the option is absent (non-breaking).
"""

import pytest

from sql_data_guard import verify_sql


def _config(force_limit=None) -> dict:
    config = {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id", "product_name", "account_id"],
            }
        ]
    }
    if force_limit is not None:
        config["force_limit"] = force_limit
    return config


class TestForceLimitInjection:
    def test_limit_injected_when_absent(self):
        result = verify_sql("SELECT id FROM orders", _config(1000), "sqlite")
        assert result["allowed"] is False
        assert result["fixed"] == "SELECT id FROM orders LIMIT 1000"
        assert any("Row limit enforced" in e for e in result["errors"])

    def test_limit_clamped_when_too_large(self):
        result = verify_sql(
            "SELECT id FROM orders LIMIT 5000", _config(1000), "sqlite"
        )
        assert result["allowed"] is False
        assert result["fixed"] == "SELECT id FROM orders LIMIT 1000"
        assert any("clamped from 5000 to 1000" in e for e in result["errors"])

    def test_limit_left_when_within_cap(self):
        result = verify_sql(
            "SELECT id FROM orders LIMIT 10", _config(1000), "sqlite"
        )
        assert result["allowed"] is True
        assert result["fixed"] is None
        assert result["errors"] == []

    def test_limit_equal_to_cap_is_allowed(self):
        result = verify_sql(
            "SELECT id FROM orders LIMIT 1000", _config(1000), "sqlite"
        )
        assert result["allowed"] is True
        assert result["fixed"] is None


class TestForceLimitUnion:
    def test_union_limit_injected_on_outer_query(self):
        result = verify_sql(
            "SELECT id FROM orders UNION SELECT account_id FROM orders",
            _config(1000),
            "sqlite",
        )
        assert result["allowed"] is False
        assert result["fixed"].endswith("LIMIT 1000")


class TestForceLimitCombinedWithFix:
    def test_limit_added_alongside_column_strip(self):
        # 'secret' is not allowed -> stripped; force_limit also applied.
        result = verify_sql(
            "SELECT id, secret FROM orders", _config(1000), "sqlite"
        )
        assert result["allowed"] is False
        assert result["fixed"] == "SELECT id FROM orders LIMIT 1000"
        assert any("Row limit enforced" in e for e in result["errors"])
        assert any("Column secret is not allowed" in e for e in result["errors"])


class TestForceLimitNonBreaking:
    def test_no_force_limit_is_noop(self):
        result = verify_sql("SELECT id FROM orders", _config(), "sqlite")
        assert result["allowed"] is True
        assert result["fixed"] is None


class TestForceLimitValidation:
    @pytest.mark.parametrize("bad", [0, -5, "1000", 10.5, True])
    def test_invalid_force_limit_returns_graceful_error(self, bad):
        result = verify_sql("SELECT id FROM orders", _config(bad), "sqlite")
        assert result["allowed"] is False
        assert any("Invalid 'force_limit'" in e for e in result["errors"])
