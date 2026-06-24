"""Unit tests for negative (deny-list) column rules & wildcard support (F10).

Per-table ``denied_columns``:

* an explicitly-referenced denied column is stripped from the SELECT,
* ``SELECT *`` expands to the allowed columns *minus* the denied ones,
* ``table.*`` expands the same way, scoped to that table,
* deny wins even when the column is otherwise allow-listed,
* the feature is a no-op when ``denied_columns`` is absent.
"""

import pytest

from sql_data_guard import verify_sql


def _config(denied=None) -> dict:
    table = {
        "table_name": "orders",
        "columns": ["id", "product_name", "ssn", "account_id"],
    }
    if denied is not None:
        table["denied_columns"] = denied
    return {"tables": [table]}


class TestExplicitDeniedColumn:
    def test_denied_column_is_stripped(self):
        result = verify_sql(
            "SELECT id, ssn FROM orders", _config(denied=["ssn"]), "sqlite"
        )
        assert result["allowed"] is False
        assert result["fixed"] == "SELECT id FROM orders"
        assert (
            "Column ssn is denied. Column should be removed from SELECT clause"
            in result["errors"]
        )

    def test_non_denied_column_passes(self):
        result = verify_sql(
            "SELECT id, product_name FROM orders", _config(denied=["ssn"]), "sqlite"
        )
        assert result["allowed"] is True
        assert result["fixed"] is None

    def test_qualified_denied_column_is_stripped(self):
        result = verify_sql(
            "SELECT orders.id, orders.ssn FROM orders", _config(denied=["ssn"]), "sqlite"
        )
        assert result["allowed"] is False
        assert (
            "Column ssn is denied. Column should be removed from SELECT clause"
            in result["errors"]
        )


class TestWildcardExclusion:
    def test_star_excludes_denied_columns(self):
        result = verify_sql("SELECT * FROM orders", _config(denied=["ssn"]), "sqlite")
        assert result["allowed"] is False
        assert result["fixed"] == "SELECT id, product_name, account_id FROM orders"
        assert "ssn" not in result["fixed"]

    def test_table_qualified_star_excludes_denied(self):
        result = verify_sql(
            "SELECT orders.* FROM orders", _config(denied=["ssn"]), "sqlite"
        )
        assert result["allowed"] is False
        assert "ssn" not in result["fixed"]
        assert "product_name" in result["fixed"]


class TestNonBreaking:
    def test_star_without_denied_expands_all(self):
        result = verify_sql("SELECT * FROM orders", _config(), "sqlite")
        assert result["fixed"] == (
            "SELECT id, product_name, ssn, account_id FROM orders"
        )

    def test_plain_query_without_denied_is_allowed(self):
        result = verify_sql("SELECT id FROM orders", _config(), "sqlite")
        assert result["allowed"] is True
        assert result["fixed"] is None


class TestValidation:
    @pytest.mark.parametrize("bad", ["ssn", [1, 2], {"a": 1}])
    def test_invalid_denied_columns_returns_graceful_error(self, bad):
        result = verify_sql("SELECT id FROM orders", _config(denied=bad), "sqlite")
        assert result["allowed"] is False
        assert any("Invalid 'denied_columns'" in e for e in result["errors"])
