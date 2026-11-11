import copy

import pytest

from sql_data_guard import merge_config, suggest_config, verify_sql
from sql_data_guard.sql_data_guard import _verify_sql


def _config(**extra) -> dict:
    return {
        "tables": [
            {"table_name": "orders", "columns": ["id", "total", "customer_id"]},
            {"table_name": "customers", "columns": ["id", "name"]},
        ],
        **extra,
    }


def _tables(delta: dict) -> dict:
    return {t["table_name"]: t["columns"] for t in delta["tables"]}


def _accepted(sql: str, config: dict, dialect: str = "sqlite") -> bool:
    result = verify_sql(sql, config, dialect)
    return result["allowed"] or result["fixed"] is not None


# ---------------------------------------------------------------------------
# suggest_config
# ---------------------------------------------------------------------------

def test_nothing_to_add_for_an_allowed_query():
    assert suggest_config("SELECT id, total FROM orders", _config(), "sqlite") == {"tables": []}


def test_unknown_table_is_added_with_the_columns_the_query_uses():
    delta = suggest_config("SELECT emp_id, amount FROM salaries", _config(), "sqlite")
    assert delta == {"tables": [{"table_name": "salaries", "columns": ["emp_id", "amount"]}]}


def test_unknown_column_of_a_known_table_is_added_to_that_table():
    delta = suggest_config("SELECT id, secret FROM orders", _config(), "sqlite")
    assert delta == {"tables": [{"table_name": "orders", "columns": ["secret"]}]}


def test_columns_used_in_where_join_group_and_order_are_included():
    sql = (
        "SELECT s.dept, SUM(s.amount) FROM orders o JOIN salaries s ON o.id = s.emp_id "
        "WHERE s.year > 2020 GROUP BY s.dept ORDER BY s.dept"
    )
    delta = suggest_config(sql, _config(), "sqlite")
    assert set(_tables(delta)["salaries"]) == {"dept", "amount", "emp_id", "year"}
    assert "orders" not in _tables(delta)


def test_only_what_is_missing_is_suggested_across_several_tables():
    delta = suggest_config(
        "SELECT c.name, s.amount FROM customers c JOIN salaries s ON c.id = s.cust_id",
        _config(), "sqlite",
    )
    assert _tables(delta) == {"salaries": ["amount", "cust_id"]}


def test_table_with_database_name_keeps_it():
    delta = suggest_config("SELECT a FROM hr.salaries", _config(), "postgres")
    assert delta == {"tables": [{"table_name": "salaries", "database_name": "hr", "columns": ["a"]}]}


def test_column_of_a_database_qualified_known_table_goes_to_the_configured_entry():
    config = {"tables": [{"table_name": "orders", "database_name": "public", "columns": ["id"]}]}
    delta = suggest_config("SELECT id, total FROM public.orders", config, "postgres")
    assert delta == {"tables": [{"table_name": "orders", "database_name": "public", "columns": ["total"]}]}


def test_names_are_matched_case_insensitively():
    delta = suggest_config("SELECT ID, Secret FROM ORDERS", _config(), "sqlite")
    assert delta == {"tables": [{"table_name": "orders", "columns": ["Secret"]}]}


def test_subquery_and_cte_tables_are_found():
    sql = "WITH s AS (SELECT amount FROM salaries) SELECT amount FROM s WHERE amount > (SELECT MAX(x) FROM bonuses)"
    delta = suggest_config(sql, _config(), "sqlite")
    assert _tables(delta) == {"salaries": ["amount"], "bonuses": ["x"]}


def test_union_branches_are_both_covered():
    delta = suggest_config("SELECT a FROM t1 UNION SELECT b FROM t2", _config(), "sqlite")
    assert _tables(delta) == {"t1": ["a"], "t2": ["b"]}


def test_join_using_column_is_suggested():
    delta = suggest_config("SELECT o.id FROM orders o JOIN salaries s USING (emp_id)", _config(), "sqlite")
    assert "emp_id" in _tables(delta)["salaries"]


@pytest.mark.parametrize("sql", [
    "SELECT * FROM salaries",            # columns unknown
    "SELECT COUNT(*) FROM salaries",     # table named but no column
    "SELECT 1 FROM salaries",
])
def test_a_new_table_without_named_columns_cannot_be_suggested(sql):
    assert suggest_config(sql, _config(), "sqlite") is None


def test_ambiguous_unqualified_column_over_several_tables_cannot_be_suggested():
    assert suggest_config("SELECT secret FROM orders, customers", _config(), "sqlite") is None


@pytest.mark.parametrize("sql", [
    "DROP TABLE orders",
    "DELETE FROM salaries",
    "UPDATE salaries SET amount = 0",
    "INSERT INTO salaries VALUES (1)",
    "SELECT id FROM orders; SELECT amount FROM salaries",
    "SELECT amount INTO copy FROM salaries",
    "SELECT id FROM orders FOR UPDATE",
    "this is not sql",
    "",
])
def test_non_query_or_unsafe_statements_never_get_a_suggestion(sql):
    assert suggest_config(sql, _config(), "postgres") is None


def test_blocked_function_is_not_made_acceptable():
    assert suggest_config("SELECT pg_read_file(path) FROM files", _config(), "postgres") is None
    assert suggest_config("SELECT secret_fn(a) FROM t", _config(blocked_functions=["secret_fn"]), "postgres") is None


def test_function_allow_list_is_not_widened():
    config = _config(allowed_functions=["count"])
    assert suggest_config("SELECT SUM(amount) FROM salaries", config, "sqlite") is None


def test_masked_column_rules_are_not_widened():
    config = {"tables": [{"table_name": "users", "columns": ["id", "ssn"], "column_masks": {"ssn": "'***'"}}]}
    assert suggest_config("SELECT id FROM users WHERE ssn = '1'", config, "sqlite") is None


def test_force_limit_does_not_prevent_a_suggestion_and_is_still_applied():
    config = _config(force_limit=10)
    delta = suggest_config("SELECT emp_id FROM salaries", config, "sqlite")
    assert delta == {"tables": [{"table_name": "salaries", "columns": ["emp_id"]}]}
    assert verify_sql("SELECT emp_id FROM salaries", merge_config(config, delta), "sqlite")["fixed"].endswith("LIMIT 10")


def test_input_config_is_not_modified():
    config = _config()
    before = copy.deepcopy(config)
    suggest_config("SELECT emp_id FROM salaries", config, "sqlite")
    assert config == before


@pytest.mark.parametrize("config", [None, {}, {"tables": "x"}, {"nope": []}, "str"])
def test_invalid_config_gives_no_suggestion(config):
    assert suggest_config("SELECT id FROM orders", config, "sqlite") is None


def test_invalid_sql_type_and_dialect_give_no_suggestion():
    assert suggest_config(None, _config(), "sqlite") is None
    assert suggest_config("SELECT id FROM orders", _config(), "no-such-dialect") is None


@pytest.mark.parametrize("sql", [
    "SELECT emp_id, amount FROM salaries",
    "SELECT id, secret FROM orders",
    "SELECT c.name, s.amount FROM customers c JOIN salaries s ON c.id = s.cust_id WHERE s.amount > 1",
    "SELECT a FROM t1 UNION ALL SELECT b FROM t2",
    "SELECT id FROM orders WHERE total > (SELECT AVG(total) FROM orders WHERE secret = 1)",
    "WITH x AS (SELECT amount FROM salaries) SELECT amount FROM x",
    "SELECT id FROM orders o WHERE EXISTS (SELECT 1 FROM salaries s WHERE s.emp_id = o.id)",
])
def test_invariant_a_suggestion_always_makes_the_query_acceptable(sql):
    config = _config()
    delta = suggest_config(sql, config, "sqlite")
    assert delta is not None
    merged = merge_config(config, delta)
    assert _accepted(sql, merged)
    # it runs as written: no table or column is left outside the allow-list ...
    left: list = []
    _verify_sql(sql, merged, "sqlite", left)
    assert left == []
    # ... and the suggestion was needed: the original config had violations
    before: list = []
    _verify_sql(sql, config, "sqlite", before)
    assert before != []


@pytest.mark.parametrize("dialect", ["sqlite", "postgres", "mysql", "bigquery", "athena", "duckdb"])
def test_works_across_dialects(dialect):
    delta = suggest_config("SELECT emp_id FROM salaries", _config(), dialect)
    assert delta == {"tables": [{"table_name": "salaries", "columns": ["emp_id"]}]}


def test_suggestion_for_a_correlated_subquery_attributes_the_outer_column():
    sql = "SELECT id FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.id AND o.secret = 1)"
    delta = suggest_config(sql, _config(), "sqlite")
    assert _tables(delta) == {"orders": ["secret"]}


# ---------------------------------------------------------------------------
# merge_config
# ---------------------------------------------------------------------------

def test_merge_adds_a_new_table():
    delta = {"tables": [{"table_name": "salaries", "columns": ["amount"]}]}
    merged = merge_config(_config(), delta)
    assert [t["table_name"] for t in merged["tables"]] == ["orders", "customers", "salaries"]


def test_merge_extends_an_existing_table_without_duplicates():
    delta = {"tables": [{"table_name": "ORDERS", "columns": ["total", "secret", "SECRET"]}]}
    merged = merge_config(_config(), delta)
    orders = [t for t in merged["tables"] if t["table_name"] == "orders"]
    assert len(orders) == 1
    assert orders[0]["columns"] == ["id", "total", "customer_id", "secret"]


def test_merge_keeps_other_settings_and_table_properties():
    config = _config(force_limit=5, blocked_functions=["f"])
    config["tables"][0]["restrictions"] = [{"column": "id", "value": 1, "operation": "="}]
    merged = merge_config(config, {"tables": [{"table_name": "orders", "columns": ["x"]}]})
    assert merged["force_limit"] == 5 and merged["blocked_functions"] == ["f"]
    assert merged["tables"][0]["restrictions"] == config["tables"][0]["restrictions"]


def test_merge_does_not_modify_its_arguments():
    config, delta = _config(), {"tables": [{"table_name": "orders", "columns": ["x"]}]}
    before = copy.deepcopy((config, delta))
    result = merge_config(config, delta)
    result["tables"][0]["columns"].append("y")
    result["tables"].append({})
    assert (config, delta) == before


def test_merge_treats_the_same_table_in_another_database_as_a_different_table():
    config = {"tables": [{"table_name": "orders", "database_name": "a", "columns": ["id"]}]}
    merged = merge_config(config, {"tables": [{"table_name": "orders", "database_name": "b", "columns": ["id"]}]})
    assert len(merged["tables"]) == 2


def test_merge_with_an_empty_delta_is_a_copy():
    config = _config()
    merged = merge_config(config, {"tables": []})
    assert merged == config and merged is not config


@pytest.mark.parametrize("delta", [
    None, [], {}, {"tables": "x"}, {"tables": [], "extra": 1},
    {"tables": [None]},
    {"tables": [{"columns": ["a"]}]},
    {"tables": [{"table_name": "t"}]},
    {"tables": [{"table_name": "t", "columns": []}]},
    {"tables": [{"table_name": "t", "columns": [1]}]},
    {"tables": [{"table_name": "t", "columns": ["a"], "database_name": 5}]},
    {"tables": [{"table_name": "t", "columns": ["a"], "restrictions": []}]},   # only the allow-list
    {"tables": [{"table_name": "t", "columns": ["a"], "column_masks": {}}]},
])
def test_merge_rejects_a_malformed_delta(delta):
    with pytest.raises(ValueError):
        merge_config(_config(), delta)


def test_merge_result_of_a_suggestion_validates_as_a_config():
    config = _config()
    delta = suggest_config("SELECT emp_id FROM salaries", config, "sqlite")
    result = verify_sql("SELECT id FROM orders", merge_config(config, delta), "sqlite")
    assert result["allowed"] is True
