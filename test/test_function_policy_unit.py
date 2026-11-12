"""allowed_functions (allow-list mode) and the validation of the function lists."""

import pytest

from sql_data_guard import verify_sql


def _config(**extra):
    config = {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id", "product_name", "account_id", "price"],
                "restrictions": [{"column": "account_id", "value": 123}],
            }
        ]
    }
    config.update(extra)
    return config


def _verify(select, dialect="sqlite", **extra):
    return verify_sql(f"SELECT {select} FROM orders WHERE account_id = 123", _config(**extra), dialect)


def test_no_allow_list_means_functions_are_allowed():
    assert _verify("upper(product_name)")["allowed"]


def test_listed_function_is_allowed_case_insensitively():
    assert _verify("UPPER(product_name)", allowed_functions=["upper"])["allowed"]
    assert _verify("upper(product_name)", allowed_functions=["UPPER"])["allowed"]


def test_unlisted_function_is_rejected_and_named():
    result = _verify("lower(product_name)", allowed_functions=["upper"])
    assert not result["allowed"] and result["fixed"] is None
    assert result["errors"] == ["Function lower is not in the allowed functions list"]


def test_function_anywhere_in_the_statement_is_checked():
    for sql in [
        "SELECT id FROM orders WHERE account_id = 123 AND lower(product_name) = 'a'",
        "SELECT id FROM orders WHERE account_id = 123 ORDER BY lower(product_name)",
        "SELECT id FROM orders WHERE account_id = 123 AND id IN (SELECT id FROM orders WHERE account_id = 123 AND lower(product_name) = 'a')",
        "WITH c AS (SELECT lower(product_name) AS n FROM orders WHERE account_id = 123) SELECT n FROM c",
        "SELECT id FROM orders WHERE account_id = 123 GROUP BY id HAVING max(lower(product_name)) = 'a'",
    ]:
        result = verify_sql(sql, _config(allowed_functions=["max"]), "sqlite")
        assert not result["allowed"], sql
        assert any("allowed functions list" in e for e in result["errors"]), (sql, result)


def test_empty_allow_list_means_no_function_at_all():
    assert not _verify("upper(product_name)", allowed_functions=[])["allowed"]
    assert _verify("id", allowed_functions=[])["allowed"]


@pytest.mark.parametrize(
    "select",
    [
        "CASE WHEN id = 1 THEN 'a' ELSE 'b' END",
        "CAST(id AS TEXT)",
        "id + 1",
        "-price",
        "price * 2",
        "product_name || 'x'",
        "(id)",
        "id IN (1, 2)",
        "id BETWEEN 1 AND 2",
        "product_name LIKE 'a%'",
        "id IS NULL",
        "EXISTS (SELECT 1 FROM orders WHERE account_id = 123)",
        # AND / OR are function nodes in newer sqlglot versions
        "id = 1 AND price > 2",
        "id = 1 OR price > 2",
        "NOT (id = 1 AND price > 2)",
        "product_name COLLATE NOCASE",
    ],
)
def test_syntax_constructs_are_not_function_calls(select):
    result = _verify(select, allowed_functions=[])
    assert result["allowed"], (select, result)


def _parsed_as_same_function(a: str, b: str, dialect: str = "sqlite") -> bool:
    """Whether the installed sqlglot normalises two spellings to the same function node."""
    import sqlglot
    import sqlglot.expressions as exp

    def node(sql):
        return type(next(sqlglot.parse_one(f"SELECT {sql}(x, 1, 2)", dialect=dialect).find_all(exp.Func)))

    return node(a) is node(b) and node(a) is not exp.Anonymous


@pytest.mark.parametrize(
    "written,listed",
    [("SUBSTR", "substring"), ("SUBSTRING", "substr")],
)
def test_aliases_of_the_same_function_match(written, listed):
    # how aliases are normalised depends on the sqlglot version, so only check when it does
    if not _parsed_as_same_function(written, listed):
        pytest.skip("this sqlglot version does not normalise these spellings to one function")
    assert _verify(f"{written}(product_name, 1, 2)", allowed_functions=[listed])["allowed"]
    assert not _verify(f"{written}(product_name, 1, 2)", blocked_functions=[listed])["allowed"]


@pytest.mark.parametrize("written", ["SUBSTR", "SUBSTRING"])
def test_listing_both_spellings_always_works(written):
    allowed = ["substr", "substring"]
    assert _verify(f"{written}(product_name, 1, 2)", allowed_functions=allowed)["allowed"]
    assert not _verify(f"{written}(product_name, 1, 2)", blocked_functions=allowed)["allowed"]


@pytest.mark.parametrize(
    "select,allowed",
    [
        ("COUNT(*)", ["count"]),
        ("MAX(price)", ["max"]),
        ("ROUND(price, 2)", ["round"]),
    ],
)
def test_plain_function_names_match(select, allowed):
    assert _verify(select, allowed_functions=allowed)["allowed"]


def test_error_prints_the_name_to_put_in_the_config():
    result = _verify("DATE_TRUNC('day', price)", "postgres", allowed_functions=["count"])
    assert not result["allowed"]
    name = result["errors"][0].split()[1]
    # using exactly that name in the allow-list makes the query pass
    assert _verify("DATE_TRUNC('day', price)", "postgres", allowed_functions=[name])["allowed"]


def test_qualified_function_needs_its_qualified_name():
    sql = "SELECT myschema.count(id) FROM orders WHERE account_id = 123"
    assert not verify_sql(sql, _config(allowed_functions=["count"]), "postgres")["allowed"]
    assert verify_sql(sql, _config(allowed_functions=["myschema.count"]), "postgres")["allowed"]


def test_deny_list_wins_over_allow_list():
    result = _verify("sleep(1)", "mysql", allowed_functions=["sleep"])
    assert not result["allowed"]
    assert result["errors"] == ["Function sleep is not allowed"]
    result = _verify("upper(product_name)", allowed_functions=["upper"], blocked_functions=["upper"])
    assert result["errors"] == ["Function upper is not allowed"]


@pytest.mark.parametrize("key", ["allowed_functions", "blocked_functions"])
@pytest.mark.parametrize("bad", ["upper", ["upper", 1], [""], [None], {"upper": 1}, 5])
def test_invalid_function_list_is_a_config_error(key, bad):
    result = _verify("id", **{key: bad})
    assert not result["allowed"] and result["fixed"] is None
    assert key in result["errors"][0]


def test_standalone_if_is_a_function_but_case_when_is_not():
    assert not _verify("IF(id = 1, 'a', 'b')", "mysql", allowed_functions=[])["allowed"]
    assert _verify("IF(id = 1, 'a', 'b')", "mysql", allowed_functions=["if"])["allowed"]
    assert _verify("CASE WHEN id = 1 THEN 'a' ELSE 'b' END", "mysql", allowed_functions=[])["allowed"]


def test_where_with_and_is_allowed_in_allow_list_mode():
    result = verify_sql(
        "SELECT id FROM orders WHERE account_id = 123 AND price > 1 OR id = 2 AND price < 5",
        _config(allowed_functions=[]),
    )
    assert not [e for e in result["errors"] if e.startswith("Function")], result


@pytest.mark.parametrize(
    "dialect,connector", [("sqlite", "OR"), ("mysql", "OR"), ("mysql", "XOR")]
)
def test_functions_inside_connectors_are_still_checked(dialect, connector):
    result = verify_sql(
        f"SELECT id FROM orders WHERE account_id = 123 AND (upper(product_name) = 'A' {connector} id = 1)",
        _config(allowed_functions=["lower"]),
        dialect,
    )
    assert result["errors"] == ["Function upper is not in the allowed functions list"]
    blocked = verify_sql(
        f"SELECT id FROM orders WHERE account_id = 123 AND (id = 1 {connector} sleep(1) = 0)",
        _config(),
        dialect,
    )
    assert "Function sleep is not allowed" in blocked["errors"]
