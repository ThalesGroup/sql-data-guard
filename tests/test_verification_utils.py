import sqlglot
import sqlglot.expressions as expr

from sql_data_guard.verification_utils import find_direct, split_to_expressions


def test_split_to_expressions_matching():
    # Scenario: Split expression of matching type (nested AND expressions)
    expression = sqlglot.parse_one("a AND b AND c")
    results = list(split_to_expressions(expression, expr.And))

    # Flattened ANDs should yield elements 'a', 'b', 'c'
    assert len(results) == 3
    assert [r.sql() for r in results] == ["a", "b", "c"]


def test_split_to_expressions_non_matching():
    # Scenario: Do not split expression of non-matching type
    expression = sqlglot.parse_one("a OR b")
    results = list(split_to_expressions(expression, expr.And))

    assert len(results) == 1
    assert results[0].sql() == "a OR b"


def test_find_direct_matching():
    # Scenario: Find direct child matching type
    expression = sqlglot.parse_one("SELECT * FROM t WHERE a AND b")
    results = list(find_direct(expression, expr.Where))

    assert len(results) == 1
    assert isinstance(results[0], expr.Where)
    assert results[0].sql() == "WHERE a AND b"


def test_find_direct_ignore_indirect():
    # Scenario: Ignore indirect child types
    expression = sqlglot.parse_one("SELECT * FROM t WHERE a AND b")
    results = list(find_direct(expression, expr.And))

    assert len(results) == 0
