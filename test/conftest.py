import sqlite3
from sqlite3 import Connection
from typing import Set

import pytest

from sql_data_guard import verify_sql


# Features the executed test queries need that older SQLite builds (e.g. Ubuntu 22.04) lack:
# (error message, first SQLite version that supports it)
_OLD_SQLITE_LIMITATIONS = [
    ("RIGHT and FULL OUTER JOINs are not currently supported", (3, 39, 0)),
    ("no such function: CONCAT", (3, 44, 0)),
]


def _execute(cnn: Connection, sql: str) -> list:
    """Runs sql; skips the test only when the installed SQLite is too old for the feature."""
    try:
        return cnn.execute(sql).fetchall()
    except sqlite3.OperationalError as e:
        for message, version in _OLD_SQLITE_LIMITATIONS:
            if message in str(e) and sqlite3.sqlite_version_info < version:
                pytest.skip(f"SQLite {sqlite3.sqlite_version} is older than {version}: {e}")
        raise


def verify_sql_test(
    sql: str,
    config: dict,
    errors: Set[str] = None,
    fix: str = None,
    dialect: str = "sqlite",
    cnn: Connection = None,
    data: list = None,
) -> str:
    result = verify_sql(sql, config, dialect)
    if errors is None:
        assert result["errors"] == []
    else:
        expected_errors = sorted(errors)
        actual_errors = list(result["errors"])
        assert actual_errors == expected_errors
    if len(result["errors"]) > 0:
        assert result["risk"] > 0
    else:
        assert result["risk"] == 0
    if fix is None:
        assert result.get("fixed") is None
        sql_to_use = sql
    else:
        assert result["fixed"] == fix
        sql_to_use = result["fixed"]
    if cnn and data is not None:
        fetched_data = _execute(cnn, sql_to_use)
        if data is not None:
            assert fetched_data == [tuple(row) for row in data]
    return sql_to_use
