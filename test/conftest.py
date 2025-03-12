from sqlite3 import Connection
from typing import Set

from sql_data_guard import verify_sql


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
        assert result["errors"] == set()
    else:
        expected_errors = list(errors)
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
    if cnn and data:
        fetched_data = cnn.execute(sql_to_use).fetchall()
        if data is not None:
            assert fetched_data == [tuple(row) for row in data]
    return sql_to_use


def verify_sql_test_data(
    sql: str, config: dict, cnn: Connection, data: list, dialect: str = "sqlite"
):
    result = verify_sql(sql, config, dialect)
    sql_to_use = result.get("fixed", sql)
    fetched_data = cnn.execute(sql_to_use).fetchall()
    assert fetched_data == [tuple(row) for row in data], fetched_data
