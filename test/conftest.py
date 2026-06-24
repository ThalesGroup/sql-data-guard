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
        assert not result["errors"]
    else:
        # errors is now an ordered list; compare order-insensitively since callers
        # pass an (unordered) set of expected messages.
        assert set(result["errors"]) == set(errors)
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
        fetched_data = cnn.execute(sql_to_use).fetchall()
        if data is not None:
            assert fetched_data == [tuple(row) for row in data]
    return sql_to_use
