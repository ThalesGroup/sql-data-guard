"""
Suggesting and applying allow-list changes to a configuration.

``suggest_config`` answers "what would this query need to be allowed?" and ``merge_config``
applies such an answer to a configuration. Both only deal with the allow-lists (``tables`` and
their columns, and ``databases``); restrictions, masks, function lists, limits and catalogs are
never touched.
"""

import copy
import logging
from typing import List, Optional

from .sql_data_guard import _verify_sql

logger = logging.getLogger(__name__)

# Every configured table needs at least one column, so a table that is being added is seeded
# with this name to let the verification go on and find the columns the query really uses.
_PLACEHOLDER_COLUMN = "__sql_data_guard_placeholder__"
_MAX_ROUNDS = 25
_DELTA_TABLE_KEYS = {"table_name", "database_name", "columns"}


def _identity(table_name: str, database_name: Optional[str]) -> tuple:
    return str(table_name).lower(), str(database_name or "").lower()


def _find_entry(tables: List[dict], table_name: str, database_name: Optional[str]) -> Optional[dict]:
    """The configured table a query reference refers to (same rule as table_matches)."""
    for entry in tables:
        if str(entry["table_name"]).lower() != str(table_name).lower():
            continue
        configured_db = entry.get("database_name")
        if database_name and configured_db and str(database_name).lower() != str(configured_db).lower():
            continue
        return entry
    return None


def _has_column(entry: dict, column: str) -> bool:
    return str(column).lower() in {str(c).lower() for c in entry["columns"]}


def validate_delta(delta: dict) -> None:
    """Raises ValueError unless ``delta`` has the shape produced by ``suggest_config``."""
    if (
        not isinstance(delta, dict)
        or "tables" not in delta
        or not set(delta) <= {"tables", "databases"}
        or not isinstance(delta["tables"], list)
    ):
        raise ValueError(
            "A config delta must be an object with a 'tables' list and optionally a 'databases' list."
        )
    if "databases" in delta:
        databases = delta["databases"]
        if not isinstance(databases, list) or not all(isinstance(d, str) and d for d in databases):
            raise ValueError("'databases' must be a list of database names.")
    for table in delta["tables"]:
        if not isinstance(table, dict) or not set(table) <= _DELTA_TABLE_KEYS:
            raise ValueError(f"A delta table may only have the keys {sorted(_DELTA_TABLE_KEYS)}.")
        if not isinstance(table.get("table_name"), str) or not table["table_name"]:
            raise ValueError("Each delta table must have a 'table_name'.")
        db = table.get("database_name")
        if db is not None and not isinstance(db, str):
            raise ValueError("'database_name' must be a string.")
        columns = table.get("columns")
        if not isinstance(columns, list) or not columns or not all(isinstance(c, str) and c for c in columns):
            raise ValueError("Each delta table must have a non-empty 'columns' list of names.")


def merge_config(config: dict, delta: dict) -> dict:
    """
    Returns a copy of ``config`` with the tables, columns and databases of ``delta`` added.

    A delta table with the same name (and database) as a configured one adds its missing columns
    to it; any other delta table is appended. Delta databases missing from ``databases`` are
    appended (the list is created if the configuration has none). Nothing else in the
    configuration changes, and the arguments are not modified. Raises ValueError if ``delta`` is
    malformed.
    """
    validate_delta(delta)
    merged = copy.deepcopy(config)
    if delta.get("databases"):
        databases = merged.setdefault("databases", [])
        for database in delta["databases"]:
            if database.lower() not in {d.lower() for d in databases}:
                databases.append(database)
    tables = merged.setdefault("tables", [])
    for table in delta["tables"]:
        existing = next(
            (
                t
                for t in tables
                if _identity(t["table_name"], t.get("database_name"))
                == _identity(table["table_name"], table.get("database_name"))
            ),
            None,
        )
        if existing is None:
            tables.append(copy.deepcopy(table))
            continue
        for column in table["columns"]:
            if not _has_column(existing, column):
                existing["columns"].append(column)
    return merged


def suggest_config(sql: str, config: dict, dialect: Optional[str] = None) -> Optional[dict]:
    """
    Suggests the smallest allow-list addition that lets ``sql`` run as written.

    Returns ``{"tables": [{"table_name", "database_name"?, "columns": [...]}, ...]}``, plus
    ``"databases": [...]`` when the configuration has a ``databases`` allow-list that lacks a
    database the query uses, to be applied with ``merge_config``; ``{"tables": []}`` if the
    query needs nothing added. Returns None when widening the allow-list cannot make the query
    acceptable:

    - the query is rejected for another reason (DML/DDL, several statements, a blocked function,
      a column that cannot be attributed to one table, a table of another catalog, an
      unqualified table in strict mode, ...);
    - it needs a table without naming any column (``SELECT *`` or ``COUNT(*)`` on a table the
      configuration does not know), because a configured table must list its columns.

    The suggestion is always checked: ``verify_sql`` on ``merge_config(config, delta)`` accepts
    the query (allowed, or fixable). It never raises.
    """
    try:
        return _suggest(sql, config, dialect)
    except Exception as e:  # noqa: BLE001 - never suggest anything on unexpected errors
        logger.error(f"Unexpected error while suggesting a config ({type(e).__name__})")
        return None


def _suggest(sql: str, config: dict, dialect: Optional[str]) -> Optional[dict]:
    if not isinstance(config, dict) or not isinstance(config.get("tables"), list):
        return None
    working = copy.deepcopy(config)
    tables = working["tables"]
    delta_tables: dict = {}  # identity -> delta table
    delta_databases: List[str] = []

    def delta_for(entry: dict) -> dict:
        key = _identity(entry["table_name"], entry.get("database_name"))
        if key not in delta_tables:
            table = {"table_name": entry["table_name"], "columns": []}
            if entry.get("database_name"):
                table["database_name"] = entry["database_name"]
            delta_tables[key] = table
        return delta_tables[key]

    for _ in range(_MAX_ROUNDS):
        violations: List[dict] = []
        _verify_sql(sql, working, dialect, violations)
        progress = False
        for v in violations:
            if v["type"] == "database":
                known = {d.lower() for d in working["databases"]}
                if v["database_name"].lower() not in known:
                    working["databases"].append(v["database_name"])
                    delta_databases.append(v["database_name"])
                    progress = True
                continue
            if v["type"] not in ("table", "column"):
                return None  # e.g. a catalog mismatch: not an allow-list problem
            if v["table_name"] is None:
                return None  # a column that belongs to no single table: nothing to suggest
            entry = _find_entry(tables, v["table_name"], v["database_name"])
            if v["type"] == "table":
                if entry is None:
                    entry = {
                        "table_name": v["table_name"],
                        **({"database_name": v["database_name"]} if v["database_name"] else {}),
                        "columns": [_PLACEHOLDER_COLUMN],
                    }
                    tables.append(entry)
                    delta_for(entry)  # listed even if no real column is found (rejected below)
                    progress = True
            else:
                if entry is None:
                    return None
                if not _has_column(entry, v["column"]):
                    entry["columns"].append(v["column"])
                    delta_for(entry)["columns"].append(v["column"])
                    progress = True
        if not progress:
            break
    else:
        return None  # did not settle

    # a table can only be configured with columns: drop what was found for none
    result = [t for t in delta_tables.values() if t["columns"]]
    if len(result) != len(delta_tables):
        return None
    delta: dict = {"tables": result}
    if delta_databases:
        delta["databases"] = delta_databases
    merged = merge_config(config, delta)
    verdict = _verify_sql(sql, merged, dialect)
    if not verdict["allowed"] and verdict["fixed"] is None:
        return None
    return delta
