"""Adversarial tests for the richer policy options, on sqlite and duckdb.

Companion of test_bypass_unit.py, with another schema and the options that file does not use:
IN / BETWEEN / comparison restrictions (two on one table), column masks, `database_name`
(an attached `hr` schema, and `strict_database`), `force_limit`, `allowed_functions` and `blocked_functions`.

Every query the guard approves (or the `fixed` rewrite it returns) is executed on the engine of
its dialect, on two databases ("a" and "b") that hold the same allowed data and differ only in
what the policy hides: forbidden and masked column values, rows outside the restrictions (and a
few extra such rows in "b"). A test fails when:
  - a forbidden marker comes back: SSN-REAL (masked value), SALARY / SECRET (forbidden columns
    and tables), OTHER (rows outside a restriction);
  - the result differs between "a" and "b" (or the query fails on only one of them): the result
    depends on hidden data, even if it never shows it (`WHERE ssn LIKE 'S%'`, `ORDER BY salary`);
  - more than `force_limit` rows come back;
  - the guard rejected a query through its internal-error fallback, or returned a `fixed`
    rewrite that fails although the input runs.
"""

import copy
import sqlite3

import duckdb
import pytest

from sql_data_guard import verify_sql

ENGINES = ["sqlite", "duckdb"]
FORCE_LIMIT = 5

POLICY = {
    "tables": [
        {
            "table_name": "employees",
            "columns": ["id", "name", "region", "level", "ssn"],
            "restrictions": [
                {"column": "region", "values": ["eu", "us"], "operation": "IN"},
                {"column": "level", "value": 2, "operation": ">="},
            ],
            "column_masks": [{"column": "ssn", "policy": "redact"}],
        },
        {
            "table_name": "invoices",
            "columns": ["id", "tenant_id", "year", "amount", "memo"],
            "restrictions": [
                {"column": "tenant_id", "value": 7},
                {"column": "year", "values": [2020, 2024], "operation": "BETWEEN"},
            ],
        },
        {
            "table_name": "reviews",
            "database_name": "hr",
            "columns": ["id", "employee_id", "score", "title"],
            "restrictions": [{"column": "score", "value": 4, "operation": "<"}],
        },
    ],
    "force_limit": FORCE_LIMIT,
}

ALLOWED_FUNCTIONS = ["count", "sum", "max", "lower", "coalesce"]
BLOCKED_FUNCTIONS = ["lower", "hex"]

_MARKERS = ("SSN-REAL", "SALARY", "SECRET", "OTHER")
_ERROR = "<error>"
_INTERNAL_ERROR = "Internal error while verifying SQL"

# ----------------------------------------------------------------------------
# Known, unfixed issues: sql -> reason. Currently empty. Each becomes a strict xfail on every engine, and
# fails loudly (XPASS) once fixed, when the entry is deleted.
# ----------------------------------------------------------------------------
_KNOWN: dict = {}


def _params(sqls):
    return [
        (
            pytest.param(s, marks=pytest.mark.xfail(strict=True, reason=_KNOWN[s]))
            if s in _KNOWN
            else s
        )
        for s in sqls
    ]


# ----------------------------------------------------------------------------
# Databases
# ----------------------------------------------------------------------------
def _rows(variant):
    """Rows per table. "b" changes only hidden data and adds rows outside the restrictions."""
    b = variant == "b"
    alt = "ALT-" if b else ""
    employees = [
        (1, "ann", "eu", 3, f"SSN-REAL-{alt}111", f"SALARY-{alt}1"),
        (2, "bob", "us", 2, f"SSN-REAL-{alt}2", f"SALARY-{alt}22"),
        # outside the restrictions (region / level)
        (
            3,
            f"OTHER-{alt}apac",
            "apac",
            3 if not b else 4,
            f"SSN-REAL-{alt}3",
            "SALARY-3",
        ),
        (
            4,
            f"OTHER-{alt}junior",
            "eu",
            1 if not b else 0,
            f"SSN-REAL-{alt}4",
            "SALARY-4",
        ),
    ]
    if b:
        employees.append((5, "OTHER-extra", "us", 1, "SSN-REAL-5", "SALARY-5"))
    invoices = [
        (i, 7, 2020 + (i - 1) // 2, i * 10, f"memo-{i}", f"SECRET-{alt}note-{i}")
        for i in range(1, 11)
    ] + [
        (11, 8, 2022, 1000 if not b else 2000, f"OTHER-{alt}tenant", "SECRET-n"),
        (12, 7, 2019, 3000 if not b else 4000, f"OTHER-{alt}year", "SECRET-n"),
    ]
    if b:
        invoices.append((13, 7, 2025, 5000, "OTHER-future", "SECRET-n"))
    hr_reviews = [
        (1, 1, 2, "good", f"SECRET-{alt}c1"),
        (2, 2, 3, "fine", f"SECRET-{alt}c2"),
        (3, 3, 9 if not b else 8, f"OTHER-{alt}high", "SECRET-c3"),
        # on the boundary of `score < 4`: outside it
        (5, 5, 4, f"OTHER-{alt}boundary", "SECRET-c5"),
    ]
    if b:
        hr_reviews.append((4, 4, 7, "OTHER-extra", "SECRET-c4"))
    # same name as hr.reviews, in the default schema, never configured
    main_reviews = [(1, 1, 1, f"SECRET-{alt}main", "SECRET-m")]
    if b:
        main_reviews.append((2, 2, 2, "SECRET-main-2", "SECRET-m"))
    audit_log = [(1, f"SECRET-{alt}audit")]
    return {
        "employees": employees,
        "invoices": invoices,
        "hr.reviews": hr_reviews,
        "reviews": main_reviews,
        "audit_log": audit_log,
    }


_DDL = {
    "employees": "(id INT, name TEXT, region TEXT, level INT, ssn TEXT, salary TEXT)",
    "invoices": "(id INT, tenant_id INT, year INT, amount INT, memo TEXT, notes TEXT)",
    "hr.reviews": "(id INT, employee_id INT, score INT, title TEXT, comment TEXT)",
    "reviews": "(id INT, employee_id INT, score INT, title TEXT, comment TEXT)",
    "audit_log": "(id INT, event TEXT)",
}


def _connect(engine, variant):
    if engine == "sqlite":
        conn = sqlite3.connect(":memory:")
        conn.execute("ATTACH DATABASE ':memory:' AS hr")
    else:
        conn = duckdb.connect()
        conn.execute("SET threads = 1")  # deterministic row order
        conn.execute("ATTACH ':memory:' AS hr")
    for table, rows in _rows(variant).items():
        conn.execute(f"CREATE TABLE {table} {_DDL[table]}")
        marks = ", ".join("?" * len(rows[0]))
        conn.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)
    return conn


@pytest.fixture(scope="module")
def dbs():
    conns = {(e, v): _connect(e, v) for e in ENGINES for v in ("a", "b")}
    yield conns
    for conn in conns.values():
        conn.close()


@pytest.fixture(params=ENGINES)
def engine(request):
    return request.param


# ----------------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------------
def _outcome(conn, sql):
    try:
        return conn.execute(sql).fetchall()
    except Exception:  # noqa: BLE001 - any engine error: "does not run"
        return _ERROR


def _marker(rows):
    for row in rows:
        for value in row:
            for marker in _MARKERS:
                if marker in str(value):
                    return f"{marker} in {row}"
    return None


def check(dbs, engine, sql, config=POLICY):
    """Verifies sql with the engine's dialect and runs what the guard lets through."""
    result = verify_sql(sql, config, engine)
    internal = [e for e in result["errors"] if e.startswith(_INTERNAL_ERROR)]
    assert not internal, f"rejected only by the internal-error fallback: {result}"
    if result["allowed"]:
        to_run = sql
    elif result["fixed"]:
        to_run = result["fixed"]
    else:
        return result
    a = _outcome(dbs[engine, "a"], to_run)
    b = _outcome(dbs[engine, "b"], to_run)
    context = f"\n  input: {sql}\n  runs:  {to_run}\n  result: {result}"
    for rows in (a, b):
        if rows is not _ERROR:
            leak = _marker(rows)
            assert leak is None, f"LEAK {leak}{context}"
    assert a == b, f"result depends on hidden data:\n  a: {a}\n  b: {b}{context}"
    limit = config.get("force_limit")
    if limit is not None and a is not _ERROR:
        assert len(a) <= limit, f"{len(a)} rows, force_limit is {limit}{context}"
    if result["fixed"] and a is _ERROR:
        assert (
            _outcome(dbs[engine, "a"], sql) is _ERROR
        ), f"fixed SQL does not run, the input does{context}"
    return result


# --------------------------------------------------------------------------- 0
# Positive controls: the policy lets legitimate queries through and returns the allowed
# data, so the bypass tests below cannot pass by rejecting everything.
POSITIVE = [
    ("SELECT id, name FROM employees", [(1, "ann"), (2, "bob")]),
    (
        "SELECT id, ssn FROM employees WHERE region IN ('eu', 'us') AND level >= 2 LIMIT 5",
        [(1, "****"), (2, "****")],
    ),
    ("SELECT count(*) FROM invoices", [(10,)]),
    (
        "SELECT sum(amount) FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2021 AND 2022",
        [(180,)],
    ),
    ("SELECT title FROM hr.reviews WHERE score < 4 LIMIT 5", [("good",), ("fine",)]),
    (
        "SELECT id FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024 LIMIT 100",
        [(1,), (2,), (3,), (4,), (5,)],
    ),
]


@pytest.mark.parametrize("sql,expected", POSITIVE)
def test_positive_controls(dbs, engine, sql, expected):
    result = check(dbs, engine, sql)
    to_run = sql if result["allowed"] else result["fixed"]
    assert to_run, f"legitimate query rejected: {result}"
    rows = [tuple(r) for r in dbs[engine, "a"].execute(to_run).fetchall()]
    assert rows == expected, f"{to_run} -> {rows}"


# --------------------------------------------------------------------------- 1
EMPLOYEES = "SELECT id, name FROM employees WHERE "
RESTRICTION_OPS = [
    # IN restriction + >= restriction on the same table
    "SELECT id, name FROM employees",
    EMPLOYEES + "region IN ('eu', 'us', 'apac') AND level >= 2",
    EMPLOYEES + "region = 'eu' OR region = 'apac' AND level >= 2",
    EMPLOYEES + "(region IN ('eu') OR 1 = 1) AND level >= 2",
    EMPLOYEES + "region IN ('eu') AND level >= 2 OR region = 'apac'",
    EMPLOYEES + "region IN ('eu', NULL) AND level >= 2",
    EMPLOYEES + "region NOT IN ('apac') AND level >= 2",
    EMPLOYEES + "NOT region NOT IN ('eu') AND level >= 2",
    EMPLOYEES + "region LIKE 'e%' AND level >= 2",
    EMPLOYEES + "region = 'EU' AND level >= 2",
    EMPLOYEES + "region = 'eu' COLLATE NOCASE AND level >= 2",
    EMPLOYEES + "lower(region) IN ('eu') AND level >= 2",
    EMPLOYEES + "region IN (SELECT region FROM employees) AND level >= 2",
    EMPLOYEES + "region IN (SELECT 'apac') AND level >= 2",
    EMPLOYEES + "region IN (SELECT 'eu' UNION SELECT 'apac') AND level >= 2",
    EMPLOYEES + "region = (SELECT 'apac') AND level >= 2",
    "SELECT e.id, e.name FROM employees e, (SELECT 'apac' AS eu) d WHERE e.region = eu AND e.level >= 2",
    "SELECT e.id, e.name FROM employees e, (SELECT 'apac' AS eu) d WHERE e.region IN (eu) AND e.level >= 2",
    EMPLOYEES + "region IN ('eu') AND level >= 1",
    EMPLOYEES + "region IN ('eu') AND level = 1",
    EMPLOYEES + "region IN ('eu') AND level > 1",
    EMPLOYEES + "region IN ('eu') AND level IN (1, 2)",
    EMPLOYEES + "region IN ('eu') AND level BETWEEN 1 AND 3",
    EMPLOYEES + "region IN ('eu') AND level >= -2",
    EMPLOYEES + "region IN ('eu') AND level >= '1'",
    EMPLOYEES + "region IN ('eu') AND level >= (SELECT 0)",
    EMPLOYEES + "region IN ('eu') AND level >= 2 - 1",
    EMPLOYEES + "region IN ('eu') AND level >= level",
    EMPLOYEES + "region IN ('eu') AND 2 <= level",
    EMPLOYEES + "region IN ('eu') AND level >= 2 UNION SELECT id, name FROM employees",
    EMPLOYEES
    + "region IN ('eu') AND level >= 2 AND id IN (SELECT id FROM employees WHERE region = 'apac')",
    EMPLOYEES
    + "region IN ('eu') AND level >= 2 AND EXISTS (SELECT 1 FROM employees x WHERE x.name LIKE 'OTHER%')",
    "SELECT a.name, b.name FROM employees a JOIN employees b ON a.id <> b.id WHERE a.region = 'eu' AND a.level >= 2",
    "SELECT name FROM employees GROUP BY name HAVING max(level) >= 2",
    # equality + BETWEEN restriction
    "SELECT id, memo FROM invoices",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2019 AND 2024",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2025",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2024 AND 2020",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year BETWEEN '2020' AND '2024'",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024.5",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year BETWEEN -2024 AND 2024",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024 + 1",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024 OR tenant_id = 8",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year NOT BETWEEN 2020 AND 2024",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year = 2019",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year IN (2019, 2020)",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 AND year >= 2020",
    "SELECT id, memo FROM invoices WHERE tenant_id IN (7, 8) AND year BETWEEN 2020 AND 2024",
    "SELECT id, memo FROM invoices WHERE tenant_id = '7' AND year BETWEEN 2020 AND 2024",
    "SELECT id, memo FROM invoices WHERE tenant_id = -7 AND year BETWEEN 2020 AND 2024",
    "SELECT id, memo FROM invoices WHERE tenant_id = (7) AND year BETWEEN 2020 AND 2024",
    "SELECT id, memo FROM invoices WHERE tenant_id = 7 + 1 AND year BETWEEN 2020 AND 2024",
    "SELECT id, memo FROM invoices WHERE tenant_id = tenant_id AND year BETWEEN 2020 AND 2024",
    "SELECT count(*), sum(amount) FROM invoices WHERE tenant_id = 7",
    "SELECT max(amount) FROM invoices WHERE year BETWEEN 2020 AND 2024",
    "SELECT i.memo FROM invoices i JOIN employees e ON e.id = i.id WHERE i.tenant_id = 7 AND i.year BETWEEN 2020 AND 2024",
    "WITH c AS (SELECT id, memo FROM invoices) SELECT memo FROM c",
    "SELECT memo FROM (SELECT memo, tenant_id, year FROM invoices) t WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024",
    # < restriction on a table of another schema
    "SELECT id, title FROM hr.reviews",
    "SELECT title FROM hr.reviews WHERE score < 10",
    "SELECT title FROM hr.reviews WHERE score <= 4",
    "SELECT title FROM hr.reviews WHERE score < 4 OR score > 4",
    "SELECT title FROM hr.reviews WHERE score = 9",
    "SELECT title FROM hr.reviews WHERE score = 4",
    "SELECT title FROM hr.reviews WHERE score IN (3, 4)",
    "SELECT title FROM hr.reviews WHERE score BETWEEN 0 AND 4",
    "SELECT title FROM hr.reviews WHERE score IN (3, 9)",
    "SELECT title FROM hr.reviews WHERE score BETWEEN 0 AND 9",
    "SELECT title FROM hr.reviews WHERE score < '10'",
    "SELECT title FROM hr.reviews WHERE score < 3 + 6",
    "SELECT title FROM hr.reviews WHERE 4 > score",
    "SELECT title FROM hr.reviews WHERE NOT score >= 4",
    "SELECT r.title FROM hr.reviews r JOIN hr.reviews s ON s.id = r.id WHERE r.score < 4",
    'SELECT title FROM "hr"."reviews"',
    "SELECT title FROM HR.REVIEWS WHERE score < 4",
    # same table name in the default schema, qualified (unqualified: see test_strict_database)
    "SELECT title FROM main.reviews",
    "SELECT title FROM main.reviews WHERE score < 4",
]


@pytest.mark.parametrize("sql", _params(RESTRICTION_OPS))
def test_restriction_operations(dbs, engine, sql):
    check(dbs, engine, sql)


# --------------------------------------------------------------------------- 2
E_WHERE = " FROM employees WHERE region IN ('eu', 'us') AND level >= 2"
MASKING = [
    "SELECT id, ssn" + E_WHERE,
    "SELECT *" + E_WHERE,
    "SELECT e.*" + E_WHERE.replace("employees", "employees e"),
    "SELECT ssn AS x" + E_WHERE,
    "SELECT ssn AS name, name AS ssn" + E_WHERE,
    'SELECT "ssn"' + E_WHERE,
    "SELECT SSN" + E_WHERE,
    "SELECT e.ssn FROM employees e WHERE e.region IN ('eu', 'us') AND e.level >= 2",
    "SELECT ssn, ssn" + E_WHERE,
    "SELECT DISTINCT ssn" + E_WHERE,
    "SELECT (ssn)" + E_WHERE,
    "SELECT upper(ssn)" + E_WHERE,
    "SELECT ssn || ''" + E_WHERE,
    "SELECT length(ssn)" + E_WHERE,
    "SELECT substr(ssn, 1, 3)" + E_WHERE,
    "SELECT CAST(ssn AS TEXT)" + E_WHERE,
    "SELECT coalesce(ssn, 'x')" + E_WHERE,
    "SELECT CASE WHEN ssn LIKE 'SSN-REAL-1%' THEN 1 ELSE 0 END" + E_WHERE,
    "SELECT ssn = 'SSN-REAL-111'" + E_WHERE,
    "SELECT max(ssn)" + E_WHERE,
    "SELECT count(DISTINCT ssn)" + E_WHERE,
    "SELECT group_concat(ssn)" + E_WHERE,
    "SELECT string_agg(ssn, ',')" + E_WHERE,
    "SELECT json_object('s', ssn)" + E_WHERE,
    "SELECT row_number() OVER (ORDER BY ssn)" + E_WHERE,
    "SELECT id" + E_WHERE + " AND ssn LIKE 'SSN-REAL-1%'",
    "SELECT id" + E_WHERE + " AND length(ssn) > 13",
    "SELECT id" + E_WHERE + " AND ssn = ssn",
    "SELECT id" + E_WHERE + " ORDER BY ssn",
    "SELECT id" + E_WHERE + " ORDER BY length(ssn)",
    "SELECT count(*)" + E_WHERE + " GROUP BY ssn",
    "SELECT ssn" + E_WHERE + " GROUP BY ssn",
    "SELECT max(id)" + E_WHERE + " HAVING max(ssn) > ''",
    "SELECT a.id FROM employees a JOIN employees b ON a.ssn = b.ssn WHERE a.region IN ('eu') AND a.level >= 2",
    "SELECT a.id FROM employees a JOIN employees b USING (ssn) WHERE a.region IN ('eu') AND a.level >= 2",
    "SELECT (SELECT ssn FROM employees x WHERE x.id = e.id) FROM employees e WHERE e.region IN ('eu') AND e.level >= 2",
    "SELECT (SELECT max(ssn) FROM employees)" + E_WHERE,
    "SELECT x FROM (SELECT ssn AS x" + E_WHERE + ") t",
    "SELECT t.ssn FROM (SELECT ssn" + E_WHERE + ") t WHERE t.ssn LIKE 'SSN-REAL-1%'",
    "SELECT t.ssn FROM (SELECT *" + E_WHERE + ") t ORDER BY t.ssn",
    "WITH c AS (SELECT id, ssn"
    + E_WHERE
    + ") SELECT id FROM c WHERE ssn LIKE 'SSN-REAL-1%'",
    "WITH c AS (SELECT id, ssn" + E_WHERE + ") SELECT max(ssn) FROM c",
    "SELECT id"
    + E_WHERE
    + " AND id IN (SELECT id FROM employees WHERE ssn LIKE 'SSN-REAL-1%')",
    "SELECT id FROM employees e WHERE e.region IN ('eu') AND e.level >= 2 AND EXISTS (SELECT 1 WHERE e.ssn LIKE 'SSN-REAL-1%')",
    "SELECT name" + E_WHERE + " AND name IN (SELECT ssn FROM employees)",
    "SELECT ssn" + E_WHERE + " UNION SELECT name" + E_WHERE,
    "SELECT name" + E_WHERE + " UNION SELECT ssn FROM employees",
    "SELECT name" + E_WHERE + " ORDER BY ssn",
    "SELECT name AS ssn" + E_WHERE + " ORDER BY ssn",
]


@pytest.mark.parametrize("sql", _params(MASKING))
def test_masking_bypass(dbs, engine, sql):
    check(dbs, engine, sql)


# --------------------------------------------------------------------------- 3
FORBIDDEN = [
    "SELECT salary FROM employees",
    "SELECT name, salary" + E_WHERE,
    "SELECT notes FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024",
    "SELECT id FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024 ORDER BY notes",
    "SELECT comment FROM hr.reviews WHERE score < 4",
    "SELECT * FROM hr.reviews WHERE score < 4",
    "SELECT * FROM audit_log",
    "SELECT * FROM hr.audit_log",
    "SELECT e.name FROM employees e JOIN audit_log a ON 1 = 1"
    + E_WHERE.replace(" FROM employees", ""),
    "SELECT t.salary FROM (SELECT *" + E_WHERE + ") t",
    "SELECT t.notes FROM (SELECT * FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024) t",
    "WITH c AS (SELECT id, salary" + E_WHERE + ") SELECT x.salary FROM c x",
    "SELECT x.comment FROM (SELECT id FROM hr.reviews WHERE score < 4) x",
    "SELECT id" + E_WHERE + " AND salary > ''",
    "SELECT (SELECT max(salary) FROM employees)" + E_WHERE,
    "SELECT id" + E_WHERE + " UNION SELECT salary FROM employees",
    "SELECT id FROM hr.reviews WHERE score < 4 UNION SELECT comment FROM hr.reviews",
]


@pytest.mark.parametrize("sql", _params(FORBIDDEN))
def test_forbidden_columns_and_tables(dbs, engine, sql):
    check(dbs, engine, sql)


# --------------------------------------------------------------------------- 4
I_WHERE = "SELECT id FROM invoices WHERE tenant_id = 7 AND year BETWEEN 2020 AND 2024"
FORCE_LIMIT_BYPASS = [
    I_WHERE,
    I_WHERE + " LIMIT 100",
    I_WHERE + " LIMIT -1",
    I_WHERE + " LIMIT ALL",
    I_WHERE + " LIMIT NULL",
    I_WHERE + " LIMIT '100'",
    I_WHERE + " LIMIT 5.5",
    I_WHERE + " LIMIT 1e2",
    I_WHERE + " LIMIT 2 + 100",
    I_WHERE + " LIMIT (SELECT 100)",
    I_WHERE + " LIMIT 5 OFFSET 0",
    I_WHERE + " LIMIT 0, 100",
    I_WHERE + " LIMIT 100 OFFSET 2",
    I_WHERE + " FETCH FIRST 100 ROWS ONLY",
    I_WHERE + " UNION ALL " + I_WHERE,
    "(" + I_WHERE + " LIMIT 3) UNION ALL (" + I_WHERE + " LIMIT 3)",
    I_WHERE + " LIMIT 3 UNION ALL " + I_WHERE + " LIMIT 3",
    "WITH c AS (" + I_WHERE + ") SELECT id FROM c",
    "WITH c AS (" + I_WHERE + " LIMIT 100) SELECT id FROM c LIMIT 100",
    "SELECT id FROM (" + I_WHERE + " LIMIT 100) t",
    "SELECT a.id FROM invoices a, invoices b WHERE a.tenant_id = 7 AND a.year BETWEEN 2020 AND 2024 "
    "AND b.tenant_id = 7 AND b.year BETWEEN 2020 AND 2024",
    I_WHERE + " ORDER BY id DESC LIMIT 100",
]


@pytest.mark.parametrize("sql", _params(FORCE_LIMIT_BYPASS))
def test_force_limit_bypass(dbs, engine, sql):
    check(dbs, engine, sql)


# --------------------------------------------------------------------------- 5
def _with(**options):
    config = copy.deepcopy(POLICY)
    config.update(options)
    return config


ALLOWED_CONFIG = _with(allowed_functions=ALLOWED_FUNCTIONS)
BLOCKED_CONFIG = _with(blocked_functions=BLOCKED_FUNCTIONS)

NOT_IN_ALLOWED_LIST = [
    "SELECT upper(name)" + E_WHERE,
    "SELECT UPPER(name)" + E_WHERE,
    'SELECT "upper"(name)' + E_WHERE,
    "SELECT main.upper(name)" + E_WHERE,
    "SELECT lower(upper(name))" + E_WHERE,
    "SELECT coalesce(NULL, hex(name))" + E_WHERE,
    "SELECT name" + E_WHERE + " AND length(name) > 1",
    "SELECT name" + E_WHERE + " ORDER BY random()",
    "SELECT count(*)" + E_WHERE + " GROUP BY abs(level)",
    "SELECT count(*)" + E_WHERE + " HAVING min(level) > 0",
    "SELECT name" + E_WHERE + " LIMIT abs(-1)",
    "SELECT (SELECT upper('x'))" + E_WHERE,
    "SELECT u FROM (SELECT upper(name) AS u" + E_WHERE + ") t",
    "WITH c AS (SELECT upper(name) AS u" + E_WHERE + ") SELECT u FROM c",
    "SELECT name" + E_WHERE + " AND id IN (SELECT abs(id) FROM employees)",
    "SELECT count(*) FILTER (WHERE length(name) > 1)" + E_WHERE,
    "SELECT row_number() OVER ()" + E_WHERE,
    "SELECT substr(name, 1)" + E_WHERE,
    "SELECT printf('%s', name)" + E_WHERE,
    "SELECT typeof(name)" + E_WHERE,
    "SELECT current_timestamp" + E_WHERE,
    "SELECT date('now')" + E_WHERE,
    "SELECT name" + E_WHERE + " UNION SELECT upper(name)" + E_WHERE,
]

IN_ALLOWED_LIST = [
    "SELECT count(*)" + E_WHERE,
    "SELECT COUNT(*)" + E_WHERE,
    "SELECT lower(name)" + E_WHERE,
    "SELECT coalesce(name, 'x')" + E_WHERE,
    "SELECT max(level), sum(level)" + E_WHERE,
    "SELECT CAST(level AS TEXT)" + E_WHERE,
    "SELECT CASE WHEN level > 2 THEN 'a' ELSE 'b' END" + E_WHERE,
    "SELECT name" + E_WHERE + " AND EXISTS (SELECT 1)",
]

BLOCKED = [
    "SELECT lower(name)" + E_WHERE,
    "SELECT LOWER(name)" + E_WHERE,
    'SELECT "lower"(name)' + E_WHERE,
    "SELECT main.lower(name)" + E_WHERE,
    "SELECT hex(name)" + E_WHERE,
    "SELECT name" + E_WHERE + " AND lower(name) = 'ann'",
    "SELECT x FROM (SELECT lower(name) AS x" + E_WHERE + ") t",
    "WITH c AS (SELECT hex(name) AS x" + E_WHERE + ") SELECT x FROM c",
    "SELECT name" + E_WHERE + " ORDER BY lower(name)",
    "SELECT coalesce(lower(name), '')" + E_WHERE,
]


def _function_errors(result):
    return [e for e in result["errors"] if e.startswith("Function ")]


@pytest.mark.parametrize("sql", _params(NOT_IN_ALLOWED_LIST))
def test_function_outside_allowed_list(dbs, engine, sql):
    result = check(dbs, engine, sql, ALLOWED_CONFIG)
    assert not result["allowed"] and result["fixed"] is None, f"not rejected: {result}"


@pytest.mark.parametrize("sql", _params(IN_ALLOWED_LIST))
def test_function_in_allowed_list(dbs, engine, sql):
    result = check(dbs, engine, sql, ALLOWED_CONFIG)
    assert not _function_errors(result), result
    assert result["allowed"] or result["fixed"], f"legitimate query rejected: {result}"


@pytest.mark.parametrize("sql", _params(BLOCKED))
def test_blocked_function(dbs, engine, sql):
    result = check(dbs, engine, sql, BLOCKED_CONFIG)
    assert not result["allowed"] and result["fixed"] is None, f"not rejected: {result}"
    assert _function_errors(result), result


def test_function_not_blocked(dbs, engine):
    result = check(dbs, engine, "SELECT upper(name)" + E_WHERE, BLOCKED_CONFIG)
    assert result["allowed"] or result["fixed"], result


# --------------------------------------------------------------------------- 6
# Without strict_database an unqualified `reviews` matches hr.reviews, and the database reads
# main.reviews (documented). With it, every table needs its database and the query must say it.
def _strict_policy():
    config = copy.deepcopy(POLICY)
    for table in config["tables"]:
        table.setdefault("database_name", "main")
    config.update(databases=["main", "hr"], strict_database=True)
    return config


STRICT_POLICY = _strict_policy()

STRICT_DATABASE = [
    "SELECT title FROM reviews WHERE score < 4",
    "SELECT id, title FROM reviews",
    "SELECT title FROM hr.reviews WHERE score < 4 UNION SELECT title FROM reviews WHERE score < 4",
    "SELECT title FROM hr.reviews WHERE id IN (SELECT id FROM reviews)",
    "SELECT title FROM (SELECT title FROM reviews) t",
    "SELECT r.title FROM hr.reviews r JOIN reviews x ON x.id = r.id WHERE r.score < 4",
    "SELECT title FROM main.reviews",
    "SELECT title FROM temp.reviews",
    "SELECT id, name FROM employees",
    "SELECT id FROM main.employees e JOIN audit_log a ON 1 = 1",
    "SELECT title FROM hr.main.reviews",
]


@pytest.mark.parametrize("sql", _params(STRICT_DATABASE))
def test_strict_database(dbs, engine, sql):
    check(dbs, engine, sql, STRICT_POLICY)


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT title FROM hr.reviews WHERE score < 4 LIMIT 5", [("good",), ("fine",)]),
        ("SELECT id, name FROM main.employees", [(1, "ann"), (2, "bob")]),
        (
            "WITH reviews AS (SELECT title FROM hr.reviews WHERE score < 4) "
            "SELECT title FROM reviews LIMIT 5",
            [("good",), ("fine",)],
        ),
    ],
)
def test_strict_database_positive(dbs, engine, sql, expected):
    result = check(dbs, engine, sql, STRICT_POLICY)
    to_run = sql if result["allowed"] else result["fixed"]
    assert to_run, f"legitimate query rejected: {result}"
    assert [tuple(r) for r in dbs[engine, "a"].execute(to_run).fetchall()] == expected


# --------------------------------------------------------------------------- 7
def _table(name, **changes):
    table = copy.deepcopy(next(t for t in POLICY["tables"] if t["table_name"] == name))
    table.update(changes)
    return table


def _replace_table(name, **changes):
    return {
        **POLICY,
        "tables": [
            _table(name, **changes) if t["table_name"] == name else t
            for t in POLICY["tables"]
        ],
    }


def _employee_masks(masks):
    return _replace_table("employees", column_masks=masks)


INVALID_CONFIGS = [
    # column masks
    _employee_masks("ssn"),
    _employee_masks([{"column": "salary", "policy": "redact"}]),
    _employee_masks([{"column": "region", "policy": "redact"}]),
    _employee_masks([{"column": "ssn", "policy": "hash"}]),
    _employee_masks([{"column": "ssn"}]),
    _employee_masks([{"column": "ssn", "policy": "redact", "replacement": 0}]),
    _employee_masks([{"policy": "redact"}]),
    _employee_masks([{"column": "ssn", "policy": "redact"}] * 2),
    _employee_masks([None]),
    # force_limit
    _with(force_limit=0),
    _with(force_limit=-1),
    _with(force_limit=True),
    _with(force_limit="5"),
    _with(force_limit=5.0),
    # function lists
    _with(allowed_functions="count"),
    _with(allowed_functions=[1]),
    _with(allowed_functions=[""]),
    _with(blocked_functions={"lower": True}),
    _with(strict_database="yes"),
    _with(strict_database=True),  # employees and invoices have no database_name
    _with(databases=["main"]),  # hr.reviews is not in it
    # tables
    _replace_table("reviews", database_name=5),
    _replace_table("reviews", table_name=None),
    _replace_table("reviews", table_name=""),
    _replace_table("employees", columns=["id", None]),
    {**POLICY, "tables": POLICY["tables"] + [_table("reviews")]},
    # restrictions
    _replace_table("invoices", restrictions={"column": "tenant_id", "value": 7}),
    _replace_table("invoices", restrictions=[{"column": "tenant_id", "value": [7]}]),
    _replace_table("invoices", restrictions=[{"column": "tenant_id", "value": None}]),
    _replace_table(
        "invoices", restrictions=[{"column": "tenant_id", "value": float("inf")}]
    ),
    _replace_table(
        "invoices", restrictions=[{"column": "tenant_id", "values": [7, None]}]
    ),
    _replace_table(
        "invoices", restrictions=[{"column": "tenant_id or 1=1", "value": 7}]
    ),
    _replace_table("invoices", restrictions=[{"column": "i.tenant_id", "value": 7}]),
    _replace_table("invoices", restrictions=[{"column": 7, "value": 7}]),
    _replace_table(
        "invoices",
        restrictions=[
            {"column": "year", "values": [2024, 2020], "operation": "BETWEEN"}
        ],
    ),
    _replace_table(
        "invoices",
        restrictions=[{"column": "year", "values": ["a", "b"], "operation": "BETWEEN"}],
    ),
    _replace_table(
        "invoices",
        restrictions=[
            {"column": "year", "values": [True, 2024], "operation": "BETWEEN"}
        ],
    ),
    _replace_table(
        "invoices",
        restrictions=[{"column": "year", "value": "2020", "operation": ">="}],
    ),
    _replace_table(
        "invoices", restrictions=[{"column": "year", "value": True, "operation": ">="}]
    ),
    _replace_table(
        "invoices", restrictions=[{"column": "year", "values": [], "operation": "IN"}]
    ),
    _replace_table(
        "invoices",
        restrictions=[{"column": "year", "values": [True], "operation": "IN"}],
    ),
    _replace_table(
        "invoices", restrictions=[{"column": "year", "value": 2020, "operation": "!="}]
    ),
    _replace_table(
        "invoices",
        restrictions=[{"column": "year", "value": 2020, "operation": "LIKE"}],
    ),
    _replace_table(
        "invoices", restrictions=[{"column": "year", "value": 2020, "operation": ["="]}]
    ),
]

CONFIG_PROBES = [
    "SELECT id, name FROM employees",
    "SELECT *" + E_WHERE,
    "SELECT id, memo FROM invoices WHERE tenant_id = 7",
    "SELECT title FROM hr.reviews",
]


@pytest.mark.parametrize("config", INVALID_CONFIGS)
def test_invalid_config_is_rejected(dbs, engine, config):
    # An invalid policy is reported as a configuration error, whatever the query.
    for sql in CONFIG_PROBES:
        result = check(dbs, engine, sql, config)
        assert not result["allowed"] and result["fixed"] is None, f"{config}\n{result}"


def _add_restriction(name, restriction):
    """The policy with one more restriction on a table: never less strict than POLICY."""
    restrictions = _table(name)["restrictions"] + [restriction]
    return _replace_table(name, restrictions=restrictions)


VALID_EDGE_CONFIGS = [
    # `=` with several values: the column may equal any of them
    _add_restriction("invoices", {"column": "tenant_id", "values": [7, 9]}),
    # string and boolean restriction values
    _add_restriction("employees", {"column": "region", "value": "eu"}),
    _add_restriction("employees", {"column": "region", "value": "e'u"}),
    _add_restriction("employees", {"column": "level", "value": True}),
    # a quoted column name
    _replace_table(
        "invoices",
        restrictions=[
            {"column": '"tenant_id"', "value": 7},
            {"column": "year", "values": [2020, 2024], "operation": "BETWEEN"},
        ],
    ),
    # masks with a custom replacement, including a quote
    _employee_masks([{"column": "ssn", "policy": "redact", "replacement": "x'--"}]),
]


@pytest.mark.parametrize("config", VALID_EDGE_CONFIGS)
def test_valid_edge_configs_hold(dbs, engine, config):
    for sql in CONFIG_PROBES + MASKING[:6] + RESTRICTION_OPS[:12]:
        check(dbs, engine, sql, config)
