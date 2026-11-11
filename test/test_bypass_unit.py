"""Adversarial tests: try to slip forbidden data past verify_sql.

Every scenario is executed the way a real caller would: if the guard approves the
query (or returns a `fixed` rewrite), the SQL that would actually run is executed on a
seeded sqlite database. The test fails if any forbidden marker comes back:
  - column `secret`            -> value contains "SECRET"
  - table `payroll`            -> value contains "SALARY"
  - rows of another account    -> value contains "OTHER"
A failing test is a confirmed bypass (or a wrong assumption in the test).
"""

import sqlite3

import pytest

from sql_data_guard import verify_sql

CONFIG = {
    "tables": [
        {
            "table_name": "orders",
            "columns": ["id", "product_name", "account_id"],
            "restrictions": [{"column": "account_id", "value": 123}],
        },
        {
            "table_name": "customers",
            "columns": ["id", "name"],
            "restrictions": [{"column": "id", "value": 123}],
        },
    ]
}

_FORBIDDEN_MARKERS = ("SECRET", "SALARY", "OTHER")

# ----------------------------------------------------------------------------
# Confirmed bypasses / robustness bugs. Each is an `xfail(strict=True)`: the test
# documents the problem now and fails loudly (XPASS) once it is fixed, at which
# point the entry should be deleted from the tables below.
# HIGH = forbidden data returned, MEDIUM = side effects / inference oracle, LOW = robustness / info.
# ----------------------------------------------------------------------------
_USING = "MEDIUM: columns in JOIN ... USING (...) are not checked against the allow-list (inference oracle)"
_INTO = "MEDIUM: SELECT ... INTO creates a table (write) and is approved"
_LOCK = "LOW: SELECT ... FOR UPDATE takes row locks and is approved"
_PAREN_TABLE = "LOW: parenthesised table/join in FROM raises AttributeError instead of returning a result"
_DEEP = "LOW: deeply nested expression raises RecursionError out of verify_sql"
_FUNC_SIDE = "MEDIUM: side-effecting / outbound / resource-exhausting function is not in the deny-list"
_FUNC_INFO = "LOW: server or environment information function is not in the deny-list"
_CFG_RAISE = "LOW: malformed config raises instead of returning an error result"
_CFG_DUP = "LOW: duplicate table entries in the config silently widen the allow-list"
_SQL_TYPE = "LOW: non-string sql raises TypeError instead of returning an error result"

_KNOWN_SQL = {
    "SELECT id, product_name FROM orders o JOIN customers c USING (secret) WHERE o.account_id = 123 AND c.id = 123": _USING,
    "SELECT id INTO newtable FROM orders WHERE account_id = 123": _INTO,
    "SELECT id FROM orders WHERE account_id = 123 FOR UPDATE": _LOCK,
    "SELECT salary FROM (payroll)": _PAREN_TABLE,
    "SELECT salary FROM (orders JOIN payroll ON 1)": _PAREN_TABLE,
}

_KNOWN_FUNCS = {
    ("SELECT current_setting('server_version')", "postgres"): _FUNC_INFO,
    ("SELECT set_config('x', 'y', false)", "postgres"): _FUNC_SIDE,
    ("SELECT version()", "postgres"): _FUNC_INFO,
    ("SELECT current_user", "postgres"): _FUNC_INFO,
    ("SELECT @@version", "mysql"): _FUNC_INFO,
    ("SELECT randomblob(1000000000)", "sqlite"): _FUNC_SIDE,
    ("SELECT zeroblob(1000000000)", "sqlite"): _FUNC_SIDE,
    ("SELECT sqlite_version()", "sqlite"): _FUNC_INFO,
    ("SELECT getenv('HOME')", "duckdb"): _FUNC_SIDE,
    ("SELECT @@version", "tsql"): _FUNC_INFO,
    ("SELECT UTL_HTTP.REQUEST('http://x')", "oracle"): _FUNC_SIDE,
}

# indexes into CONFIG_EDGE_CASES that currently raise (see below); 28 is the duplicate-table case
_KNOWN_CONFIG_RAISE = {3, 9, 10, 11, 12, 13, 17, 20, 21, 31, 32, 33, 35}
_KNOWN_CONFIG_DUP = {28}


def _xfail(reason):
    return pytest.mark.xfail(strict=True, reason=reason)


def _known_sql(sql):
    if sql in _KNOWN_SQL:
        return _KNOWN_SQL[sql]
    if "((((((((((" in sql and len(sql) > 300:
        return _DEEP
    return None


def _mark_sql(sqls):
    return [
        pytest.param(s, marks=_xfail(_known_sql(s))) if _known_sql(s) else s
        for s in sqls
    ]


@pytest.fixture(scope="module")
def cnn():
    with sqlite3.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE orders (id INT, product_name TEXT, account_id INT, secret TEXT)"
        )
        conn.execute("INSERT INTO orders VALUES (1, 'mine', 123, 'SECRET-1')")
        conn.execute("INSERT INTO orders VALUES (2, 'OTHER-product', 999, 'SECRET-2')")
        conn.execute("CREATE TABLE customers (id INT, name TEXT, secret TEXT)")
        conn.execute("INSERT INTO customers VALUES (123, 'me', 'SECRET-C1')")
        conn.execute("INSERT INTO customers VALUES (999, 'OTHER-cust', 'SECRET-C2')")
        conn.execute("CREATE TABLE payroll (id INT, salary TEXT)")
        conn.execute("INSERT INTO payroll VALUES (1, 'SALARY-100')")
        yield conn


def _leaked(cnn, sql: str):
    """Runs sql on sqlite; returns the first forbidden marker found in the output."""
    try:
        rows = cnn.execute(sql).fetchall()
    except sqlite3.Error:
        return None  # does not run on this engine, nothing leaks
    for row in rows:
        for value in row:
            for marker in _FORBIDDEN_MARKERS:
                if marker in str(value):
                    return f"{marker} in {row}"
    return None


def assert_no_leak(cnn, sql: str, config: dict = CONFIG, dialect: str = "sqlite"):
    result = verify_sql(sql, config, dialect)
    if result["allowed"]:
        sql_to_run = sql
    elif result["fixed"]:
        sql_to_run = result["fixed"]
    else:
        return result  # rejected outright
    leak = _leaked(cnn, sql_to_run)
    assert (
        leak is None
    ), f"LEAK {leak}\n  input:  {sql}\n  runs:   {sql_to_run}\n  result: {result}"
    return result


# --------------------------------------------------------------------------- 1
RESTRICTION_BYPASS = [
    "SELECT id, product_name FROM orders WHERE account_id = 123 OR 1 = 1",
    "SELECT id, product_name FROM orders WHERE account_id = 123 OR account_id = 999",
    "SELECT id, product_name FROM orders WHERE account_id = 999 OR account_id = 123",
    "SELECT id, product_name FROM orders WHERE account_id IN (123, 999)",
    "SELECT id, product_name FROM orders WHERE account_id BETWEEN 100 AND 1000",
    "SELECT id, product_name FROM orders WHERE NOT account_id = 123",
    "SELECT id, product_name FROM orders WHERE NOT (account_id <> 123)",
    "SELECT id, product_name FROM orders WHERE account_id != 123",
    "SELECT id, product_name FROM orders WHERE account_id > 0",
    "SELECT id, product_name FROM orders WHERE account_id = 123 OR id > 0",
    "SELECT id, product_name FROM orders WHERE (account_id = 123 OR id > 0)",
    "SELECT id, product_name FROM orders WHERE account_id = 123 OR TRUE",
    "SELECT id, product_name FROM orders WHERE account_id = id",
    "SELECT id, product_name FROM orders WHERE account_id = account_id",
    "SELECT id, product_name FROM orders WHERE account_id = 123 IS NOT NULL",
    "SELECT id, product_name FROM orders WHERE (account_id = 123) IS NOT 5",
    "SELECT id, product_name FROM orders WHERE CASE WHEN 1=1 THEN 1 ELSE account_id = 123 END",
    "SELECT id, product_name FROM orders WHERE coalesce(account_id = 123, 1)",
    "SELECT id, product_name FROM orders WHERE account_id = 123 OR NULL IS NULL",
    "SELECT id, product_name FROM orders WHERE account_id = '123' OR account_id = '999'",
    "SELECT id, product_name FROM orders WHERE account_id = 123.0 OR 1",
    "SELECT id, product_name FROM orders WHERE account_id = 123 AND 1 OR 1",
    "SELECT id, product_name FROM orders WHERE 1 OR account_id = 123",
    "SELECT id, product_name FROM orders GROUP BY id HAVING account_id = 123",
    "SELECT id, product_name FROM orders GROUP BY id, product_name, account_id HAVING account_id = 123 OR 1",
    "SELECT id, product_name FROM orders WHERE account_id = 123 UNION SELECT id, product_name FROM orders",
    "SELECT id, product_name FROM orders WHERE id IN (SELECT id FROM orders WHERE account_id = 123) OR 1",
    "SELECT id, product_name FROM orders WHERE EXISTS (SELECT 1 FROM orders WHERE account_id = 123)",
    "SELECT id, product_name FROM orders WHERE id = (SELECT 2 WHERE account_id = 123)",
    "SELECT id, product_name FROM (SELECT * FROM orders) t",
    "SELECT id, product_name FROM (SELECT id, product_name, account_id FROM orders) t WHERE account_id = 123 OR 1",
    "WITH o AS (SELECT id, product_name, account_id FROM orders) SELECT id, product_name FROM o",
    "WITH o AS (SELECT id, product_name, account_id FROM orders WHERE account_id = 123) SELECT id, product_name FROM orders",
    "WITH o AS (SELECT id, product_name, account_id FROM orders WHERE account_id = 123) SELECT id, product_name FROM orders",
    "SELECT id, product_name FROM (SELECT id, product_name FROM orders WHERE account_id = 123) t, orders",
    "SELECT id FROM orders WHERE id IN (SELECT id FROM orders WHERE account_id = 123)",
    "SELECT id, product_name FROM orders WHERE account_id = 123 LIMIT 5 OFFSET (SELECT 0)",
    "SELECT id, product_name FROM orders ORDER BY (SELECT 1)",
    "SELECT o.id, o.product_name FROM orders o JOIN orders o2 ON o2.account_id = 123 WHERE o.account_id = 123 OR 1",
    "SELECT o.id, o.product_name FROM orders o JOIN orders o2 ON 1 = 1 WHERE o2.account_id = 123",
    "SELECT o.id, o.product_name FROM orders o, orders p WHERE p.account_id = 123",
    "SELECT o.id, o.product_name FROM orders o, orders p WHERE o.account_id = 123",
    "SELECT o.id, o.product_name FROM orders o LEFT JOIN customers c ON o.account_id = 123",
    "SELECT o.id, o.product_name FROM orders o RIGHT JOIN customers c ON c.id = 123 WHERE c.id = 123",
    "SELECT o.id, o.product_name FROM customers c CROSS JOIN orders o WHERE c.id = 123",
    "SELECT o.id, o.product_name FROM orders o NATURAL JOIN customers c WHERE c.id = 123",
    "SELECT id, product_name FROM orders USE INDEX (x) WHERE account_id = 123 OR 1",
]


RESTRICTION_BYPASS = _mark_sql(RESTRICTION_BYPASS)


@pytest.mark.parametrize("sql", RESTRICTION_BYPASS)
def test_restriction_bypass(cnn, sql):
    assert_no_leak(cnn, sql)


# --------------------------------------------------------------------------- 2
COLUMN_BYPASS = [
    "SELECT * FROM orders WHERE account_id = 123",
    "SELECT o.* FROM orders o WHERE o.account_id = 123",
    "SELECT orders.* FROM orders WHERE account_id = 123",
    "SELECT secret FROM orders WHERE account_id = 123",
    'SELECT "secret" FROM orders WHERE account_id = 123',
    "SELECT `secret` FROM orders WHERE account_id = 123",
    "SELECT [secret] FROM orders WHERE account_id = 123",
    "SELECT SECRET FROM orders WHERE account_id = 123",
    'SELECT "SeCrEt" FROM orders WHERE account_id = 123',
    "SELECT orders.secret FROM orders WHERE account_id = 123",
    "SELECT main.orders.secret FROM orders WHERE account_id = 123",
    "SELECT id AS secret, secret AS id FROM orders WHERE account_id = 123",
    "SELECT id, product_name FROM orders WHERE account_id = 123 ORDER BY secret",
    "SELECT id, product_name FROM orders WHERE account_id = 123 ORDER BY 3",
    "SELECT id, product_name FROM orders WHERE account_id = 123 GROUP BY secret",
    "SELECT max(secret) FROM orders WHERE account_id = 123",
    "SELECT count(*), secret FROM orders WHERE account_id = 123 GROUP BY secret",
    "SELECT group_concat(secret) FROM orders WHERE account_id = 123",
    "SELECT json_object('a', secret) FROM orders WHERE account_id = 123",
    "SELECT json_group_array(secret) FROM orders WHERE account_id = 123",
    "SELECT CASE WHEN 1 THEN secret END FROM orders WHERE account_id = 123",
    "SELECT coalesce(secret, 'x') FROM orders WHERE account_id = 123",
    "SELECT (SELECT secret FROM orders o2 WHERE o2.account_id = 123) FROM orders WHERE account_id = 123",
    "SELECT (SELECT max(secret) FROM orders) FROM orders WHERE account_id = 123",
    "SELECT id FROM orders WHERE account_id = 123 AND EXISTS (SELECT 1 FROM orders o2 WHERE o2.secret LIKE 'SECRET%')",
    "SELECT id FROM orders WHERE account_id = 123 AND secret LIKE 'SECRET-1%'",
    "SELECT id FROM orders WHERE account_id = 123 AND substr(secret, 1, 1) = 'S'",
    "SELECT sum(id) OVER (PARTITION BY secret) FROM orders WHERE account_id = 123",
    "SELECT row_number() OVER (ORDER BY secret) FROM orders WHERE account_id = 123",
    "SELECT id, product_name FROM orders WHERE account_id = 123 AND id IN (SELECT id FROM orders WHERE secret IS NOT NULL)",
    "SELECT t.secret FROM (SELECT * FROM orders WHERE account_id = 123) t",
    "SELECT secret FROM (SELECT id, secret FROM orders WHERE account_id = 123) t",
    "SELECT * FROM (SELECT id, product_name, account_id FROM orders WHERE account_id = 123) t",
    "WITH o AS (SELECT * FROM orders WHERE account_id = 123) SELECT secret FROM o",
    "WITH o AS (SELECT secret AS x FROM orders WHERE account_id = 123) SELECT x FROM o",
    "SELECT x FROM (SELECT secret AS x FROM orders WHERE account_id = 123)",
    "SELECT o.id, c.secret FROM orders o JOIN customers c ON c.id = o.account_id WHERE o.account_id = 123 AND c.id = 123",
    "SELECT id, product_name FROM orders o JOIN customers c USING (secret) WHERE o.account_id = 123 AND c.id = 123",
    "SELECT sub_select.secret FROM orders sub_select WHERE account_id = 123",
    "SELECT secret FROM orders WHERE account_id = 123 AND id IN (SELECT x FROM (SELECT 1 AS secret) t)",
    "SELECT id FROM orders WHERE account_id = 123 AND EXISTS (SELECT secret)",
    "SELECT id FROM orders o WHERE o.account_id = 123 AND EXISTS (SELECT 1 FROM (SELECT 1 AS x) d WHERE secret = 1)",
    "SELECT secret FROM (SELECT id FROM orders WHERE account_id = 123) t, orders",
    "SELECT secret FROM (SELECT id, secret FROM orders WHERE account_id = 123)",
    "WITH c AS (SELECT id, secret FROM orders WHERE account_id = 123) SELECT secret FROM c",
    "WITH c AS (SELECT id, secret FROM orders WHERE account_id = 123) SELECT x.secret FROM c x",
    "SELECT id, product_name FROM orders WHERE account_id = 123 AND (secret) = 'x'",
    "SELECT id, product_name FROM orders WHERE account_id = 123 AND secret IN ('x')",
    "SELECT id, product_name, orders FROM orders WHERE account_id = 123",
    "SELECT id, product_name FROM orders WHERE account_id = 123 LIMIT (SELECT length(secret) FROM orders LIMIT 1)",
]


COLUMN_BYPASS = _mark_sql(COLUMN_BYPASS)


@pytest.mark.parametrize("sql", COLUMN_BYPASS)
def test_column_bypass(cnn, sql):
    result = assert_no_leak(cnn, sql)
    # Boolean-oracle / inference queries return no marker but still use a forbidden column.
    # If the guard approved them unchanged, the forbidden column must not appear in the SQL.
    if result["allowed"]:
        assert "secret" not in sql.lower(), f"approved a query using `secret`: {result}"


# --------------------------------------------------------------------------- 3
TABLE_BYPASS = [
    "SELECT salary FROM payroll",
    "SELECT * FROM payroll",
    "SELECT id FROM orders WHERE account_id = 123 UNION SELECT salary FROM payroll",
    "SELECT id FROM orders WHERE account_id = 123 UNION ALL SELECT salary FROM payroll",
    "SELECT id FROM orders WHERE account_id = 123 INTERSECT SELECT salary FROM payroll",
    "SELECT id FROM orders WHERE account_id = 123 EXCEPT SELECT salary FROM payroll",
    "SELECT salary FROM payroll UNION SELECT id FROM orders WHERE account_id = 123",
    "SELECT id FROM orders WHERE account_id = 123 UNION (SELECT salary FROM payroll)",
    "SELECT id FROM orders WHERE account_id = 123 UNION SELECT 1 UNION SELECT salary FROM payroll",
    "(SELECT id FROM orders WHERE account_id = 123) UNION (SELECT salary FROM payroll)",
    "SELECT id FROM orders WHERE account_id = 123 AND id IN (SELECT salary FROM payroll)",
    "SELECT id FROM orders WHERE account_id = 123 AND EXISTS (SELECT 1 FROM payroll)",
    "SELECT (SELECT salary FROM payroll) FROM orders WHERE account_id = 123",
    "SELECT id FROM orders WHERE account_id = 123 ORDER BY (SELECT salary FROM payroll)",
    "SELECT id FROM orders WHERE account_id = 123 GROUP BY id HAVING max(id) > (SELECT count(*) FROM payroll)",
    "SELECT id FROM orders WHERE account_id = 123 LIMIT (SELECT count(*) FROM payroll)",
    "SELECT id FROM orders WHERE account_id = 123 OFFSET (SELECT count(*) FROM payroll)",
    "SELECT * FROM orders, payroll",
    "SELECT o.id FROM orders o, payroll p WHERE o.account_id = 123",
    "SELECT o.id FROM orders o JOIN payroll p ON 1 = 1 WHERE o.account_id = 123",
    "SELECT salary FROM orders o JOIN payroll p ON p.id = o.id WHERE o.account_id = 123",
    "SELECT id FROM orders WHERE account_id = 123 AND id IN (SELECT id FROM (SELECT id FROM payroll))",
    "WITH p AS (SELECT salary FROM payroll) SELECT salary FROM p",
    "WITH orders AS (SELECT id, salary AS product_name, 123 AS account_id FROM payroll) SELECT id, product_name FROM orders WHERE account_id = 123",
    "WITH a AS (SELECT 1), b AS (SELECT salary FROM payroll) SELECT * FROM b",
    "WITH RECURSIVE r(x) AS (SELECT salary FROM payroll UNION ALL SELECT x FROM r LIMIT 1) SELECT x FROM r",
    "SELECT x FROM (SELECT salary AS x FROM payroll)",
    "SELECT x FROM (SELECT salary AS x FROM payroll) AS orders",
    "SELECT x FROM (SELECT salary AS x FROM payroll) AS customers",
    "SELECT salary FROM payroll AS orders",
    "SELECT salary FROM payroll orders",
    "SELECT salary FROM main.payroll",
    "SELECT salary FROM temp.payroll",
    'SELECT salary FROM "payroll"',
    "SELECT salary FROM `payroll`",
    "SELECT salary FROM PAYROLL",
    "SELECT salary FROM [payroll]",
    "SELECT name FROM sqlite_master",
    "SELECT sql FROM sqlite_master",
    "SELECT name FROM sqlite_schema",
    "SELECT * FROM pragma_table_info('orders')",
    "SELECT * FROM json_each('[1]')",
    "SELECT value FROM json_each((SELECT json_group_array(salary) FROM payroll))",
    "SELECT id FROM orders WHERE account_id = 123 AND (SELECT salary FROM payroll) IS NOT NULL",
    "SELECT salary FROM (payroll)",
    "SELECT salary FROM (orders JOIN payroll ON 1)",
    "SELECT x.salary FROM (SELECT * FROM orders WHERE account_id = 123) o JOIN payroll x ON 1",
    "SELECT * FROM orders LEFT JOIN payroll ON 1 WHERE account_id = 123",
    "SELECT * FROM orders NATURAL JOIN payroll WHERE account_id = 123",
    "SELECT * FROM (SELECT 1) JOIN payroll ON 1",
    "SELECT * FROM (VALUES (1)) JOIN payroll ON 1",
    "SELECT (SELECT salary FROM payroll) AS x",
    "VALUES ((SELECT salary FROM payroll))",
    "SELECT 1 WHERE (SELECT salary FROM payroll) IS NOT NULL",
    "SELECT CASE WHEN (SELECT 1 FROM payroll) THEN 1 END",
    "SELECT salary FROM payroll WHERE 1 = 1",
]


TABLE_BYPASS = _mark_sql(TABLE_BYPASS)


@pytest.mark.parametrize("sql", TABLE_BYPASS)
def test_table_bypass(cnn, sql):
    result = assert_no_leak(cnn, sql)
    if result["allowed"]:
        assert "payroll" not in sql.lower().replace(
            "sqlite_", ""
        ), f"approved a query touching payroll: {result}"


# --------------------------------------------------------------------------- 4
STATEMENT_BYPASS = [
    "SELECT id FROM orders WHERE account_id = 123; DROP TABLE orders",
    "SELECT id FROM orders WHERE account_id = 123;DROP TABLE orders",
    "SELECT id FROM orders WHERE account_id = 123 ; -- x\n DROP TABLE orders",
    "SELECT id FROM orders WHERE account_id = 123; /* x */ DELETE FROM orders",
    "SELECT id FROM orders WHERE account_id = 123;; DROP TABLE orders",
    "; DROP TABLE orders",
    "DROP TABLE orders",
    "DELETE FROM orders",
    "DELETE FROM orders WHERE account_id = 123",
    "UPDATE orders SET account_id = 123",
    "UPDATE orders SET product_name = 'x' WHERE account_id = 123",
    "INSERT INTO orders VALUES (3, 'x', 123, 'y')",
    "INSERT INTO orders SELECT * FROM orders",
    "REPLACE INTO orders VALUES (3, 'x', 123, 'y')",
    "INSERT OR REPLACE INTO orders VALUES (3, 'x', 123, 'y')",
    "CREATE TABLE x AS SELECT * FROM orders",
    "CREATE VIEW v AS SELECT * FROM payroll",
    "CREATE TEMP TABLE t AS SELECT salary FROM payroll",
    "ALTER TABLE orders ADD COLUMN x INT",
    "TRUNCATE TABLE orders",
    "WITH x AS (SELECT 1) DELETE FROM orders",
    "WITH x AS (SELECT 1) UPDATE orders SET id = 1",
    "WITH x AS (SELECT 1) INSERT INTO orders SELECT * FROM orders",
    "INSERT INTO orders VALUES (1,'a',123,'b') RETURNING secret",
    "UPDATE orders SET id = 1 RETURNING secret",
    "DELETE FROM orders RETURNING secret",
    "SELECT id INTO newtable FROM orders WHERE account_id = 123",
    "SELECT id FROM orders WHERE account_id = 123 FOR UPDATE",
    "EXPLAIN SELECT salary FROM payroll",
    "EXPLAIN QUERY PLAN SELECT salary FROM payroll",
    "PRAGMA table_info(orders)",
    "PRAGMA writable_schema = 1",
    "ATTACH DATABASE 'x.db' AS x",
    "DETACH DATABASE x",
    "VACUUM",
    "VACUUM INTO 'x.db'",
    "ANALYZE",
    "REINDEX",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT s",
    "SET search_path = x",
    "SET ROLE admin",
    "SHOW TABLES",
    "DESCRIBE orders",
    "USE other_db",
    "COPY orders TO '/tmp/x'",
    "COPY (SELECT salary FROM payroll) TO STDOUT",
    "CALL some_proc()",
    "EXEC xp_cmdshell 'dir'",
    "GRANT ALL ON orders TO me",
    "LOCK TABLE orders",
    "MERGE INTO orders USING payroll ON 1 WHEN MATCHED THEN DELETE",
    "LOAD DATA INFILE '/etc/passwd' INTO TABLE orders",
    "SELECT 1 INTO OUTFILE '/tmp/x'",
    "DO $$ BEGIN PERFORM 1; END $$",
    "",
    "   ",
    ";",
    "--",
    "/* */",
    "SELECT",
    "SELECT FROM",
    "NOT SQL AT ALL",
    "SELECT 1; SELECT 2",
]


STATEMENT_BYPASS = _mark_sql(STATEMENT_BYPASS)


@pytest.mark.parametrize("sql", STATEMENT_BYPASS)
def test_statement_bypass(sql):
    result = verify_sql(sql, CONFIG, "sqlite")
    assert not result["allowed"], f"approved non-SELECT statement: {result}"


# --------------------------------------------------------------------------- 5
PARSER_DIFFERENTIAL = [
    # comment tricks
    "SELECT id FROM orders WHERE account_id = 123 /* ; DROP TABLE orders */",
    "SELECT id FROM orders WHERE account_id = 123 -- ; DROP TABLE orders",
    "SELECT id FROM orders WHERE account_id = 123 #; DROP TABLE orders",
    "SELECT id/**/FROM/**/orders/**/WHERE/**/account_id=123/**/OR/**/1=1",
    "SELECT id FROM orders WHERE account_id = 123 /*! OR 1=1 */",
    "SELECT id FROM orders WHERE account_id = 123 /*!50000 OR 1=1 */",
    "SELECT id FROM orders /* WHERE account_id = 123 */",
    "SELECT id FROM orders -- WHERE account_id = 123\n",
    "SELECT id FROM orders WHERE account_id = 123 /* unterminated",
    "SELECT id FROM orders /*/ WHERE account_id = 123 /*/ ",
    "SELECT id FROM orders /* /* nested */ WHERE 1=1 */",
    "SELECT /*+ INDEX(orders) */ id FROM orders",
    # quoting / escaping
    "SELECT id FROM orders WHERE account_id = 123 AND product_name = 'a\\' OR 1=1 -- '",
    "SELECT id FROM orders WHERE product_name = 'a'' OR 1=1 --' AND account_id = 123",
    "SELECT id FROM orders WHERE product_name = E'a\\' OR 1=1 --' AND account_id = 123",
    "SELECT id FROM orders WHERE product_name = $$a$$ OR 1=1 AND account_id = 123",
    "SELECT id FROM orders WHERE product_name = N'a' OR 1=1",
    "SELECT id FROM orders WHERE product_name = X'41' OR 1=1",
    "SELECT id FROM orders WHERE product_name = '\u0000' OR 1=1",
    "SELECT id FROM orders WHERE account_id = 123\u0000 OR 1=1",
    "SELECT id FROM orders WHERE account_id = 123 OR 1=1",
    "SELECT id FROM orders WHERE account_id = 123 OR 1=1",
    "SELECT id FROM orders WHERE account_id = 123\tOR\t1=1",
    "SELECT id FROM orders WHERE account_id = 123\nOR\n1=1",
    "SELECT id FROM orders WHERE account_id = 123\rOR\r1=1",
    "SELECT id FROM orders WHERE account_id＝123 OR 1=1",
    "SELECT　id FROM orders WHERE account_id = 123 OR 1=1",
    # identifier tricks
    'SELECT id FROM "orders" WHERE "account_id" = 123 OR 1=1',
    'SELECT id FROM "Orders" WHERE "Account_ID" = 123 OR 1=1',
    "SELECT id FROM ORDERS WHERE ACCOUNT_ID = 123 OR 1=1",
    "SELECT id FROM orders WHERE account_id = 0x7b OR 1=1",
    "SELECT id FROM orders WHERE account_id = 1_23 OR 1=1",
    "SELECT id FROM orders WHERE account_id = +123 OR 1=1",
    "SELECT id FROM orders WHERE account_id = 123e0 OR 1=1",
    "SELECT id FROM orders WHERE account_id = (123) OR 1=1",
    "SELECT id FROM orders WHERE (((account_id = 123))) OR 1=1",
    "SELECT id FROM orders WHERE account_id = 124 - 1 OR 1=1",
    "SELECT id FROM orders WHERE account_id = 123 + 0",
    "SELECT id FROM orders WHERE account_id + 0 = 123 OR 1=1",
    "SELECT id FROM orders WHERE account_id || '' = '123'",
    "SELECT id FROM orders WHERE CAST(account_id AS TEXT) = '123' OR 1=1",
    "SELECT id FROM orders WHERE abs(account_id) = 123",
    "SELECT id FROM orders WHERE account_id = abs(-999)",
    "SELECT id FROM orders WHERE account_id = (SELECT 999)",
    "SELECT id FROM orders WHERE account_id = (SELECT account_id FROM orders WHERE id = 2)",
    "SELECT id FROM orders WHERE account_id = (SELECT 123) OR account_id = (SELECT 999)",
    "SELECT id FROM orders WHERE account_id IN (SELECT 123 UNION SELECT 999)",
    "SELECT id FROM orders WHERE account_id IN (123) OR account_id IN (999)",
    "SELECT id FROM orders WHERE account_id IN (123, NULL)",
    "SELECT id FROM orders WHERE account_id = ANY (SELECT 999)",
    "SELECT id FROM orders WHERE account_id = 123 AND ((1)) OR ((1))",
    # depth / size
    "SELECT id FROM orders WHERE account_id = 123 AND " + "(" * 200 + "1=1" + ")" * 200,
    "SELECT id FROM orders WHERE account_id = 123" + " OR 1=1" * 500,
    "SELECT " + ", ".join(["id"] * 3000) + " FROM orders WHERE account_id = 123",
    "SELECT id FROM " + "(SELECT id FROM " * 40 + "orders" + ")" * 40,
]


PARSER_DIFFERENTIAL = _mark_sql(PARSER_DIFFERENTIAL)


@pytest.mark.parametrize("sql", PARSER_DIFFERENTIAL)
@pytest.mark.parametrize("dialect", ["sqlite", "mysql", "postgres", "duckdb", None])
def test_parser_differential(cnn, sql, dialect):
    # The guard parses with `dialect`; the data is always executed on sqlite.
    try:
        assert_no_leak(cnn, sql, dialect=dialect)
    except RecursionError:
        pytest.fail("RecursionError escaped verify_sql")


# --------------------------------------------------------------------------- 6
FUNCTION_BYPASS = [
    ("SELECT pg_read_file('/etc/passwd')", "postgres"),
    ("SELECT PG_READ_FILE('/etc/passwd')", "postgres"),
    ("SELECT pg_catalog.pg_read_file('/etc/passwd')", "postgres"),
    ("SELECT \"pg_read_file\"('/etc/passwd')", "postgres"),
    ("SELECT pg_read_binary_file('/etc/passwd')", "postgres"),
    ("SELECT pg_ls_dir('/')", "postgres"),
    ("SELECT pg_sleep(10)", "postgres"),
    ("SELECT pg_sleep_for('5 seconds')", "postgres"),
    ("SELECT lo_import('/etc/passwd')", "postgres"),
    ("SELECT lo_export(1, '/tmp/x')", "postgres"),
    ("SELECT * FROM dblink('host=x', 'select 1') AS t(a int)", "postgres"),
    ("SELECT dblink_exec('host=x', 'drop table x')", "postgres"),
    ("SELECT * FROM generate_series(1, 100000000)", "postgres"),
    ("SELECT current_setting('server_version')", "postgres"),
    ("SELECT set_config('x', 'y', false)", "postgres"),
    ("SELECT version()", "postgres"),
    ("SELECT current_user", "postgres"),
    ("SELECT * FROM pg_catalog.pg_tables", "postgres"),
    ("SELECT * FROM information_schema.tables", "postgres"),
    ("SELECT * FROM pg_shadow", "postgres"),
    ("SELECT load_file('/etc/passwd')", "mysql"),
    ("SELECT sleep(10)", "mysql"),
    ("SELECT benchmark(100000000, md5('x'))", "mysql"),
    ("SELECT @@version", "mysql"),
    ("SELECT * FROM mysql.user", "mysql"),
    ("SELECT * FROM information_schema.tables", "mysql"),
    ("SELECT load_extension('x')", "sqlite"),
    ("SELECT readfile('/etc/passwd')", "sqlite"),
    ("SELECT writefile('/tmp/x', 'y')", "sqlite"),
    ("SELECT randomblob(1000000000)", "sqlite"),
    ("SELECT zeroblob(1000000000)", "sqlite"),
    ("SELECT sqlite_version()", "sqlite"),
    ("SELECT * FROM sqlite_master", "sqlite"),
    ("SELECT * FROM read_csv('/etc/passwd')", "duckdb"),
    ("SELECT * FROM read_csv_auto('/etc/passwd')", "duckdb"),
    ("SELECT * FROM read_parquet('/etc/x.parquet')", "duckdb"),
    ("SELECT * FROM read_json('/etc/x.json')", "duckdb"),
    ("SELECT * FROM read_text('/etc/passwd')", "duckdb"),
    ("SELECT * FROM glob('/*')", "duckdb"),
    ("SELECT * FROM '/etc/passwd'", "duckdb"),
    ("SELECT * FROM 'x.csv'", "duckdb"),
    ("SELECT * FROM duckdb_tables()", "duckdb"),
    ("SELECT * FROM duckdb_settings()", "duckdb"),
    ("SELECT getenv('HOME')", "duckdb"),
    ("SELECT * FROM range(100000000)", "duckdb"),
    ("SELECT xp_cmdshell('dir')", "tsql"),
    ("SELECT * FROM OPENROWSET('x', 'y', 'z')", "tsql"),
    ("SELECT * FROM OPENQUERY(x, 'select 1')", "tsql"),
    ("SELECT @@version", "tsql"),
    ("SELECT * FROM sys.tables", "tsql"),
    ("SELECT UTL_HTTP.REQUEST('http://x')", "oracle"),
    ("SELECT * FROM all_tables", "oracle"),
    ("SELECT * FROM dual", "oracle"),
]


FUNCTION_BYPASS = [
    (
        pytest.param(*item, marks=_xfail(_KNOWN_FUNCS[item]))
        if item in _KNOWN_FUNCS
        else item
    )
    for item in FUNCTION_BYPASS
]


@pytest.mark.parametrize("sql,dialect", FUNCTION_BYPASS)
def test_function_and_catalog_bypass(sql, dialect):
    result = verify_sql(sql, CONFIG, dialect)
    assert not result["allowed"], f"approved side-effecting/catalog query: {result}"


# --------------------------------------------------------------------------- 7
CONFIG_EDGE_CASES = [
    {"tables": []},
    {"tables": None},
    {"tables": "orders"},
    {"tables": [None]},
    {"tables": [{}]},
    {"tables": [{"table_name": "orders"}]},
    {"tables": [{"table_name": "orders", "columns": []}]},
    {"tables": [{"table_name": "orders", "columns": None}]},
    {"tables": [{"table_name": "orders", "columns": "id"}]},
    {"tables": [{"table_name": "orders", "columns": [1, 2]}]},
    {"tables": [{"table_name": "orders", "columns": ["id"], "restrictions": None}]},
    {"tables": [{"table_name": "orders", "columns": ["id"], "restrictions": [{}]}]},
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id"}],
            }
        ]
    },
    {
        "tables": [
            {"table_name": "orders", "columns": ["id"], "restrictions": [{"value": 1}]}
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": None}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": [1]}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": {"a": 1}}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "values": []}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [
                    {"column": "id", "values": [1], "operation": "BETWEEN"}
                ],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": 1, "operation": None}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": 1, "operation": 5}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id; DROP TABLE x", "value": 1}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": "1 OR 1=1"}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": "1'; DROP TABLE x; --"}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [
                    {"column": "id", "values": ["1) OR (1=1"], "operation": "IN"}
                ],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": 10**400}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": float("nan")}],
            }
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "id", "value": True}],
            }
        ]
    },
    {
        "tables": [
            {"table_name": "orders", "columns": ["id"]},
            {"table_name": "orders", "columns": ["id", "secret"]},
        ]
    },
    {
        "tables": [
            {
                "table_name": "orders",
                "columns": ["id"],
                "restrictions": [{"column": "secret", "value": 1}],
            }
        ]
    },
    {"tables": [{"table_name": "orders", "columns": ["id"]}], "max_length": -1},
    {"tables": [{"table_name": "orders", "columns": ["id"]}], "max_length": "x"},
    {"tables": [{"table_name": "orders", "columns": ["id"]}], "max_length": None},
    {
        "tables": [{"table_name": "orders", "columns": ["id"]}],
        "blocked_functions": None,
    },
    {
        "tables": [{"table_name": "orders", "columns": ["id"]}],
        "blocked_functions": "sleep",
    },
    {"tables": [{"table_name": "orders", "columns": ["id"]}], "blocked_functions": [1]},
]


CONFIG_EDGE_CASES = [
    (
        pytest.param(
            c, marks=_xfail(_CFG_RAISE if i in _KNOWN_CONFIG_RAISE else _CFG_DUP)
        )
        if (i in _KNOWN_CONFIG_RAISE or i in _KNOWN_CONFIG_DUP)
        else c
    )
    for i, c in enumerate(CONFIG_EDGE_CASES)
]


@pytest.mark.parametrize("config", CONFIG_EDGE_CASES)
def test_bad_config_never_raises_and_never_leaks(cnn, config):
    # A malformed config must produce a result (rejecting is fine), never raise,
    # and never let the forbidden `secret` column through.
    for sql in [
        "SELECT id FROM orders",
        "SELECT id FROM orders WHERE id = 1",
        "SELECT secret FROM orders",
        "SELECT * FROM orders",
    ]:
        result = verify_sql(sql, config, "sqlite")
        assert isinstance(result, dict)
        if result["allowed"] or result["fixed"]:
            sql_to_run = sql if result["allowed"] else result["fixed"]
            assert _leaked(cnn, sql_to_run) is None, f"{config} {sql} -> {result}"


@pytest.mark.parametrize(
    "bad_input",
    [
        pytest.param(None, marks=_xfail(_SQL_TYPE)),
        pytest.param(5, marks=_xfail(_SQL_TYPE)),
        b"SELECT 1",
        ["SELECT 1"],
        {"sql": "SELECT 1"},
    ],
)
def test_non_string_sql_never_raises(bad_input):
    try:
        result = verify_sql(bad_input, CONFIG, "sqlite")
    except Exception as e:  # noqa: BLE001
        pytest.fail(f"verify_sql raised {type(e).__name__} for {bad_input!r}: {e}")
    assert not result["allowed"]


def test_alias_named_like_forbidden_names_is_harmless(cnn):
    # Aliases/CTEs that merely reuse a forbidden name must not expose real forbidden data.
    assert_no_leak(
        cnn,
        "SELECT product_name AS secret FROM orders WHERE account_id = 123 ORDER BY secret",
    )
    assert_no_leak(
        cnn,
        "WITH payroll AS (SELECT id, product_name AS salary FROM orders WHERE account_id = 123) "
        "SELECT salary FROM payroll",
    )


# An invalid dialect name raises ValueError from sqlglot. That is a caller (programmer)
# error and fails closed, so it is intentionally not treated as a bypass.
