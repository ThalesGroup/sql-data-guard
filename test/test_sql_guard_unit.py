import logging
import sqlite3
from sqlite3 import Connection

import pytest

from sql_data_guard import verify_sql


@pytest.fixture(autouse=True)
def pytest_configure():
    logging.getLogger('sqlfluff').setLevel(logging.WARNING)

def _test_sql(sql: str, config: dict, errors: list = None, fix: str = None, dialect: str = "sqlite",
              cnn: Connection = None, data: list = None):
    result = verify_sql(sql, config, dialect)
    if errors is None:
        assert result["errors"] == []
    else:
        assert result["errors"] == errors
    if fix is None:
        assert result["fixed"] is None
        sql_to_use = sql

    else:
        assert result["fixed"] == fix
        sql_to_use = result["fixed"]
    if cnn:
        fetched_data = cnn.execute(sql_to_use).fetchall()
        if data is not None:
            assert fetched_data == data




class TestSQLErrors:
    def test_basic_sql_error(self):
        result = verify_sql("this is not an sql statement ",{})
        assert result["allowed"] == False
        assert "Found unparsable section" in result["errors"][0]


class TestSingleTable:

    @pytest.fixture(scope="class")
    def config(self) -> dict:
        return { "tables": [
                    {
                        "table_name": "orders",
                        "database_name": "orders_db",
                        "columns": ["id", "product_name", "account_id"],
                        "restrictions": [{"column": "id", "value": 123}]
                    }
                ]
            }

    @pytest.fixture(scope="class")
    def cnn(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("CREATE TABLE orders (id INT, product_name TEXT, account_id INT)")
            conn.execute("INSERT INTO orders VALUES (123, 'product1', 123)")
            conn.execute("INSERT INTO orders VALUES (124, 'product2', 124)")
            yield conn

    def test_select_illegal_table(self, config):
        _test_sql("SELECT * FROM users", config, errors=["Table users is not allowed"])

    def test_select_two_illegal_tables(self, config):
        _test_sql("SELECT col1 FROM users AS u1, products AS p1", config,
                  errors=["Table users is not allowed", "Table products is not allowed"])

    def test_select_star(self, config, cnn):
        _test_sql("SELECT * FROM orders WHERE id = 123", config, errors=["SELECT * is not allowed"],
                  fix="SELECT id, product_name, account_id FROM orders WHERE id = 123",
                  cnn=cnn, data=[(123, "product1", 123)])

    def test_two_cols(self, config, cnn):
        _test_sql("SELECT id, product_name FROM orders WHERE id = 123", config, cnn=cnn, data=[(123, 'product1')])


    def test_quote_and_alias(self, config, cnn):
        _test_sql('SELECT "id" AS my_id FROM orders WHERE id = 123', config, cnn=cnn, data=[(123,)])

    def test_sql_with_group_by_and_order_by(self, config, cnn):
        _test_sql("SELECT id FROM orders GROUP BY id ORDER BY id", config,
                  errors=["Missing restriction for table: orders column: id value: 123"],
                  fix="SELECT id FROM orders WHERE id = 123 GROUP BY id ORDER BY id",
                  cnn=cnn, data=[(123,)])

    def test_sql_with_where_and_group_by_and_order_by(self, config):
        _test_sql("SELECT id FROM orders WHERE product_name='' GROUP BY id ORDER BY id",config,
                  errors=["Missing restriction for table: orders column: id value: 123"],
                  fix="SELECT id FROM orders WHERE ( product_name='') AND id = 123 GROUP BY id ORDER BY id")

    def test_col_expression(self, config):
        _test_sql("SELECT col + 1 FROM orders WHERE id = 123", config,
                  errors=["Column col is not allowed. Column removed from SELECT clause",
                          "No legal elements in SELECT clause"])

    def test_select_illegal_col(self, config):
        _test_sql("SELECT col, id FROM orders WHERE id = 123", config,
                  errors=["Column col is not allowed. Column removed from SELECT clause"],
                  fix="SELECT id FROM orders WHERE id = 123")

    def test_select_no_legal_cols(self, config):
        _test_sql("SELECT col1, col2 FROM orders WHERE id = 123", config,
                  errors=["Column col1 is not allowed. Column removed from SELECT clause",
                          "Column col2 is not allowed. Column removed from SELECT clause",
                          "No legal elements in SELECT clause"])

    def test_missing_restriction(self, config):
        _test_sql("SELECT id FROM orders", config,
                  errors=["Missing restriction for table: orders column: id value: 123"],
                  fix="SELECT id FROM orders WHERE id = 123")

    def test_wrong_restriction(self, config):
        _test_sql("SELECT id FROM orders WHERE id = 234", config,
                  errors=["Missing restriction for table: orders column: id value: 123"],
                  fix="SELECT id FROM orders WHERE ( id = 234) AND id = 123")

    def test_table_and_database(self, config):
        _test_sql("SELECT id FROM orders_db.orders AS o WHERE id = 123", config)

    def test_function_call(self, config):
        _test_sql("SELECT COUNT(DISTINCT id) FROM orders_db.orders AS o WHERE id = 123", config)

    def test_function_call_illegal_col(self, config):
        _test_sql("SELECT COUNT(DISTINCT col) FROM orders_db.orders AS o WHERE id = 123", config,
                  errors=["Column col is not allowed. Column removed from SELECT clause",
                          "No legal elements in SELECT clause"])

    def test_table_prefix(self, config):
        _test_sql("SELECT orders.id FROM orders AS o WHERE id = 123", config)

    def test_table_and_db_prefix(self, config):
        _test_sql("SELECT orders_db.orders.id FROM orders_db.orders WHERE orders_db.orders.id = 123", config)

    def test_table_alias(self, config):
        _test_sql("SELECT a.id FROM orders_db.orders AS a WHERE a.id = 123", config)

    def test_bad_restriction(self, config):
        _test_sql("SELECT id FROM orders WHERE id = 123 OR id = 234", config,
                  errors=["Missing restriction for table: orders column: id value: 123"],
                  fix="SELECT id FROM orders WHERE ( id = 123 OR id = 234) AND id = 123")

    def test_sql_injection(self, config):
        _test_sql("SELECT id FROM orders WHERE id = 123 OR 1 = 1", config,
                  errors=["Static expression is not allowed"])

    def test_with_clause(self, config, cnn):
        _test_sql("WITH data AS (SELECT id FROM orders WHERE id = 123) SELECT id FROM data", config,
                  cnn=cnn, data=[(123,)])

    def test_with_clause_missing_restriction(self, config, cnn):
        _test_sql("WITH data AS (SELECT id FROM orders) SELECT id FROM data", config,
                    errors=["Missing restriction for table: orders column: id value: 123"],
                  fix="WITH data AS (SELECT id FROM orders WHERE id = 123) SELECT id FROM data",
                  cnn=cnn, data=[(123,)])





class TestJoinTable:

    @pytest.fixture
    def config(self) -> dict:
        return {
                "tables": [
                    {
                        "table_name": "orders",
                        "database_name": "orders_db",
                        "columns": ["order_id", "account_id", "product_id"],
                        "restrictions": [{"column": "account_id", "value": 123}]
                    },
                    {
                        "table_name": "products",
                        "database_name": "orders_db",
                        "columns": ["product_id", "product_name"],
                    }
                ]
        }

    def test_using(self, config):
        _test_sql("SELECT order_id, account_id, product_name "
                      "FROM orders INNER JOIN products USING (product_id) WHERE account_id = 123",
                  config)