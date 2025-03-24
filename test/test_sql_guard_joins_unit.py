import json
import logging
import os
import sqlite3
from sqlite3 import Connection
from typing import Set, Generator

import pytest

from sql_data_guard import verify_sql

def _test_sql(sql: str, config: dict, errors: Set[str] = None, fix: str = None, dialect: str = "sqlite",
              cnn: Connection = None, data: list = None):
    result = verify_sql(sql, config, dialect)
    if errors is None:
        assert result["errors"] == set()
    else:
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
    if cnn and data:
        fetched_data = cnn.execute(sql_to_use).fetchall()
        if data is not None:
            assert fetched_data == [tuple(row) for row in data]

class TestInvalidQueries:

    @pytest.fixture(scope="class")
    def cnn(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS orders_db")

            # Creating products table
            conn.execute("""
            CREATE TABLE orders_db.products1 (
                id INT,
                prod_name TEXT,
                deliver TEXT,
                access TEXT,
                date TEXT,
                cust_id TEXT
            )""")

            # Insert values into products1 table
            conn.execute("INSERT INTO products1 VALUES (324, 'prod1', 'delivered', 'granted', '27-02-2025', 'c1')")
            conn.execute("INSERT INTO products1 VALUES (324, 'prod2', 'delivered', 'pending', '27-02-2025', 'c1')")
            conn.execute("INSERT INTO products1 VALUES (435, 'prod2', 'delayed', 'pending', '02-03-2025', 'c2')")
            conn.execute("INSERT INTO products1 VALUES (445, 'prod3', 'shipped', 'granted', '28-02-2025', 'c3')")

            # Creating customers table
            conn.execute("""
            CREATE TABLE orders_db.customers (
                id INT,
                cust_id TEXT,
                cust_name TEXT,
                prod_name TEXT)""")

            # Insert values into customers table
            conn.execute("INSERT INTO customers VALUES (324, 'c1', 'cust1', 'prod1')")
            conn.execute("INSERT INTO customers VALUES (435, 'c2', 'cust2', 'prod2')")
            conn.execute("INSERT INTO customers VALUES (445, 'c3', 'cust3', 'prod3')")

            yield conn


    @pytest.fixture(scope="class")
    def config(self) -> dict:
        return {
            "tables": [
                {
                    "table_name": "products1",
                    "database_name": "orders_db",
                    "columns": ["id", "prod_name", "deliver", "access", "date", "cust_id"],
                    "restrictions": [
                        {"column": "access", "value": "granted"},
                        {"column": "date", "value": "27-02-2025"},
                        {"column": "cust_id", "value": "c1"}
                    ]
                },
                {
                    "table_name": "customers",
                    "database_name": "orders_db",
                    "columns": ["id", "cust_id", "cust_name", "prod_name", "access"],
                    "restrictions": [
                        {"column": "id", "value": 324},
                        {"column": "cust_id", "value": "c1"},
                        {"column": "cust_name", "value": "cust1"},
                        {"column": "prod_name", "value": "prod1"},
                        {"column": "access", "value": "granted"}
                    ]
                }
            ]
        }

    def test_access_denied(self, config):
        result = verify_sql('''SELECT id, prod_name FROM products1 
        WHERE id = 324 AND access = 'granted' AND date = '27-02-2025'
        AND cust_id = 'c1' ''', config)
        assert result["allowed"] == True, result                         # changed from select id, prod_name to this query

    def test_restricted_access(self, config):
        result = verify_sql('''SELECT id, prod_name, deliver, access, date, cust_id 
        FROM products1 WHERE access = 'granted'
        AND date = '27-02-2025' AND cust_id = 'c1' ''', config)            # Changed from select * to this query
        assert result["allowed"] == True, result

    def test_invalid_query1(self, config):
        res = verify_sql("SELECT I from H", config)
        assert not res["allowed"]                                      # gives error only when invalid table is mentioned
        assert 'Table H is not allowed' in res['errors']

    def test_invalid_select(self, config):
        res = verify_sql('''SELECT id, prod_name, deliver FROM 
        products1 WHERE id = 324 AND access = 'granted' 
        AND date = '27-02-2025' AND cust_id = 'c1' ''', config)
        assert res['allowed'] == True, res                       #changed from select id, prod_name, deliver from products1 where id = 324 to this

# checking error
    def test_invalid_select_error_check(self, config):
        res = verify_sql('''select id, prod_name, deliver from products1 where id = 324 ''', config)
        assert not res['allowed']
        assert 'Missing restriction for table: products1 column: access value: granted' in res['errors']
        assert 'Missing restriction for table: products1 column: cust_id value: c1' in res['errors']
        assert 'Missing restriction for table: products1 column: date value: 27-02-2025' in res['errors']

    def test_missing_col(self, config):
        res = verify_sql("SELECT prod_details from products1 where id = 324", config)
        assert not res["allowed"]
        assert "Column prod_details is not allowed. Column removed from SELECT clause" in res['errors']

    def test_insert_row_not_allowed(self, config):
        res = verify_sql("INSERT into products1 values(554, 'prod4', 'shipped', 'granted', '28-02-2025', 'c2')", config)
        assert res["allowed"] == False, res
        assert "INSERT statement is not allowed" in res["errors"], res

    def test_insert_row_not_allowed1(self, config):
        res = verify_sql("INSERT into products1 values(645, 'prod5', 'shipped', 'granted', '28-02-2025', 'c2')", config)
        assert res["allowed"] == False, res
        assert "INSERT statement is not allowed" in res["errors"], res

    def test_missing_restriction(self, config, cnn):
        cursor = cnn.cursor()
        sql = "SELECT id, prod_name FROM products1 WHERE id = 324"
        cursor.execute(sql)
        result = cursor.fetchall()
        expected = [(324, 'prod1'), (324, 'prod2')]
        assert result == expected
        result = verify_sql(sql, config)
        assert not result["allowed"], result
        cursor.execute(result["fixed"])
        assert cursor.fetchall() == [(324, "prod1")]

    def test_using_cnn(self, config,cnn):
        cursor = cnn.cursor()
        sql = "SELECT id, prod_name FROM products1 WHERE id = 324 and access = 'granted' "
        cursor.execute(sql)
        res = cursor.fetchall()
        expected = [(324, 'prod1')]
        assert res == expected
        res = verify_sql(sql, config)
        assert not res['allowed'], res
        cursor.execute(res['fixed'])
        assert cursor.fetchall() == [(324, "prod1")]

    def test_update_value(self,config):
        res = verify_sql("Update products1 set id = 224 where id = 324",config)
        assert res['allowed'] == False, res
        assert "UPDATE statement is not allowed" in res['errors']

class TestJoins:

    @pytest.fixture(scope="class")
    def cnn(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS orders_db")

            # Creating products table
            conn.execute("""
                CREATE TABLE orders_db.products1 (
                    id INT,
                    prod_name TEXT,
                    deliver TEXT,
                    access TEXT,
                    date TEXT,
                    cust_id TEXT
                )""")

            # Insert values into products1 table
            conn.execute("INSERT INTO products1 VALUES (324, 'prod1', 'delivered', 'granted', '27-02-2025', 'c1')")
            conn.execute("INSERT INTO products1 VALUES (324, 'prod2', 'delivered', 'pending', '27-02-2025', 'c1')")
            conn.execute("INSERT INTO products1 VALUES (435, 'prod2', 'delayed', 'pending', '02-03-2025', 'c2')")
            conn.execute("INSERT INTO products1 VALUES (445, 'prod3', 'shipped', 'granted', '28-02-2025', 'c3')")

            # Creating customers table
            conn.execute("""
                CREATE TABLE orders_db.customers (
                    id INT,
                    cust_id TEXT,
                    cust_name TEXT,
                    prod_name TEXT)""")

            # Insert values into customers table
            conn.execute("INSERT INTO customers VALUES (324, 'c1', 'cust1', 'prod1')")
            conn.execute("INSERT INTO customers VALUES (435, 'c2', 'cust2', 'prod2')")
            conn.execute("INSERT INTO customers VALUES (445, 'c3', 'cust3', 'prod3')")

            yield conn

    @pytest.fixture(scope="class")
    def config(self) -> dict:
        return {
        "tables": [
            {
                "table_name": "products1",
                "database_name": "orders_db",
                "columns": ["id", "prod_name", "category"],
                "restrictions": [{"column": "id", "value": 324}]
            },
            {
                "table_name": "customers",
                "database_name": "orders_db",
                "columns": ["cust_id", "cust_name", "access"],
                "restrictions": [{"column": "access", "value": "restricted"}]
            }
        ]
    }

    def test_restriction_passed(self, config):
        res = verify_sql('SELECT id, prod_name from products1 where id = 324 and access = "granted" ',config)
        assert res["allowed"] == True, res

    def test_restriction_restricted(self, config):
        res = verify_sql('SELECT id, prod_name from products1 where id = 435',config)
        assert res["allowed"] == False, res

    def test_inner_join_on_id(self, config):
        res = verify_sql('''SELECT id, prod_name FROM products1
         INNER JOIN customers ON products1.id = customers.id 
         WHERE (id = 324) AND access = 'restricted' ''', config)
        assert res["allowed"] == True, res

    def test_full_outer_join(self, config):
        res = verify_sql('''SELECT id, prod_name from products1
        FULL OUTER JOIN customers on products1.id = customers.id
        where (id = 324) AND access = 'restricted' ''', config)
        assert res["allowed"] == True, res

    def test_right_join(self,config):
        res = verify_sql('''SELECT id, prod_name FROM products1
         RIGHT JOIN customers ON products1.id = customers.id 
         WHERE ((id = 324)) AND access = 'restricted' ''', config)
        assert res["allowed"] == True, res

    def test_left_join(self,config):
        res = verify_sql('''SELECT id, prod_name FROM products1
                 LEFT JOIN customers ON products1.id = customers.id 
                 WHERE ((id = 324)) AND access = 'restricted' ''', config)
        assert res["allowed"] == True, res

    def test_union(self,config):
        res = verify_sql('''select id from products1
        union select id from customers''', config)
        assert not res["allowed"]
        assert 'Column id is not allowed. Column removed from SELECT clause' in res['errors']

    def test_inner_join_fail(self,config):
        res = verify_sql('''SELECT id, prod_name FROM products1
         INNER JOIN customers ON products1.id = customers.id 
         WHERE (id = 324) AND access = 'granted' ''', config)
        assert not res["allowed"]
        assert "Missing restriction for table: customers column: access value: restricted" in res["errors"]

    def test_full_outer_join_fail(self, config):
        res = verify_sql('''SELECT id, prod_name from products1
        FULL OUTER JOIN customers on products1.id = customers.id
        where (id = 324) AND access = 'pending' ''', config)
        assert not res["allowed"]
        assert "Missing restriction for table: customers column: access value: restricted" in res["errors"]

    def test_right_join_fail(self,config):
        res = verify_sql('''SELECT id, prod_name FROM products1
         RIGHT JOIN customers ON products1.id = customers.id 
         WHERE ((id = 324)) AND access = 'granted' ''', config)
        assert not res["allowed"]
        assert "Missing restriction for table: customers column: access value: restricted" in res["errors"]

    def test_left_join_fail(self,config):
        res = verify_sql('''SELECT id, prod_name FROM products1
                 LEFT JOIN customers ON products1.id = customers.id 
                 WHERE ((id = 324)) AND access = 'granted' ''', config)
        assert not res["allowed"]
        assert "Missing restriction for table: customers column: access value: restricted" in res["errors"]
