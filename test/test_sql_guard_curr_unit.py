import json
import os
import sqlite3
from sqlite3 import Connection
from typing import Set, Generator

import pytest
from sql_data_guard import verify_sql
from conftest import verify_sql_test, verify_sql_test_data


class TestSQLJoins:

    @pytest.fixture(scope="class")
    def config(self) -> dict:
        """Provide the configuration for SQL validation"""
        return {
            "tables": [
                {
                    "table_name": "products",
                    "database_name": "orders_db",
                    "columns": ["prod_id", "prod_name", "category", "price"],
                    "restrictions": [
                        {"column": "price", "value": 100, "operation": ">="}  # Restriction on price column
                    ]
                },
                {
                    "table_name": "orders",
                    "database_name": "orders_db",
                    "columns": ["order_id", "prod_id"],
                    "restrictions": []
                }
            ]
        }

    @pytest.fixture(scope="class")
    def cnn(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS orders_db")
            conn.execute("""
                   CREATE TABLE orders_db.products (
                       prod_id INT, 
                       prod_name TEXT, 
                       category TEXT, 
                       price REAL
                   )""")
            conn.execute("""
                   CREATE TABLE orders_db.orders (
                       order_id INT,
                       prod_id INT
                   )""")

            conn.execute("INSERT INTO orders_db.products VALUES (1, 'Product1', 'CategoryA', 120)")
            conn.execute("INSERT INTO orders_db.products VALUES (2, 'Product2', 'CategoryB', 80)")
            conn.execute("INSERT INTO orders_db.orders VALUES (1, 1)")
            conn.execute("INSERT INTO orders_db.orders VALUES (2, 2)")
            yield conn


    def test_inner_join_using(self, config):
        verify_sql_test(
            "SELECT prod_id, prod_name, order_id "
            "FROM products INNER JOIN orders USING (prod_id) WHERE price = 100",
            config,
        )
    def test_inner_join_with_restriction(self,config):
        verify_sql_test(
            "SELECT prod_name "
            "FROM products INNER JOIN orders USING (prod_id) WHERE price > 100 AND price = 100",
            config,
        )

    def test_inner_join_with_price_restriction(self, config):
        sql_query = """
            SELECT prod_name
            FROM products
            INNER JOIN orders ON products.prod_id = orders.prod_id
            WHERE price > 100 AND price = 100
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is True, res
        assert res["errors"] == set(), res  # Check that errors is an empty set

    def test_left_join_with_price_restriction(self, config):
        sql_query = """
            SELECT prod_name
            FROM products
            LEFT JOIN orders ON products.prod_id = orders.prod_id
            WHERE price >= 100 AND price = 100
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is True, res
        assert res["errors"] == set(), res  # Check that errors is an empty set

    def test_right_join_with_price_less_than_100(self, config):
        sql_query = """
            SELECT prod_name
            FROM products
            RIGHT JOIN orders ON products.prod_id = orders.prod_id
            WHERE price < 100
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res
        # Adjust the expected error message to reflect the restriction on price = 100, not price >= 100
        assert "Missing restriction for table: products column: price value: 100" in res["errors"], res

    def test_left_join_with_price_greater_than_or_equal_50(self, config):
        sql_query = """
            SELECT prod_name
            FROM products
            LEFT JOIN orders ON products.prod_id = orders.prod_id
            WHERE price >= 50
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res

    def test_inner_join_no_match(self, config):
        sql_query = """
               SELECT prod_name
               FROM products
               INNER JOIN orders ON products.prod_id = orders.prod_id
               WHERE price < 100
           """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res
        assert "Missing restriction for table: products column: price value: 100" in res["errors"], res

    def test_full_outer_join_with_no_matching_rows(self, config):
        sql_query = """
               SELECT prod_name
               FROM products
               FULL OUTER JOIN orders ON products.prod_id = orders.prod_id
               WHERE price >= 100 AND price = 100
           """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is True, res
        assert res["errors"] == set(), res

    def test_left_join_no_match(self, config):
        sql_query = """
               SELECT prod_name
               FROM products
               LEFT JOIN orders ON products.prod_id = orders.prod_id
               WHERE price < 100
           """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res
        assert "Missing restriction for table: products column: price value: 100" in res["errors"], res

    def test_inner_join_on_specific_prod_id(self, config):
        sql_query = """
               SELECT prod_name
               FROM products
               INNER JOIN orders ON products.prod_id = orders.prod_id
               WHERE products.prod_id = 1 AND price = 100
           """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is True, res
        assert res["errors"] == set(), res

    def test_inner_join_with_multiple_conditions(self, config):
        sql_query = """
               SELECT prod_name
               FROM products
               INNER JOIN orders ON products.prod_id = orders.prod_id
               WHERE price >= 100 AND price = 100
           """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is True, res
        assert res["errors"] == set(), res

    def test_union_with_invalid_column(self, config):
        sql_query = """
               SELECT prod_name FROM products
               UNION
               SELECT order_id FROM orders
           """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res


    def test_right_join_with_no_matching_prod_id(self, config):
        sql_query = """
               SELECT prod_name
               FROM products
               RIGHT JOIN orders ON products.prod_id = orders.prod_id
               WHERE products.prod_id = 999 AND price = 100
           """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is True, res
        assert res["errors"] == set(), res


class TestSQLJsonArrayQueries:

    # Fixture to provide the configuration for SQL validation with updated restrictions
    @pytest.fixture(scope="class")
    def config(self) -> dict:
        """Provide the configuration for SQL validation with restriction on prod_category"""
        return {
            "tables": [
                {
                    "table_name": "products",
                    "database_name": "orders_db",
                    "columns": ["prod_id", "prod_name", "prod_category", "price", "attributes"],
                    "restrictions": [
                        {"column": "prod_category", "value": "CategoryB", "operation": "!="}
                        # Restriction on prod_category: not equal to "CategoryB"
                    ]
                },
                {
                    "table_name": "orders",
                    "database_name": "orders_db",
                    "columns": ["order_id", "prod_id"],
                    "restrictions": []  # No restrictions for the 'orders' table
                }
            ]
        }
        # Additional Fixture for JSON and Array tests
    @pytest.fixture(scope="class")
    def cnn_with_json_and_array(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS orders_db")

            # Creating 'products' table with JSON and array-like column
            conn.execute("""
                CREATE TABLE orders_db.products (
                    prod_id INT,
                    prod_name TEXT,
                    prod_category TEXT,
                    price REAL,
                    attributes JSON
                )""")

            # Creating a second table 'orders'
            conn.execute("""
                CREATE TABLE orders_db.orders (
                    order_id INT,
                    prod_id INT
                )""")

            # Insert sample data with JSON column
            conn.execute("""
                INSERT INTO orders_db.products (prod_id, prod_name, prod_category, price, attributes)
                VALUES (1, 'Product1', 'CategoryA', 120, '{"colors": ["red", "blue"], "size": "M"}')
            """)
            conn.execute("""
                INSERT INTO orders_db.products (prod_id, prod_name, prod_category, price, attributes)
                VALUES (2, 'Product2', 'CategoryB', 80, '{"colors": ["green"], "size": "S"}')
            """)
            conn.execute("""
                INSERT INTO orders_db.orders (order_id, prod_id) 
                VALUES (1, 1), (2, 2)
            """)

            yield conn


    # Test Array-like column using JSON with the updated restriction on prod_category
    def test_array_column_query_with_json(self, cnn_with_json_and_array, config):
        sql_query = """
            SELECT prod_id, prod_name, json_extract(attributes, '$.colors[0]') AS first_color
            FROM products
            WHERE prod_category != 'CategoryB'
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res

    # Test querying JSON field with the updated restriction on prod_category
    def test_json_field_query(self, cnn_with_json_and_array, config):
        sql_query = """
            SELECT prod_name, json_extract(attributes, '$.size') AS size
            FROM products
            WHERE json_extract(attributes, '$.size') = 'M' AND prod_category != 'CategoryB'
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res

    # Test for additional restrictions in config
    def test_restrictions_query(self, cnn_with_json_and_array, config):
        sql_query = """
            SELECT prod_id, prod_name
            FROM products
            WHERE prod_category != 'CategoryB'
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res

    # Test Array-like column using JSON and filtering based on the array's first element
    def test_json_array_column_with_filter(self, cnn_with_json_and_array, config):
        sql_query = """
            SELECT prod_id, prod_name, json_extract(attributes, '$.colors[0]') AS first_color
            FROM products
            WHERE json_extract(attributes, '$.colors[0]') = 'red' AND prod_category != 'CategoryB'
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res


    # Test Array-like column with CROSS JOIN UNNEST (for SQLite support of arrays)
    def test_array_column_unnest(self, cnn_with_json_and_array, config):
        sql_query = """
            SELECT prod_id, prod_name, color
            FROM products, json_each(attributes, '$.colors') AS color
            WHERE prod_category != 'CategoryB'
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res


    # Test Table Alias and JSON Querying (Self-Join with aliases and JSON extraction)
    def test_self_join_with_alias_and_json(self, cnn_with_json_and_array, config):
        sql_query = """
            SELECT p1.prod_name, p2.prod_name AS related_prod, json_extract(p1.attributes, '$.size') AS p1_size
            FROM products p1
            INNER JOIN products p2 ON p1.prod_id != p2.prod_id
            WHERE p1.prod_category != 'CategoryB' AND json_extract(p1.attributes, '$.size') = 'M'
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res

    # Test JSON Nested Query with Array Filtering
    def test_json_array_filtering(self, cnn_with_json_and_array, config):
        sql_query = """
            SELECT prod_id, prod_name
            FROM products
            WHERE json_extract(attributes, '$.colors[0]') = 'red' AND prod_category != 'CategoryB'
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res



