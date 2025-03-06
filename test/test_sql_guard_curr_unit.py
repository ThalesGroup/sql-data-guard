import json
import os
import sqlite3
from sqlite3 import Connection
from typing import Set, Generator

import pytest
from sql_data_guard import verify_sql
from test_sql_guard_unit import _test_sql



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



class TestAdditionalSqlCases:

    @pytest.fixture(scope="class")
    def cnn(self):
        with sqlite3.connect(":memory:") as conn:
            # Creating 'orders' table in the in-memory database
            conn.execute("""
            CREATE TABLE orders (
                id INT,
                product_name TEXT,
                account_id TEXT,
                status TEXT,
                not_allowed TEXT
            )""")
            # Inserting values into the 'orders' table
            conn.execute("INSERT INTO orders VALUES (123, 'product_1', 'acc_1', 'shipped', 'no')")
            conn.execute("INSERT INTO orders VALUES (124, 'product_2', 'acc_2', 'delivered', 'yes')")
            conn.execute("INSERT INTO orders VALUES (125, 'product_3', 'acc_3', 'pending', 'no')")
            conn.execute("INSERT INTO orders VALUES (126, 'product_4', 'acc_4', 'shipped', 'no')")
            yield conn

    @pytest.fixture(scope="class")
    def config(self) -> dict:
        """Provide the configuration for SQL validation"""
        return {
            "tables": [
                {
                    "table_name": "orders",
                    "database_name": "orders_db",
                    "columns": ["id", "product_name", "account_id", "status", "not_allowed"],
                    "restrictions": [{"column": "id", "value": 123}]
                }
            ]
        }

    def test_invalid_sql_syntax(self, config):
        """Test for invalid SQL syntax"""
        result = verify_sql("SELECT * FORM orders", config)  # Invalid SQL, typo in 'FROM'
        assert result["allowed"] == False, result

    def test_invalid_query(self, config):
        """Test for invalid SQL query like DROP"""
        result = verify_sql("DROP TABLE users;", config)  # Invalid query, drop statement not allowed
        assert result["allowed"] == False, result

    def test_select_with_invalid_column(self, config):
        """Test for selecting an invalid column"""
        result = verify_sql("SELECT id, invalid_column FROM orders", config)
        assert result["allowed"] == False
        # Ensure that "invalid_column" is mentioned in the errors, regardless of other errors
        error_messages = result["errors"]
        assert any("invalid_column" in error for error in error_messages), f"Unexpected errors: {error_messages}"

    def test_missing_column_in_select(self, config):
        """Test for selecting a non-existing column"""
        result = verify_sql("SELECT non_existing_column FROM orders", config)
        assert result["allowed"] == False  # Expecting this to be disallowed
        assert "Column non_existing_column is not allowed. Column removed from SELECT clause" in result["errors"]

    def test_select_with_multiple_restrictions(self, config):
        """Test for selecting with multiple restrictions"""
        result = verify_sql("SELECT id FROM orders WHERE id = 123", config)
        assert result["allowed"] == True
        assert len(result["errors"]) == 0

    def test_select_with_invalid_table(self, config):
        """Test for selecting from a table that doesn't exist"""
        result = verify_sql("SELECT id FROM unknown_table", config)
        assert result["allowed"] == False
        assert "Table unknown_table is not allowed" in result["errors"]

    def test_select_with_no_where_clause(self, config):
        """Test for selecting data without applying any restrictions"""
        result = verify_sql("SELECT * FROM orders", config)
        assert result["allowed"] == False
        assert "Missing restriction for table: orders column: id value: 123" in result["errors"]

    def test_select_with_correct_column_but_wrong_value(self, config):
        """Test for selecting a column with a restriction, but using an incorrect value"""
        result = verify_sql("SELECT id FROM orders WHERE id = 999", config)
        assert result["allowed"] == False
        assert "Missing restriction for table: orders column: id value: 123" in result["errors"]

    def test_select_with_valid_column_and_value(self, config):
        """Test for selecting data with correct column and value (should be allowed)"""
        result = verify_sql("SELECT id FROM orders WHERE id = 123", config)
        assert result["allowed"] == True
        assert len(result["errors"]) == 0

    def test_select_with_incorrect_syntax_in_where_clause(self, config):
        """Test for SQL query with incorrect syntax in WHERE clause"""
        result = verify_sql("SELECT * FROM orders WHERE id == 123", config)  # Intentional syntax error in WHERE clause
        assert result["allowed"] == False
        assert "SELECT * is not allowed" in result["errors"]
