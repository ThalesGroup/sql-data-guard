import json
import os
import sqlite3
from sqlite3 import Connection
from typing import Set, Generator

import pytest
from sql_data_guard import verify_sql
from conftest import verify_sql_test


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
                        {
                            "column": "price",
                            "value": 100,
                            "operation": ">=",
                        }  # Restriction on price column
                    ],
                },
                {
                    "table_name": "orders",
                    "database_name": "orders_db",
                    "columns": ["order_id", "prod_id"],
                    "restrictions": [],
                },
            ]
        }

    @pytest.fixture(scope="class")
    def cnn(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS orders_db")
            conn.execute(
                """
                   CREATE TABLE orders_db.products (
                       prod_id INT, 
                       prod_name TEXT, 
                       category TEXT, 
                       price REAL
                   )"""
            )
            conn.execute(
                """
                   CREATE TABLE orders_db.orders (
                       order_id INT,
                       prod_id INT
                   )"""
            )

            conn.execute(
                "INSERT INTO orders_db.products VALUES (1, 'Product1', 'CategoryA', 120)"
            )
            conn.execute(
                "INSERT INTO orders_db.products VALUES (2, 'Product2', 'CategoryB', 100)"
            )

            conn.execute(
                "INSERT INTO orders_db.products VALUES (3, 'Product3', 'CategoryC', 80)"
            )
            conn.execute(
                "INSERT INTO orders_db.products VALUES (4, 'Product4', 'CategoryD', 100)"
            )
            conn.execute(
                "INSERT INTO orders_db.products VALUES (5, 'Product5', 'CategoryE', 150)"
            )
            conn.execute(
                "INSERT INTO orders_db.products VALUES (6, 'Product6', 'CategoryF', 200)"
            )
            conn.execute("INSERT INTO orders_db.orders VALUES (1, 1)")
            conn.execute("INSERT INTO orders_db.orders VALUES (2, 2)")
            conn.execute("INSERT INTO orders_db.orders VALUES (3, 3)")
            conn.execute("INSERT INTO orders_db.orders VALUES (4, 4)")
            conn.execute("INSERT INTO orders_db.orders VALUES (5, 5)")
            conn.execute("INSERT INTO orders_db.orders VALUES (6, 6)")

            yield conn

    def test_select_product_with_price_120(self, config, cnn):
        """Test case for selecting product with price 120"""
        verify_sql_test(
            """
            SELECT prod_id FROM products WHERE price = 120 AND price = 100
            """,
            config,
            cnn=cnn,
            data=[],
        )

    def test_inner_join_using(self, config, cnn):
        verify_sql_test(
            "SELECT prod_id, prod_name, order_id "
            "FROM products INNER JOIN orders USING (prod_id) WHERE price = 100",
            config,
            cnn=cnn,
            data=[(2, "Product2", 2), (4, "Product4", 4)],
        )

    def test_inner_join_with_restriction(self, config, cnn):
        """Test case for inner join with price restrictions"""
        sql_query = """
            SELECT prod_name
            FROM products
            INNER JOIN orders ON products.prod_id = orders.prod_id
            WHERE price = 100
        """
        verify_sql_test(
            sql_query,
            config,
            cnn=cnn,
            data=[
                ["Product2"],
                ["Product4"],
            ],
        )

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
        assert (
            "Missing restriction for table: products column: price value: 100"
            in res["errors"]
        ), res

    def test_left_join_with_price_greater_than_50(self, config):
        sql_query = """
            SELECT prod_name
            FROM products
            LEFT JOIN orders ON products.prod_id = orders.prod_id
            WHERE price > 50
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
        assert (
            "Missing restriction for table: products column: price value: 100"
            in res["errors"]
        ), res

    def test_full_outer_join_with_no_matching_rows(self, config, cnn):
        sql_query = """
            SELECT prod_name
            FROM products
            FULL OUTER JOIN orders ON products.prod_id = orders.prod_id
            WHERE price = 100
        """
        verify_sql_test(
            sql_query,
            config,
            cnn=cnn,
            data=[
                {
                    "Product2",  # Product2 has price = 100
                },
                {
                    "Product4",  # Product4 has price = 100
                },
            ],
        )

    def test_left_join_no_match(self, config):
        sql_query = """
               SELECT prod_name
               FROM products
               LEFT JOIN orders ON products.prod_id = orders.prod_id
               WHERE price < 100
           """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res
        assert (
            "Missing restriction for table: products column: price value: 100"
            in res["errors"]
        ), res

    def test_inner_join_on_specific_prod_id(self, config, cnn):
        sql_query = """
            SELECT prod_name
            FROM products
            INNER JOIN orders ON products.prod_id = orders.prod_id
            WHERE products.prod_id = 1 AND price = 100
        """
        verify_sql_test(
            sql_query,
            config,
            cnn=cnn,
            data=[],
        )

    def test_inner_join_with_multiple_conditions(self, config):
        sql_query = """
               SELECT prod_name
               FROM products
               INNER JOIN orders ON products.prod_id = orders.prod_id
               WHERE price > 100 AND price = 100
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
                    "columns": [
                        "prod_id",
                        "prod_name",
                        "prod_category",
                        "price",
                        "attributes",
                    ],
                    "restrictions": [
                        {
                            "column": "prod_category",
                            "value": "CategoryB",
                            "operation": "!=",
                        }
                        # Restriction on prod_category: not equal to "CategoryB"
                    ],
                },
                {
                    "table_name": "orders",
                    "database_name": "orders_db",
                    "columns": ["order_id", "prod_id"],
                    "restrictions": [],  # No restrictions for the 'orders' table
                },
            ]
        }
        # Additional Fixture for JSON and Array tests

    @pytest.fixture(scope="class")
    def cnn_with_json_and_array(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS orders_db")

            # Creating 'products' table with JSON and array-like column
            conn.execute(
                """
                CREATE TABLE orders_db.products (
                    prod_id INT,
                    prod_name TEXT,
                    prod_category TEXT,
                    price REAL,
                    attributes JSON
                )"""
            )

            # Creating a second table 'orders'
            conn.execute(
                """
                CREATE TABLE orders_db.orders (
                    order_id INT,
                    prod_id INT
                )"""
            )

            # Insert sample data with JSON column
            conn.execute(
                """
                INSERT INTO orders_db.products (prod_id, prod_name, prod_category, price, attributes)
                VALUES (1, 'Product1', 'CategoryA', 120, '{"colors": ["red", "blue"], "size": "M"}')
            """
            )
            conn.execute(
                """
                INSERT INTO orders_db.products (prod_id, prod_name, prod_category, price, attributes)
                VALUES (2, 'Product2', 'CategoryB', 80, '{"colors": ["green"], "size": "S"}')
            """
            )
            conn.execute(
                """
                INSERT INTO orders_db.orders (order_id, prod_id) 
                VALUES (1, 1), (2, 2)
            """
            )

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
    def test_json_nested_array_filtering(self, cnn_with_json_and_array, config):
        sql_query = """
            SELECT prod_id, prod_name
            FROM products
            WHERE json_extract(attributes, '$.colors[0]') = 'red' AND prod_category != 'CategoryB'
        """
        res = verify_sql(sql_query, config)
        assert res["allowed"] is False, res

    def test_query_json_array_filter(self, cnn_with_json_and_array, config):
        query = """
        SELECT prod_id, prod_name, prod_category, price, attributes
        FROM orders_db.products
        WHERE JSON_EXTRACT(attributes, '$.colors[0]') = 'red'
        """
        # result = verify_sql(query, config)
        # assert result["allowed"] is False, result

        result = cnn_with_json_and_array.execute(query).fetchall()
        assert len(result) == 1  # Only Product1 should match the color "red"
        assert result[0][1] == "Product1"  # Ensure it's the correct product

    def test_query_json_array_non_matching(self, cnn_with_json_and_array, config):
        query = """
        SELECT prod_id, prod_name, prod_category, price, attributes
        FROM orders_db.products
        WHERE JSON_EXTRACT(attributes, '$.colors[0]') = 'yellow'
        """
        # result = verify_sql(query, config)
        # assert result["allowed"] is False, result

        result = cnn_with_json_and_array.execute(query).fetchall()
        assert len(result) == 0  # No product should match the color "yellow"

    def test_query_json_array_multiple_colors(self, cnn_with_json_and_array, config):
        query = """
        SELECT prod_id, prod_name, prod_category, price, attributes
        FROM orders_db.products
        WHERE JSON_ARRAY_LENGTH(JSON_EXTRACT(attributes, '$.colors')) > 1
        """
        # result = verify_sql(query, config)
        # assert result["allowed"] is False, result

        result = cnn_with_json_and_array.execute(query).fetchall()
        assert (
            len(result) == 1
        )  # Only Product1 should match (has two colors: "red" and "blue")
        assert result[0][1] == "Product1"


# Test class that contains all the SQL cases for various SQL scenarios
class TestSQLOrderDateBetweenRestrictions:

    # Fixture to provide the configuration for SQL validation with updated restrictions
    @pytest.fixture(scope="class")
    def config(self) -> dict:
        """Provide the configuration for SQL validation with a price range using BETWEEN."""
        return {
            "tables": [
                {
                    "table_name": "products",
                    "database_name": "orders_db",
                    "columns": [
                        "prod_id",
                        "prod_name",
                        "prod_category",
                        "price",
                    ],
                    "restrictions": [
                        {
                            "column": "price",
                            "values": [80, 150],
                            "operation": "BETWEEN",
                        },
                    ],
                },
                {
                    "table_name": "orders",
                    "database_name": "orders_db",
                    "columns": ["order_id", "prod_id", "quantity", "order_date"],
                    "restrictions": [],  # No restrictions for the 'orders' table
                },
            ]
        }

    # Fixture for setting up an in-memory SQLite database with required tables and sample data
    @pytest.fixture(scope="class")
    def cnn(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS orders_db")

            # Creating 'products' table with price and stock columns
            conn.execute(
                """
                CREATE TABLE orders_db.products (
                    prod_id INT,
                    prod_name TEXT,
                    prod_category TEXT,
                    price REAL
                )
            """
            )

            # Creating 'orders' table
            conn.execute(
                """
                CREATE TABLE orders_db.orders (
                    order_id INT,
                    prod_id INT,
                    quantity INT,
                    order_date DATE
                )
            """
            )

            # Inserting sample data into the 'products' table
            conn.execute(
                """
                INSERT INTO orders_db.products (prod_id, prod_name, prod_category, price) 
                VALUES 
                    (1, 'Product A', 'CategoryA', 120),
                    (2, 'Product B', 'CategoryB', 80),
                    (3, 'Product C', 'CategoryA', 150),
                    (4, 'Product D', 'CategoryB', 60)
            """
            )

            # Inserting sample data into the 'orders' table
            conn.execute(
                """
                INSERT INTO orders_db.orders (order_id, prod_id, quantity, order_date) 
                VALUES 
                    (1, 1, 10, '03-01-2025'),
                    (2, 2, 5, '02-02-2025'),
                    (3, 3, 7, '03-03-2025'),
                    (4, 4, 12, '16-01-2025')
            """
            )

            yield conn

    def test_price_between_valid(self, cnn, config):
        verify_sql_test(
            "SELECT prod_id, prod_name, price FROM products WHERE price BETWEEN 80 AND 150",
            config,
            cnn=cnn,
            errors={
                "Missing restriction for table: products column: price value: [80, 150]"
            },
            fix="SELECT prod_id, prod_name, price FROM products WHERE (price BETWEEN 80 AND "
            "150) AND price BETWEEN 80 AND 150",
            data=[
                (1, "Product A", 120.0),
                (2, "Product B", 80.0),
                (3, "Product C", 150.0),
            ],
        )

    def test_price_not_between_restriction(self, cnn, config):
        sql_query = """
            SELECT prod_id, prod_name, price
            FROM products
            WHERE price NOT BETWEEN 80 AND 150
        """
        verify_sql_test(
            sql_query,
            config,
            errors={
                "Missing restriction for table: products column: price value: [80, 150]"
            },
            fix="SELECT prod_id, prod_name, price FROM products WHERE (NOT price BETWEEN 80 AND 150) AND price BETWEEN 80 AND 150",
            cnn=cnn,
            data=[],
        )

    def test_count_products_within_price_range(self, cnn, config):
        verify_sql_test(
            "SELECT COUNT(*) FROM products WHERE price BETWEEN 80 AND 150",
            config,
            errors={
                "Missing restriction for table: products column: price value: [80, 150]"
            },
            fix="SELECT COUNT(*) FROM products WHERE (price BETWEEN 80 AND 150) AND price BETWEEN 80 AND 150",
            cnn=cnn,
            data=[(3,)],  # Expecting 3 products in this range
        )

    def test_left_join_products_with_orders(self, cnn, config):
        verify_sql_test(
            """SELECT p.prod_name, o.order_id, COALESCE(o.quantity, 0) AS quantity 
            FROM products p 
            LEFT JOIN orders o ON p.prod_id = o.prod_id 
            WHERE p.price BETWEEN 80 AND 150""",
            config,
            errors={
                "Missing restriction for table: products column: p.price value: [80, 150]"
            },
            fix="SELECT p.prod_name, o.order_id, COALESCE(o.quantity, 0) AS quantity FROM "
            "products AS p LEFT JOIN orders AS o ON p.prod_id = o.prod_id WHERE (p.price "
            "BETWEEN 80 AND 150) AND p.price BETWEEN 80 AND 150",
            cnn=cnn,
            data=[("Product A", 1, 10), ("Product B", 2, 5), ("Product C", 3, 7)],
        )

    def test_select_products_below_price_restriction(self, cnn, config):
        verify_sql_test(
            "SELECT prod_name, price FROM products WHERE price < 90",
            config,
            errors={
                "Missing restriction for table: products column: price value: [80, 150]"
            },
            fix="SELECT prod_name, price FROM products WHERE (price < 90) AND price BETWEEN 80 AND 150",
            cnn=cnn,
            data=[("Product B", 80)],
        )

    def test_price_between_and_category_restriction(self, cnn, config):
        verify_sql_test(
            "SELECT prod_id, prod_name, price, prod_category "
            "FROM products "
            "WHERE price BETWEEN 80 AND 150 AND prod_category = 'CategoryA'",
            config,
            errors={
                "Missing restriction for table: products column: price value: [80, 150]"
            },
            fix="SELECT prod_id, prod_name, price, prod_category FROM products WHERE (price BETWEEN 80 AND 150 AND prod_category = 'CategoryA') AND price BETWEEN 80 AND 150",
            cnn=cnn,
            data=[
                (1, "Product A", 120, "CategoryA"),
                (3, "Product C", 150, "CategoryA"),
            ],
        )

    def test_group_by_with_price_between(self, cnn, config):
        verify_sql_test(
            "SELECT COUNT(prod_id) AS product_count, prod_category "
            "FROM products "
            "WHERE price BETWEEN 90 AND 125 "
            "GROUP BY prod_category",
            config,
            errors={
                "Missing restriction for table: products column: price value: [80, 150]"
            },
            fix="SELECT COUNT(prod_id) AS product_count, prod_category FROM products WHERE (price BETWEEN 90 AND 125) AND price BETWEEN 80 AND 150 GROUP BY prod_category",
            cnn=cnn,
            data=[(1, "CategoryA")],  # Only Product A fits in this range
        )

    def test_join_with_price_between(self, cnn, config):
        verify_sql_test(
            "SELECT o.order_id, p.prod_name, p.price "
            "FROM orders o "
            "INNER JOIN products p ON o.prod_id = p.prod_id "
            "WHERE p.price BETWEEN 90 AND 150",
            config,
            errors={
                "Missing restriction for table: products column: p.price value: [80, 150]"
            },
            fix="SELECT o.order_id, p.prod_name, p.price FROM orders AS o INNER JOIN products AS p "
            "ON o.prod_id = p.prod_id WHERE (p.price BETWEEN 90 AND 150) AND p.price BETWEEN 80 AND 150",
            cnn=cnn,
            data=[
                (1, "Product A", 120),
                (3, "Product C", 150),
            ],
        )

    def test_non_existent_product_between(self, cnn, config):
        verify_sql_test(
            "SELECT prod_id, prod_name, price "
            "FROM products "
            "WHERE price BETWEEN 200 AND 300",
            config,
            errors={
                "Missing restriction for table: products column: price value: [80, 150]"
            },
            fix="SELECT prod_id, prod_name, price FROM products WHERE (price BETWEEN 200 AND 300) AND price BETWEEN 80 AND 150",
            cnn=cnn,
            data=[],  # No products in this range
        )

    def test_group_by_having_price_avg(self, cnn, config):
        verify_sql_test(
            "SELECT prod_category, AVG(price) AS avg_price "
            "FROM products "
            "GROUP BY prod_category "
            "HAVING AVG(price) > 100",
            config,
            errors={
                "Missing restriction for table: products column: price value: [80, 150]"
            },
            fix="SELECT prod_category, AVG(price) AS avg_price FROM products WHERE price BETWEEN 80 AND 150 GROUP BY prod_category HAVING AVG(price) > 100",
            cnn=cnn,
            data=[("CategoryA", 135)],  # Avg of 120 and 150 in CategoryA
        )
