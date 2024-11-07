from sql_data_guard import verify_sql

class TestSQLErrors:
    def test_basic_sql_error(self):
        result = verify_sql("this is not an sql statement ",{})
        assert result["allowed"] == False
        assert "Found unparsable section" in result["errors"][0]


class TestSingleTable:

    @staticmethod
    def verify(sql: str):
        return verify_sql(sql,
                        {
                            "tables": [
                                {
                                    "table_name": "orders",
                                    "database_name": "orders_db",
                                    "columns": ["id", "product_name", "account_id"],
                                    "restrictions": [{"column": "id", "value": 123}]
                                }
                            ]
                        })

    def test_select_illegal_table(self):
        result = self.verify("SELECT * FROM users")
        assert result == {'allowed': False,
 'errors': ['Table users is not allowed'],
 'fixed': None}

    def test_select_two_illegal_tables(self):
        result = self.verify("SELECT col1 FROM users AS u1, products AS p1")
        assert result == {'allowed': False,
                          'errors': ['Table users is not allowed', 'Table products is not allowed'],
                          'fixed': None}

    def test_select_star(self):
        result = self.verify("SELECT * FROM orders WHERE id = 123")
        assert result == {'allowed': False,'errors': ['SELECT * is not allowed'],
                          "fixed": "SELECT id, product_name, account_id FROM orders WHERE id = 123"}

    def test_two_cols(self):
        result = self.verify("SELECT id, product_name FROM orders WHERE id = 123")
        assert result == {'allowed': True,'errors': [],
                          "fixed": None}


    def test_quote_and_alias(self):
        result = self.verify('SELECT "id" AS my_id, 1 FROM "orders" AS my_orders WHERE id = 123')
        assert result == {'allowed': True,'errors': [],
                          "fixed": None}

    def test_sql_with_group_by_and_order_by(self):
        result = self.verify("SELECT id FROM orders GROUP BY id ORDER BY id")
        assert result == {'allowed': False,'errors': ['Missing restriction for table: orders column: id value: 123'],
                          "fixed": "SELECT id FROM orders WHERE id = 123 GROUP BY id ORDER BY id"}

    def test_sql_with_where_and_group_by_and_order_by(self):
        result = self.verify("SELECT id FROM orders WHERE product_name='' GROUP BY id ORDER BY id")
        assert result == {'allowed': False,'errors': ['Missing restriction for table: orders column: id value: 123'],
                          "fixed": "SELECT id FROM orders WHERE ( product_name='') AND id = 123 GROUP BY id ORDER BY id"}

    def test_col_expression(self):
        result = self.verify("SELECT col + 1 FROM orders WHERE id = 123")
        assert result == {'allowed': False,
                          'errors': ['Column col is not allowed. Column removed from SELECT clause',
                                     'No legal elements in SELECT clause'], "fixed": None}

    def test_select_illegal_col(self):
        result = self.verify("SELECT col, id FROM orders WHERE id = 123")
        assert result == {'allowed': False,'errors': ['Column col is not allowed. Column removed from SELECT clause'],
                          "fixed": "SELECT id FROM orders WHERE id = 123"}

    def test_select_no_legal_cols(self):
        result = self.verify("SELECT col1, col2 FROM orders WHERE id = 123")
        assert result == {'allowed': False,'errors': ['Column col1 is not allowed. Column removed from SELECT clause',
                                                      'Column col2 is not allowed. Column removed from SELECT clause',
                                                      'No legal elements in SELECT clause'],
                          "fixed": None}

    def test_missing_restriction(self):
        result = self.verify("SELECT id FROM orders")
        assert result == {'allowed': False,'errors': ['Missing restriction for table: orders column: id value: 123'],
                          "fixed": "SELECT id FROM orders WHERE id = 123"}

    def test_wrong_restriction(self):
        result = self.verify("SELECT id FROM orders WHERE id = 234")
        assert result == {'allowed': False,
                          'errors': ["Missing restriction for table: orders column: id value: 123"],
                          "fixed": "SELECT id FROM orders WHERE ( id = 234) AND id = 123"}

    def test_table_and_database(self):
        result = self.verify("SELECT id FROM orders_db.orders AS o WHERE id = 123")
        assert result == {'allowed': True,
                          'errors': [],
                          "fixed": None}

    def test_function_call(self):
        result = self.verify("SELECT COUNT(DISTINCT id) FROM orders_db.orders AS o WHERE id = 123")
        assert result == {'allowed': True,
                          'errors': [],
                          "fixed": None}

    def test_function_call_illegal_col(self):
        result = self.verify("SELECT COUNT(DISTINCT col) FROM orders_db.orders AS o WHERE id = 123")
        assert result == {'allowed': False,
                          'errors': ['Column col is not allowed. Column removed from SELECT clause',
                                     'No legal elements in SELECT clause'],
                          "fixed": None}

    def test_table_prefix(self):
        result = self.verify("SELECT orders.id FROM orders AS o WHERE id = 123")
        assert result == {'allowed': True,
                          'errors': [],
                          "fixed": None}

    def test_table_and_db_prefix(self):
        result = self.verify("SELECT orders_db.orders.id FROM orders_db.orders WHERE orders_db.orders.id = 123")
        assert result == {'allowed': True,
                          'errors': [],
                          "fixed": None}

    def test_table_alias(self):
        result = self.verify("SELECT a.id FROM orders_db.orders AS a WHERE a.id = 123")
        assert result == {'allowed': True,
                          'errors': [],
                          "fixed": None}


    def test_bad_restriction(self):
        result = self.verify("SELECT id FROM orders WHERE id = 123 OR id = 234")
        assert result == {'allowed': False,
                          'errors': ['Missing restriction for table: orders column: id value: 123'],
                          "fixed": "SELECT id FROM orders WHERE ( id = 123 OR id = 234) AND id = 123"}


    def test_sql_injection(self):
        result = self.verify("SELECT id FROM orders WHERE id = 123 OR 1 = 1")
        assert result == {'allowed': False,
                          'errors': ['Static expression is not allowed'],
                          "fixed": None}



class TestJoinTable:

    @staticmethod
    def verify(sql: str):
        return verify_sql(sql,
                        {
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
                        })

    def test_using(self):
        result = self.verify("SELECT order_id, account_id, product_name FROM orders INNER JOIN products USING (product_id) WHERE account_id = 123")
        assert result == {'allowed': True,
 'errors': [],
 'fixed': None}
