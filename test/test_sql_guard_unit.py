from sql_guard import verify_sql

class TestSQLErrors:
    def test_basic_sql_error(self):
        result = verify_sql("this is not an sql statement ",{}, False)
        assert result["allowed"] == False
        assert "Found unparsable section" in result["errors"][0]


class TestSingleTable:

    @staticmethod
    def verify(sql: str):
        return verify_sql(sql,
                        {
                            "tables": {
                                "orders": {
                                    "columns": ["id", "product_name", "account_id"],
                                    "restrictions": [{"column": "id", "value": 123}]
                                }
                            }
                        }, True)

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

    def test_select_illegal_col(self):
        result = self.verify("SELECT id, col FROM orders WHERE id = 123")
        assert result == {'allowed': False,'errors': ['Column col is not allowed. Column removed from SELECT clause'],
                          "fixed": "SELECT id FROM orders WHERE id = 123"}

    def test_select_no_legal_cols(self):
        result = self.verify("SELECT col1, col2 FROM orders WHERE id = 123")
        assert result == {'allowed': False,'errors': ['Column col1 is not allowed. Column removed from SELECT clause',
                                                      'Column col2 is not allowed. Column removed from SELECT clause'],
                          "fixed": None}

    def test_missing_restriction(self):
        result = self.verify("SELECT id FROM orders")
        assert result == {'allowed': False,'errors': ['Missing restriction for table: orders column: id value: 123'],
                          "fixed": "SELECT id FROM orders WHERE id = 123"}

    def test_wrong_restriction(self):
        result = self.verify("SELECT id FROM orders WHERE id = 234")
        assert result == {'allowed': False,
                          'errors': ["Missing restriction for table: orders column: id value: 123"],
                          "fixed": "SELECT id FROM orders WHERE id = 234 AND id = 123"}


