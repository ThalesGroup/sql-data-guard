import pytest

from rest.sql_data_guard_rest import app

class TestRestAppErrors:
    def test_verify_sql_method_not_allowed(self):
        result = app.test_client().get("/verify-sql")
        assert result.status_code == 405

    def test_verify_sql_no_json_data(self):
        result = app.test_client().post("/verify-sql")
        assert result.status_code == 400
        assert result.json == {"error": "Request must be JSON"}

    def test_verify_sql_no_sql(self):
        result = app.test_client().post("/verify-sql", json={"config": {}})
        assert result.status_code == 400
        assert result.json == {"error": "Missing 'sql' in request"}

    def test_very_sql_no_config(self):
        result = app.test_client().post("/verify-sql", json={"sql": "SELECT * FROM my_table"})
        assert result.status_code == 400
        assert result.json == {"error": "Missing 'config' in request"}

class TestRestAppVerifySql:
    @pytest.fixture(scope="class")
    def config(self) -> dict:
        return { "tables": [
                    {
                        "table_name": "orders",
                        "database_name": "orders_db",
                        "columns": ["id", "product_name", "account_id", "day"],
                        "restrictions": [{"column": "id", "value": 123}]
                    }
                ]
            }

    def test_verify_sql(self, config):
        result = app.test_client().post("/verify-sql",
                                        json={"sql": "SELECT id FROM orders WHERE id = 123",
                                              "config": config})
        assert result.status_code == 200
        assert result.json == {'allowed': True, 'errors': [], 'fixed': None}


    def test_verify_sql_error(self, config):
        result = app.test_client().post("/verify-sql",
                                        json={"sql": "SELECT id, another_col FROM orders WHERE id = 123",
                                              "config": config})
        assert result.status_code == 200
        assert result.json == {
            'allowed': False,
            'errors': ['Column another_col is not allowed. Column removed from SELECT clause'],
            'fixed': 'SELECT id FROM orders WHERE id = 123'
       }
