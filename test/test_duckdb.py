import duckdb
import pytest

from sql_data_guard import verify_sql
from conftest import verify_sql_test
from conftest import verify_sql_test_duckdb


class TestDuckdbDialect:

    @pytest.fixture(scope="class")
    def cnn(self):
        with duckdb.connect(":memory:") as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS football_db")

            conn.execute(
                """
            CREATE TABLE players (
                name TEXT,
                jersey_no INT,
                position TEXT,
                age INT,
                national_team TEXT
            )"""
            )

            conn.execute(
                "INSERT INTO players VALUES ('Ronaldo', 7, 'CF', 40, 'Portugal')"
            )
            conn.execute(
                "INSERT INTO players VALUES ('Messi', 10, 'RWF', 38, 'Argentina')"
            )
            conn.execute(
                "INSERT INTO players VALUES ('Neymar', 10, 'LWF', 32, 'Brazil')"
            )
            conn.execute(
                "INSERT INTO players VALUES ('Mbappe', 10, 'LWF', 26, 'France')"
            )

            conn.execute(
                """
            CREATE TABLE stats (
                player_name TEXT,
                goals INT,
                assists INT,
                trophies INT)"""
            )

            conn.execute("INSERT INTO stats VALUES ('Ronaldo', 1030, 234, 37)")
            conn.execute("INSERT INTO stats VALUES ('Messi', 991, 372, 43)")
            conn.execute("INSERT INTO stats VALUES ('Neymar', 650, 182, 31)")
            conn.execute("INSERT INTO stats VALUES ('Mbappe', 410, 102, 19)")

            yield conn

    @pytest.fixture(scope="class")
    def config(self) -> dict:
        return {
            "tables": [
                {
                    "table_name": "players",
                    "database_name": "football_db",
                    "columns": [
                        "name",
                        "jersey_no",
                        "position",
                        "age",
                        "national_team",
                    ],
                    "restrictions": [
                        {"column": "name", "value": "Ronaldo"},
                        {"column": "position", "value": "CF"},
                    ],
                },
                {
                    "table_name": "stats",
                    "database_name": "football_db",
                    "columns": ["player_name", "goals", "assists", "trophies"],
                    "restrictions": [
                        {"column": "assists", "value": 234},
                    ],
                },
            ]
        }

    def test_access_not_allowed(self, config):
        verify_sql_test_duckdb(
            "SELECT * FROM test_table",
            config,
            errors={"Table test_table is not allowed"},
        )

    def test_access_with_restriction_pass(self, config):
        verify_sql_test_duckdb(
            """SELECT name, position from players WHERE name = 'Ronaldo' AND position = 'CF' """,
            config,
        )

    def test_access_with_restriction(self, config, cnn):
        verify_sql_test_duckdb(
            """SELECT name, position from players WHERE 
            name = 'Ronaldo' AND position = 'CF' """,
            config,
        )

    def test_insertion_not_allowed(self, config):
        verify_sql_test_duckdb(
            "INSERT into players values('Lewandowski', 9, 'CF', 'Poland' )",
            config,
            errors={"INSERT statement is not allowed"},
        )

    def test_access_restricted(self, config):
        verify_sql_test_duckdb(
            """SELECT goals from stats where assists = 234""",
            config,
        )

    def test_aggregate_sum_goals(self, config, cnn):
        res = verify_sql_test_duckdb(
            "SELECT sum(goals) from stats where assists = 234", config
        )

    def test_aggregate_sum_assists_condition(self, config):
        res = verify_sql("select sum(assists) from stats WHERE assists = 234", config)
        assert res["allowed"] == True, res

    def test_update_not_allowed(self, config):
        res = verify_sql(
            "UPDATE players SET national_team = 'Portugal' WHERE name = 'Messi'",
            config,
        )
        assert res["allowed"] == False, res
        assert "UPDATE statement is not allowed" in res["errors"], res

    def test_inner_join(self, config, cnn):
        res = verify_sql(
            """
            SELECT p.name, s.assists
            FROM players p 
            INNER JOIN stats s ON p.name = s.player_name
            WHERE p.name = 'Ronaldo' AND p.position = 'CF' AND s.assists = 234
            """,
            config,
        )
        assert res["allowed"] == True, res

    def test_cross_join_not_allowed(self, config):
        res = verify_sql(
            """
            SELECT p.name, s.trophies
            FROM players p 
            CROSS JOIN stats s
            """,
            config,
        )
        assert res["allowed"] == False, res
        assert (
            "Missing restriction for table: stats column: s.assists value: 234"
            in res["errors"]
        )
