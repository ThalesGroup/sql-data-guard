from sql_data_guard import verify_sql
from sql_data_guard.cypher_guard import tokenize_and_strip


def test_tokenize_and_strip():
    """Tests the comment and string literal stripping logic."""
    query = "MATCH (n:User {name: 'Alice'}) // line comment\nRETURN n.id /* block comment */"
    cleaned, strings = tokenize_and_strip(query)
    assert "Alice" not in cleaned
    assert "$STR_0$" in cleaned
    assert "line comment" not in cleaned
    assert "block comment" not in cleaned
    assert strings == ["'Alice'"]


def test_verify_cypher_invalid_config():
    """Tests verify_cypher with invalid configuration."""
    res = verify_sql("MATCH (n) RETURN n", {}, dialect="cypher")
    assert not res["allowed"]
    assert "Invalid configuration" in res["errors"][0]


def test_verify_cypher_max_length():
    """Tests maximum length validation for Cypher."""
    config = {"tables": [{"table_name": "User", "columns": ["id"]}]}
    res = verify_sql("MATCH (n:User) RETURN n.id", {"max_length": 10, **config}, dialect="cypher")
    assert not res["allowed"]
    assert "exceeds maximum length" in res["errors"][0]


def test_mutation_blocking():
    """Tests that mutating clauses are rejected while keeping non-mutating ones allowed."""
    config = {
        "tables": [
            {"table_name": "User", "columns": ["id", "name"]},
            {"table_name": "Node", "columns": ["id"]},
        ]
    }

    # CREATE is rejected
    res = verify_sql("MATCH (n:User) CREATE (m:Node) RETURN n", config, dialect="cypher")
    assert not res["allowed"]
    assert any("CREATE statement is not allowed" in err for err in res["errors"])

    # DETACH DELETE is rejected
    res = verify_sql("MATCH (u:User {id: 123}) DETACH DELETE u", config, dialect="cypher")
    assert not res["allowed"]
    assert any("DELETE statement is not allowed" in err for err in res["errors"])
    assert any("DETACH statement is not allowed" in err for err in res["errors"])

    # SET is rejected
    res = verify_sql("MATCH (u:User) SET u.name = 'Bob'", config, dialect="cypher")
    assert not res["allowed"]
    assert any("SET statement is not allowed" in err for err in res["errors"])

    # MERGE is rejected
    res = verify_sql("MERGE (u:User {id: 123}) RETURN u", config, dialect="cypher")
    assert not res["allowed"]
    assert any("MERGE statement is not allowed" in err for err in res["errors"])

    # Allowed: mutating words inside string literals
    res = verify_sql("MATCH (u:User) WHERE u.name = 'CREATE user' RETURN u.id", config, dialect="cypher")
    assert res["allowed"]
    assert not res["errors"]

    # Allowed: mutating words inside comments
    res = verify_sql("MATCH (u:User) RETURN u.id // please do not CREATE anything", config, dialect="cypher")
    assert res["allowed"]
    assert not res["errors"]


def test_label_whitelisting():
    """Tests whitelisting of node labels and relationship types."""
    config = {
        "tables": [
            {"table_name": "User", "columns": ["id"]},
            {"table_name": "FRIEND_OF", "columns": ["since"]},
        ]
    }

    # Allowed label and relationship type
    res = verify_sql("MATCH (u:User)-[r:FRIEND_OF]->(f:User) RETURN u.id", config, dialect="cypher")
    assert res["allowed"]

    # Unauthorized node label
    res = verify_sql("MATCH (a:Admin) RETURN a.id", config, dialect="cypher")
    assert not res["allowed"]
    assert any("Table Admin is not allowed" in err for err in res["errors"])

    # Unauthorized relationship type
    res = verify_sql("MATCH (u:User)-[r:BLOCKED]->(f:User) RETURN u.id", config, dialect="cypher")
    assert not res["allowed"]
    assert any("Table BLOCKED is not allowed" in err for err in res["errors"])


def test_property_whitelisting():
    """Tests property/column whitelisting across inline maps and dot notation."""
    config = {
        "tables": [
            {"table_name": "User", "columns": ["id", "name", "accountId"]},
            {"table_name": "FRIEND_OF", "columns": ["since"]},
        ]
    }

    # Allowed properties
    query = (
        "MATCH (u:User {id: 123})-[r:FRIEND_OF {since: 2020}]->(f:User) "
        "WHERE u.accountId = 456 RETURN f.name"
    )
    res = verify_sql(query, config, dialect="cypher")
    assert res["allowed"]

    # Unauthorized dot notation property
    res = verify_sql("MATCH (u:User) RETURN u.password", config, dialect="cypher")
    assert not res["allowed"]
    assert any("Column password is not allowed" in err for err in res["errors"])

    # Unauthorized inline map property
    res = verify_sql("MATCH (u:User {password: 'secret'}) RETURN u.name", config, dialect="cypher")
    assert not res["allowed"]
    assert any("Column password is not allowed" in err for err in res["errors"])

    # Fallback checking on unlabeled variable
    res = verify_sql("MATCH (u) RETURN u.password", config, dialect="cypher")
    assert not res["allowed"]
    assert any("Column password is not allowed" in err for err in res["errors"])


def test_property_restrictions():
    """Tests property value restriction validation."""
    config = {
        "tables": [
            {
                "table_name": "User",
                "columns": ["id", "name", "accountId"],
                "restrictions": [{"column": "accountId", "value": 123}]
            }
        ]
    }

    # Query complies via WHERE clause (numeric)
    res = verify_sql("MATCH (u:User) WHERE u.accountId = 123 RETURN u.name", config, dialect="cypher")
    assert res["allowed"]

    # Query complies via inline map (numeric)
    res = verify_sql("MATCH (u:User {accountId: 123}) RETURN u.name", config, dialect="cypher")
    assert res["allowed"]

    # Query violates (wrong value)
    res = verify_sql("MATCH (u:User) WHERE u.accountId = 456 RETURN u.name", config, dialect="cypher")
    assert not res["allowed"]
    assert any("Missing restriction for table: User column: accountId value: 123" in err for err in res["errors"])

    # Query violates (missing restriction completely)
    res = verify_sql("MATCH (u:User) RETURN u.name", config, dialect="cypher")
    assert not res["allowed"]
    assert any("Missing restriction for table: User column: accountId value: 123" in err for err in res["errors"])


def test_neo4j_dialect_alias():
    """Tests that the 'neo4j' dialect is correctly handled too."""
    config = {"tables": [{"table_name": "User", "columns": ["id"]}]}
    res = verify_sql("MATCH (u:User) RETURN u.id", config, dialect="neo4j")
    assert res["allowed"]
