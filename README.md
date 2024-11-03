# sql-guard

sql-guard is an open-source project designed to verify that SQL queries access only the data they are allowed to. It takes a query and a restriction configuration, and returns whether the query is allowed to run or not. Additionally, it can modify the query to ensure it complies with the restrictions.

## Why Use sql-guard?

You should use sql-guard if your application constructs SQL queries, and you need to ensure that only allowed data can be accessed. This is particularly useful if:
- Your application constructs complex SQL queries.
- Your application uses LLM (Large Language Models) to construct SQL queries and you cannot fully control the queries it creates.

sql-guard does not replace the database permissions model. Instead, it provides an additional layer of security, which is crucial when it is difficult or impossible to implement fine-grained, column-level, and row-level security in many database implementations. Instead of relying on the database  to enforce these restrictions, sql-guard helps you to overcome vendor-specific limitation can verify and modify queries before they are executed.

## How It Works

1. **Input**: sql-guard takes an SQL query and a restriction configuration as input.
2. **Verification**: It verifies whether the query complies with the restrictions specified in the configuration.
3. **Modification**: If the query does not comply, sql-guard can modify the query to ensure it meets the restrictions.
4. **Output**: It returns whether the query is allowed to run or not, and if necessary, the modified query.

## Example

```python
from sql_guard import verify_sql

config = {
    "tables": {
        "orders": {
            "columns": ["id", "product_name", "account_id"],
            "restrictions": [{"column": "account_id", "value": 123}]
        }
    }           
}

query = "SELECT account_id, account_name FROM orders WHERE account_id = 123"
result = verify_sql(query, config)
print(result)
```
Output:
```json
{
    "allowed": false,
    "errors": ["Column account_name is not allowed. Column removed from SELECT clause"],
    "fixed": "SELECT account_id FROM orders WHERE account_id = 123"
}
```
Here is a table with more examples of SQL queries and their corresponding JSON outputs:

| SQL Query                                                    | JSON Output                                                                                                                                                                                 |
|--------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SELECT id, product_name FROM orders WHERE account_id = 123` | ```{ "allowed": true, "errors": [], "fixed": null } ```                                                                                                                                     |
| `SELECT id FROM orders WHERE account_id = 456`               | ```{ "allowed": false, "errors": ["Missing restriction for table: orders column: account_id value: 123"], "fixed": "SELECT id FROM orders WHERE account_id = 456 AND account_id = 123" } ``` |

This table provides a variety of SQL queries and their corresponding JSON outputs, demonstrating how `sql-guard` handles different scenarios.

## Installation
To install sql-guard, use pip:

```bash
pip install sql-guard
```

## Docker Repository

sql-guard is also available as a Docker image, which can be used to run the application in a containerized environment. This is particularly useful for deployment in cloud environments or for maintaining consistency across different development setups.

### Running the Docker Container

To run the sql-guard Docker container, use the following command:

```bash
docker run -d --name sql-guard -p 8080:8080 imperva/sql-guard:latest

### Calling the Docker Container Using REST API

Once the `sql-guard` Docker container is running, you can interact with it using its REST API. Below is an example of how to verify an SQL query using `curl`:

```bash
curl -X POST http://localhost:8080/verify_sql \
     -H "Content-Type: application/json" \
     -d '{
           "query": "SELECT * FROM orders WHERE account_id = 123",
           "config": {
             "tables": {
               "orders": {
                 "columns": ["id", "product_name", "account_id"],
                 "restrictions": [{"column": "account_id", "value": 123}]
               }
             }
           }
         }'
```

## Contributing
We welcome contributions! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
