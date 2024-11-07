# sql-data-guard: Safety Layer for LLM Database Interactions

SQL is the go-to language for performing queries on databases and for a good reason - it’s well known, easy to use and pretty simple. However, it seems that it’s as easy to use as it is to exploit and SQL injection is still one of the most targeted vulnerabilities.


To help solve this problem, we developed sql-data-guard, an open-source project designed to verify that SQL queries access only the data they are allowed to. It takes a query and a restriction configuration, and returns whether the query is allowed to run or not. Additionally, it can modify the query to ensure it complies with the restrictions.
sql-data-guard is particularly useful when constructing SQL queries with Large Language Models (LLMs), as such queries can’t run as prepared statements. Prepared statements secure a query’s structure, but LLM-generated queries are dynamic and lack this fixed form, increasing SQL injection risk. sql-data-guard mitigates this by inspecting and validating the query content.


By verifying and modifying queries before they are executed, sql-data-guard helps prevent unauthorized data access and accidental data exposure. Adding sql-data-guard to your application can prevent or minimize data breaches and sql-injection attacks impact, ensuring that only permitted data is accessed.
Connecting LLMs to SQL databases without strict controls can risk accidental data exposure, as models may generate SQL queries that access sensitive information. OWASP highlights cases of poor sandboxing leading to unauthorized disclosures, emphasizing the need for clear access controls and prompt validation. Businesses should adopt rigorous access restrictions, regular audits, and robust API security, especially to comply with privacy laws and regulations like GDPR and CCPA, which penalize unauthorized data exposure.


## Why Use sql-data-guard?

Consider using sql-data-guard if your application constructs SQL queries, and you need to ensure that only permitted data is accessed. This is particularly beneficial if:
- Your application generates complex SQL queries.
- Your application employs LLM (Large Language Models) to create SQL queries, making it difficult to fully control the queries.
- Different application users and roles should have different permissions, and you need to correlate an application user or role with a fine-grained data access permission.
- In multi-tenant applications, you need to ensure that each tenant can access only their data, which requires row-level security and often cannot be done using the database permissions model.

sql-data-guard does not replace the database permissions model. Instead, it adds an extra layer of security, which is crucial when implementing fine-grained, column-level, and row-level security is challenging or impossible. 
Data restrictions are often complex and cannot be expressed by the database permissions model. For instance, you may need to restrict access to specific columns or rows based on intricate business logic, which many database implementations do not support. Instead of relying on the database to enforce these restrictions, sql-data-guard helps you overcome vendor-specific limitations by verifying and modifying queries before they are executed.

## How It Works

1. **Input**: sql-data-guard takes an SQL query and a restriction configuration as input.
2. **Verification**: It verifies whether the query complies with the restrictions specified in the configuration.
3. **Modification**: If the query does not comply, sql-data-guard can modify the query to ensure it meets the restrictions.
4. **Output**: It returns whether the query is allowed to run or not, and if necessary, the modified query.

sql-data-guard is designed to be easy to integrate into your application. It provides a simple API that you can call to verify and modify SQL queries before they are executed. You can integrate it using REST API or directly in your application code. 

## Example

Below you can find a python snippet with allowed data access configuration, and usage of sql-data-guard. sql-data-guard finds a restricted column and an “always-true” possible injection and removes them both. It also adds a missing data restriction:

```python
from sql_data_guard import verify_sql

config = {
    "tables": [
        {
            "name": "orders",
            "columns": ["id", "product_name", "account_id"],
            "restrictions": [{"column": "account_id", "value": 123}]
        }
    ] 
}

query = "SELECT id, name FROM orders WHERE 1 = 1"
result = verify_sql(query, config)
print(result)
```
Output:
```json
{
    "allowed": false,
    "errors": ["Column name not allowed. Column removed from SELECT clause", 
      "Always-True expression is not allowed", "Missing restriction for table: orders column: account_id value: 123"],
    "fixed": "SELECT id, product_name, account_id FROM orders WHERE account_id = 123"
}
```
Here is a table with more examples of SQL queries and their corresponding JSON outputs:

| SQL Query                                               | JSON Output                                                                                                                                                                         |
|---------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SELECT id, product_name FROM orders WHERE account_id = 123 | { "allowed": true, "errors": [], "fixed": null }                                                                                                                                    |
| SELECT id FROM orders WHERE account_id = 456            | { "allowed": false, "errors": ["Missing restriction for table: orders column: account_id value: 123"], "fixed": "SELECT id FROM orders WHERE account_id = 456 AND account_id = 123" } |
| SELECT id, col FROM orders WHERE account_id = 123      | { "allowed": false, "errors": ["Column col is not allowed. Column removed from SELECT clause"], "fixed": "SELECT id FROM orders WHERE account_id = 123" } ```               |
| SELECT id FROM orders WHERE account_id = 123 OR 1 = 1 | { "allowed": false, "errors": ["Always-True expression is not allowed"], "fixed": "SELECT id FROM orders WHERE account_id = 123" }                                                  |
|SELECT * FROM orders WHERE account_id = 123| {"allowed": false, "errors": ["SELECT * is not allowed"], "fixed": "SELECT id, product_name, account_id FROM orders WHERE account_id = 123"}                                |
```


This table provides a variety of SQL queries and their corresponding JSON outputs, demonstrating how `sql-data-guard` handles different scenarios.

## Installation
To install sql-data-guard, use pip:

```bash
pip install sql-data-guard
```

## Docker Repository

sql-data-guard is also available as a Docker image, which can be used to run the application in a containerized environment. This is particularly useful for deployment in cloud environments or for maintaining consistency across different development setups.

### Running the Docker Container

To run the sql-data-guard Docker container, use the following command:

```bash
docker run -d --name sql-data-guard -p 8080:8080 imperva/sql-data-guard:latest

### Calling the Docker Container Using REST API

Once the `sql-data-guard` Docker container is running, you can interact with it using its REST API. Below is an example of how to verify an SQL query using `curl`:

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
