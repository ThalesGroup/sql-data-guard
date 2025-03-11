def build_query(filters):
    conditions = []
    for filter in filters:
        column = filter["column"]
        values = filter["values"]

        # Convert list of values into a SQL 'IN' clause
        if isinstance(values, list):
            placeholders = ", ".join(str(v) for v in values)
            conditions.append(f"{column} IN ({placeholders})")
        else:
            conditions.append(f"{column} = {values}")

    where_clause = " AND ".join(conditions)
    query = f"SELECT * FROM my_table WHERE {where_clause}"
    return query


# Example usage
filters = [{"column": "id", "values": [1, 2, 3]}]
sql_query = build_query(filters)
print(sql_query)
