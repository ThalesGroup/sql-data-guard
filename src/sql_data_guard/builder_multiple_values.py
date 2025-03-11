def build_query(filters):
    conditions = []
    for filter in filters:
        column = filter["column"]
        values = filter["values"]

        # Handle lists for SQL 'IN' clause
        if isinstance(values, list):
            formatted_values = ", ".join(
                f"'{v}'" if isinstance(v, str) else str(v) for v in values
            )
            conditions.append(f"{column} IN ({formatted_values})")
        else:
            formatted_value = f"'{values}'" if isinstance(values, str) else str(values)
            conditions.append(f"{column} = {formatted_value}")

    where_clause = " AND ".join(conditions)
    query = f"SELECT * FROM orders WHERE {where_clause}"
    return query


# Example usage
filters = [
    {"column": "id", "values": [1, 2, 3]},
    {"column": "status", "values": ["pending", "shipped"]},
]

sql_query = build_query(filters)
print(sql_query)
