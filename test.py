from python_sql_query2 import SQLQueryBuilder


# Example usage:
table_columns = {
    "customers": ["first_name", "last_name", "email"],
    "orders": [],
    "products": ["product_name", "price"]
}

# Create an instance of SQLQueryBuilder for a specific table
table_name = "products"
columns = table_columns[table_name]
sql_builder = SQLQueryBuilder(table_name, columns)

# Generate SQL query
query = sql_builder.generate_sql_query()

# Pass the query to another function or use it as needed
print(query)