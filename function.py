'''Certainly! You can create a Python function called `generate_sql_query` that takes a table name and a list of columns
 as arguments and generates an SQL query based on them. Here's an example implementation using f-strings:'''


def generate_sql_query(table_name, columns):
    # Check if the columns list is not empty
    if not columns:
        return f"SELECT * FROM {table_name};"
    
    # Generate the comma-separated column names
    column_names = ', '.join(columns)
    
    # Create the SQL query
    sql_query = f"SELECT {column_names} FROM {table_name};"
    
    return sql_query




table_name = "customers"
columns = ["id", "name", "email"]
query = generate_sql_query(table_name, columns)
print(query)


