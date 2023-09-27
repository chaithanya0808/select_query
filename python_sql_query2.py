'''
Here's a `SQLQueryBuilder` class that allows you to create an instance with a table name and then use the `generate_sql_query` 
method to generate an SQL query based on the provided table name and column list 
(where the table name is the key and columns are the value in a dictionary). 
You can then pass this query to another function or use it as needed.


In this `SQLQueryBuilder` class, you create an instance with a table name and optional columns. 
Then, you can use the `generate_sql_query` method to generate an SQL query for that specific table and columns. 
You can change the `table_name` and `columns` as needed for different tables.
'''


class SQLQueryBuilder:
    def __init__(self, table_name, columns=None):
        self.table_name = table_name
        self.columns = columns

    def generate_sql_query(self):
        if not self.columns:
            # If the list of columns is empty, select all columns with "*"
            query = f"SELECT * FROM {self.table_name};"
        else:
            # Join the list of column names with commas
            column_names = ', '.join(self.columns)
            query = f"SELECT {column_names} FROM {self.table_name};"

        return query





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