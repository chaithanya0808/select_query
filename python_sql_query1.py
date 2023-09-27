'''
here's a modified `SQLQueryBuilder` class that takes a table name as a parameter and returns an SQL query as an output. 
You can use this class to generate SQL queries based on the provided table names and column lists by calling its methods.


In this modified `SQLQueryBuilder` class, you can create an instance of the class with a table name and then use the 
`generate_sql_query` method to generate an SQL query based on the provided table name and column list. 
You can pass this query to another function or use it as needed.
'''



class SQLQueryBuilder:
    def __init__(self, table_name):
        self.table_name = table_name

    def generate_sql_query(self, columns=None):
        if not columns:
            # If the list of columns is empty, select all columns with "*"
            query = f"SELECT * FROM {self.table_name};"
        else:
            # Join the list of column names with commas
            column_names = ', '.join(columns)
            query = f"SELECT {column_names} FROM {self.table_name};"

        return query

# Example usage:
table_name = "customers"
columns = ["first_name", "last_name", "email"]

# Create an instance of SQLQueryBuilder
sql_builder = SQLQueryBuilder(table_name)

# Generate SQL query
query = sql_builder.generate_sql_query(columns)

# Pass the query to another function or print it
print(query)