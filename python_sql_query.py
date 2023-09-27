'''
Certainly! Here's a Python class with a `generate_sql_query` method that takes a dictionary where the keys are table names, 
and the values are lists of column names. It generates SQL SELECT queries based on the provided table names and column lists, 
and stores them in a list for further use:

This class `SQLQueryBuilder` has an `_init_` method to initialize an empty list to store queries, 
and the `generate_sql_query` method to generate SQL queries based on the provided table names and column lists. 
You can use it as shown in the example usage to generate and print the SQL queries.


'''

class SQLQueryBuilder:
    def __init__(self):
        self.queries = []

    def generate_sql_query(self, table_columns_dict):
        for table_name, columns in table_columns_dict.items():
            if not columns:
                # If the list of columns is empty, select all columns with "*"
                query = f"SELECT * FROM {table_name};"
            else:
                # Join the list of column names with commas
                column_names = ', '.join(columns)
                query = f"SELECT {column_names} FROM {table_name};"
            
            self.queries.append(query)

        return self.queries
    
# Example usage:
table_columns = {
    "customers": ["first_name", "last_name", "email"],
    "orders": [],
    "products": ["product_name", "price"]
}

sql_builder = SQLQueryBuilder()
queries = sql_builder.generate_sql_query(table_columns)

for query in queries:
    print(query)