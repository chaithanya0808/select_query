'''
Certainly! Here's an updated version of the `SQLQueryBuilder` class that allows you to generate an SQL query with optional column
 filtering using a WHERE condition on one or more columns. You can change the `table_name`, `columns`, and `where_conditions`
as needed for different tables.

In this updated `SQLQueryBuilder` class, you can specify optional `columns` and `where_conditions` when creating an instance. 
The `generate_sql_query` method generates an SQL query that includes the specified columns and applies the WHERE conditions as needed. 
You can adjust these parameters for different tables and filtering requirements.

'''

'person':['id','name']



class SQLQueryBuilder:
    def __init__(self, table_name, columns=None, where_conditions=None):
        self.table_name = table_name
        self.columns = columns
        self.where_conditions = where_conditions    

    def generate_sql_query(self):
        if not self.columns:
            # If the list of columns is empty, select all columns with "*"
            column_names = "*"
        else:
            # Join the list of column names with commas
            column_names = ', '.join(self.columns)

        query = f"SELECT {column_names} FROM {self.table_name}"

        if self.where_conditions:
            # Join multiple WHERE conditions with "AND"
            where_clause = ' AND '.join(self.where_conditions)
            query += f" WHERE {where_clause};"

        return query

# Example usage:
#table_name = "customers"
#columns = ["first_name", "last_name", "email"]
#where_conditions = ["age > 25", "city = 'New York'"]
tables={'table_name':'customers','columns':["first_name", "last_name", "email"],'where_conditions':["age > 25", "city = 'New York'"]}
# Create an instance of SQLQueryBuilder for a specific table with filtering
sql_builder = SQLQueryBuilder(tables['table_name'], tables['columns'], tables['where_conditions'])

# Generate SQL query
sql_query = sql_builder.generate_sql_query()

# Pass the query to another function or use it as needed
print(sql_query)