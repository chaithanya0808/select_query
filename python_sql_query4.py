'''
the `SQLQueryBuilder` class to take `columns` and `where_conditions` as dictionaries, 
allowing you to create an instance with these options and generate SQL queries accordingly. 
Here's an example implementation:

In this version of the `SQLQueryBuilder` class, `columns` and `where_conditions` are dictionaries, 
allowing you to specify which columns to select and the WHERE conditions for filtering. You can adjust the dictionaries 
for different tables and filtering requirements.

'''


class SQLQueryBuilder:
    def __init__(self, table_name, columns=None, where_conditions=None):
        self.table_name = table_name
        self.columns = columns
        self.where_conditions = where_conditions

    def generate_sql_query(self):
        query = f"SELECT {self._format_columns()} FROM {self.table_name}"
        if self.where_conditions:
            query += f" WHERE {self._format_where_conditions()}"
        return query

    def _format_columns(self):
        if not self.columns:
            return "*"
        return ', '.join(self.columns)

    def _format_where_conditions(self):
        if not self.where_conditions:
            return ""
        conditions = []
        for column, value in self.where_conditions.items():
            conditions.append(f"{column} = '{value}'")
        return ' AND '.join(conditions)

# Example usage:
table_name = "customers"
columns_dict = {"first_name": None, "last_name": None, "email": None}
where_conditions_dict = {"age": 25, "city": "New York"}

# Create an instance of SQLQueryBuilder for a specific table with filtering
sql_builder = SQLQueryBuilder(table_name, columns_dict, where_conditions_dict)

# Generate SQL query
query = sql_builder.generate_sql_query()

# Pass the query to another function or use it as needed
print(query)