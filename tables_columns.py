

from python_sql_query2 import *

table_name = "products"
columns = table_columns[table_name]
sql_builder = SQLQueryBuilder(table_name, columns)

query = sql_builder.generate_sql_query()





