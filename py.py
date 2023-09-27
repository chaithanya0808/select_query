from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json

class SQLServerConnector:
    def _init_(self, spark_session, key_columns):
        self.spark_session = spark_session
        self.key_columns = key_columns

    def read_data(self, jdbc_url, table_name):
        properties = {
            "user": "Shreyansh\Admin",
            "password": "",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "false"
        }
        sql_df = self.spark_session.read.jdbc(url=jdbc_url, table=table_name, properties=properties, numPartitions=5)
        sql_df.cache()

        # Generate select expressions for key and value dynamically
        select_expr = [col(column_name).cast("string").alias(column_name) for column_name in self.key_columns]
        select_expr.append(to_json(struct('*')).alias("value"))

        kafka_df = sql_df.select(*select_expr)
        return kafka_df