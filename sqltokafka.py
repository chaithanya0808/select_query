import pyspark
import kafka
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json
import os
#from confluent_kafka import Producer
from kafka import KafkaProducer

class KafkaDataPublisher:
    def __init__(self, kafka_bootstrap_servers):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

    def connect_kafka_producer(self):
        producer_config = {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'client.id': 'kafka-producer'
        }
        producer = KafkaProducer(producer_config)
        return producer

    def publish_message(self, producer_instance, topic_name, key, value):
        try:
            producer_instance.produce(topic_name, key=key, value=value)
            producer_instance.flush()
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message:')
            print(str(ex))

    def publish_data_to_kafka(self, table_name):
        # Create a SparkSession
        spark = SparkSession.builder \
            .appName("SQLServerToKafka") \
            .config("spark.driver.extraClassPath",
                    "C:\\Users\\chait\\Downloads\\sqljdbc_12.4.1.0_enu\\sqljdbc_12.4\\enu\\jars\\mssql-jdbc-12.4.1.jre8.jar") \
            .config("spark.executor.extraClassPath",
                    "C:\\Users\\chait\\Downloads\\sqljdbc_12.4.1.0_enu\\sqljdbc_12.4\\enu\\jars\\mssql-jdbc-12.4.1.jre8.jar") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g").getOrCreate()

        # Read data from SQL Server
        jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=initial;"
        properties = {
            "user": "chaithanya",
            "password": "12345678",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "false"
        }

        sql_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties, numPartitions=5)
        sql_df.cache()

        # Transforming Data for Kafka
        kafka_df = sql_df.selectExpr("CAST(FirstName AS STRING) AS key", "to_json(struct(*)) AS value")

        # Kafka topic to send the message to
        topic = 'Reddy'

        # Create a Kafka producer instance
        kafka_producer = self.connect_kafka_producer()

        # Publish messages to the Kafka topic
        for row in kafka_df.collect():
            key = row.key
            value = row.value
            self.publish_message(kafka_producer, topic, key, value)

        print("Data written to Kafka topic 'Reddy'")

        # Close the Kafka producer
        kafka_producer.flush()
        kafka_producer.close()

if __name__ == "__main__":
    kafka_bootstrap_servers = "Shreyansh:9092"
    table_name = "Persons"

    data_publisher = KafkaDataPublisher(kafka_bootstrap_servers)
    data_publisher.publish_data_to_kafka(table_name)