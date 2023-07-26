from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
from cassandra.cluster import Cluster
import sys

APP_NAME = "feedback_processor"
MASTER = "local"
KAFKA_SERVERS = "localhost:9092"
KAFKA_TOPIC = "customer_feedback"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "flask_app"
CASSANDRA_TABLE = "feedback"

try:
    spark_session = SparkSession.builder.master(MASTER).appName(APP_NAME) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()
    cassandra_cluster = Cluster(port = CASSANDRA_PORT)
    cassandra_session = cassandra_cluster.connect(keyspace = CASSANDRA_KEYSPACE)
except Exception as e:
    print(f"Error --> {e}")
    sys.exit()

create_table_query = f"""CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
                        timestamp TIMESTAMP, productRating INT, serviceRating INT,
                        comments TEXT, PRIMARY KEY (timestamp)
                        );"""
cassandra_session.execute(create_table_query)

schema = StructType().add("product_rating", StringType()) \
                    .add("service_rating", StringType()) \
                    .add("comments", StringType())

df = spark_session.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

def process_kafka_data(df, epoch):

    parsed_df = df.withColumn("parsed_value", from_json(col("value").cast("string"), schema))
    parsed_df = parsed_df.withColumn("key", col("key").cast("string"))

    product_rating = parsed_df.select(col("parsed_value.product_rating"))
    service_rating = parsed_df.select(col("parsed_value.service_rating"))
    comments = parsed_df.select(col("parsed_value.comments"))
    timestamp = parsed_df.select(col("key"))

    # showing the values in console for testing purposes
    product_rating.show()
    service_rating.show()
    comments.show()
    timestamp.show()


    # TODO call the write function

def write_data_to_cassandra():
    # TODO logic to add the data to the Cassandra table, NOTE: timestamp is in string format, will probably throw an error when trying to insert it as it is now
    pass


query = df.writeStream.foreachBatch(process_kafka_data).start()
query.awaitTermination()
