from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, first
from pyspark.sql.types import StructType, StringType
from cassandra.cluster import Cluster
from datetime import datetime
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

    product_rating = parsed_df.select(col("parsed_value.product_rating")).collect()
    service_rating = parsed_df.select(col("parsed_value.service_rating")).collect()
    comments = parsed_df.select(col("parsed_value.comments")).collect()
    timestamp_str = parsed_df.select(col("key")).collect()
    


    write_data_to_cassandra(p_rating = product_rating, s_rating = service_rating,
                           comments = comments, timestamp_str = timestamp_str)

def write_data_to_cassandra(p_rating, s_rating, comments, timestamp_str):
    
    # Checking if there is data to write, the p_rating is a required field in the form so it should always be present.
    if len(p_rating) == 0:
        print("No data to write.")
        return
    
    timestamp_dt = datetime.strptime(timestamp_str[0][0], "%Y-%m-%dT%H:%M:%S.%f")

    insert_statement = f"""INSERT INTO {CASSANDRA_TABLE}
                            (timestamp, productRating, serviceRating, comments)
                            VALUES (?, ?, ?, ?);
                            """
    prepared_insert_statement = cassandra_session.prepare(insert_statement)

    # TODO The values, excluding timestamp dt, still in list format. Need to convert them to the actual values in the correct dtype.
    cassandra_session.execute(prepared_insert_statement, (timestamp_dt, p_rating, s_rating, comments))
    print("Data inserted")
    return 


query = df.writeStream.foreachBatch(process_kafka_data).start()
query.awaitTermination()
