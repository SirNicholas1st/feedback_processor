from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import sys
import time

APP_NAME = "read_data"
MASTER = "local"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "flask_app"
CASSANDRA_TABLE = "feedback"

# Cassandra as a datasource doesnt support streamed reading.
# This is in no way even near the best practises but was done as a
# learning experience.
# More better way of doing this could be for example with Airflow

try:
    spark_session = SparkSession.builder \
    .appName(APP_NAME) \
    .config("spark.cassandra.connection.host", "localhost:9042") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .getOrCreate()
except Exception as e:
    print(f"Error --> {e}")
    sys.exit()

while True:

    cassandra_df = spark_session.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table = CASSANDRA_TABLE, keyspace = CASSANDRA_KEYSPACE) \
    .load()
    
    service_rating_average = cassandra_df.select(avg(col("serviceRating")).alias("serviceRatingAverage"))
    product_rating_average = cassandra_df.select(avg(col("productRating")).alias("productRatingAverage"))
    
    averages_df =service_rating_average.crossJoin(product_rating_average)

    averages_df.show(truncate = False)

    time.sleep(10)
    