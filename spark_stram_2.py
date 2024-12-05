import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        logging.info("Streaming is being started...")

        # Output to the console instead of Cassandra
        streaming_query = (selection_df.writeStream.format("console")
                           .outputMode("append")
                           .option("truncate", "false")  # To avoid truncation of long text fields
                           .start())

        streaming_query.awaitTermination()
