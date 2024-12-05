import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Cassandra keyspace and table creation
def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace 'spark_streams' created successfully!")
    except Exception as e:
        logging.error(f"Failed to create keyspace: {e}")


def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT);
        """) 
        logging.info("Table 'created_users' created successfully!")
    except Exception as e:
        logging.error(f"Failed to create table: {e}")

# Insert data into Cassandra
def insert_data(session, **kwargs):
    logging.debug(f"Preparing to insert data: {kwargs}")
    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            kwargs.get('id'), kwargs.get('first_name'), kwargs.get('last_name'),
            kwargs.get('gender'), kwargs.get('address'), kwargs.get('post_code'),
            kwargs.get('email'), kwargs.get('username'), kwargs.get('registered_date'),
            kwargs.get('phone'), kwargs.get('picture')
        ))
        logging.info(f"Data inserted for {kwargs.get('first_name')} {kwargs.get('last_name')}")
    except Exception as e:
        logging.error(f"Failed to insert data: {e}")

# Create Spark connection
def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection established successfully!")
        return spark_conn
    
    
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None

# Connect to Kafka
def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'latest') \
            .option("failOnDataLoss", "false") \
            .load()
        

        logging.info("Kafka DataFrame created successfully.")
        return spark_df
    
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

# Create Cassandra connection
def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        logging.info("Cassandra connection established successfully!")
        return session
    except Exception as e:
        logging.error(f"Failed to create Cassandra connection: {e}")
        return None

# Parse Kafka DataFrame and extract schema
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

    try:
        parsed_df = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')) \
            .select("data.*")
        logging.info("Kafka data parsed successfully.")
        return parsed_df
    except Exception as e:
        logging.error(f"Failed to parse Kafka data: {e}")
        return None

# Function to print received batch data to the console
def print_batch(batch_df, batch_id):
    logging.info(f"Processing new batch with id {batch_id}:")
    batch_df.show(truncate=False)

if __name__ == "__main__":
    logging.info("Application started.")
    
    # Create Spark connection
    spark_conn = create_spark_connection()
    if spark_conn:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        if spark_df:
            # Parse Kafka data
            selection_df = create_selection_df_from_kafka(spark_df)
            if selection_df:
                # Create Cassandra connection
                cassandra_session = create_cassandra_connection()
                if cassandra_session:
                    create_keyspace(cassandra_session)
                    create_table(cassandra_session)

                    try:
                        logging.info("Starting streaming...")

                        # Write data to Cassandra and print to console
                        # streaming_query = (selection_df.writeStream
                        #                    .foreachBatch(print_batch)  # Print each batch's data
                        #                    .outputMode("append")  # Or 'update' depending on your use case
                        #                    .start())

                        # streaming_query.awaitTermination()

                        streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                        .option('checkpointLocation', '/tmp/checkpoint')
                        .option('keyspace', 'spark_streams')
                        .option('table', 'created_users')
                        .start())

                        streaming_query.awaitTermination()

                    except Exception as e:
                        logging.error(f"Streaming failed: {e}")
