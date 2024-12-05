from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkKafkaIntegration") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "broker:29092"  # Kafka broker address

# Read data from Kafka topic
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "your-kafka-topic") \
    .load()

# Do some processing on the data
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show()

# Writing data to Kafka (optional)
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", "another-kafka-topic") \
    .save()