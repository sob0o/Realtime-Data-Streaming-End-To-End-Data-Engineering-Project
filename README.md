# Realtime-Data-Streaming-End-To-End-Data-Engineering-Project
 building a real-time data streaming pipeline, covering each phase from data ingestion to processing and finally storage. We'll utilize a powerful stack of tools and technologies, including Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra—all neatly containerized using Docker.


## System Architecture

![System Architecture](https://github.com/sob0o/Realtime-Data-Streaming-End-To-End-Data-Engineering-Project/blob/main/Data%20engineering%20architecture.png)

The project is designed with the following components:

- **Data Source**: We use `randomuser.me` API to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.

## Tasks

- Setting up a data pipeline with Apache Airflow
- Real-time data streaming with Apache Kafka
- Distributed synchronization with Apache Zookeeper
- Data processing techniques with Apache Spark
- Data storage solutions with Cassandra and PostgreSQL
- Containerizing your entire data engineering setup with Docker

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker



**I did this project toto enhance my data engineering skills and explore modern technologies in the field.**

