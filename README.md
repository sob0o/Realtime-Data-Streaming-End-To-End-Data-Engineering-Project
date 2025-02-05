# Real-Time Data Streaming: End-to-End Data Engineering Project

This project demonstrates the construction of a **real-time data streaming pipeline**, covering every phase from data ingestion to processing and storage. The pipeline leverages a robust stack of tools and technologies, including **Apache Airflow**, **Python**, **Apache Kafka**, **Apache Zookeeper**, **Apache Spark**, and **Cassandra**, all containerized using **Docker**.

---

## System Architecture

![System Architecture](https://github.com/sob0o/Realtime-Data-Streaming-End-To-End-Data-Engineering-Project/blob/main/Data%20engineering%20architecture.png)

The architecture of the project is composed of the following key components:

- **Data Source**: The `randomuser.me` API is used to generate random user data for the pipeline.
- **Apache Airflow**: Orchestrates the pipeline and stores the fetched data in a **PostgreSQL** database.
- **Apache Kafka and Zookeeper**: Facilitates real-time data streaming from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Enables monitoring and schema management for Kafka streams.
- **Apache Spark**: Handles data processing with master and worker nodes.
- **Cassandra**: Serves as the final storage destination for processed data.

---

## Key Tasks

The project involves the following tasks:

1. **Setting up a Data Pipeline with Apache Airflow**  
   - Orchestrate workflows and automate data ingestion.
   
2. **Real-Time Data Streaming with Apache Kafka**  
   - Stream data efficiently between components.

3. **Distributed Synchronization with Apache Zookeeper**  
   - Ensure coordination and synchronization across distributed systems.

4. **Data Processing with Apache Spark**  
   - Perform distributed data processing and transformations.

5. **Data Storage Solutions**  
   - Store raw data in **PostgreSQL** and processed data in **Cassandra**.

6. **Containerization with Docker**  
   - Containerize the entire setup for portability and scalability.

---

## Technologies Used

- **Apache Airflow**: Workflow orchestration.
- **Python**: Scripting and automation.
- **Apache Kafka**: Real-time data streaming.
- **Apache Zookeeper**: Distributed coordination.
- **Apache Spark**: Distributed data processing.
- **Cassandra**: NoSQL database for scalable storage.
- **PostgreSQL**: Relational database for raw data storage.
- **Docker**: Containerization of the entire setup.

---

## Motivation

This project was undertaken to **enhance my data engineering skills** and gain hands-on experience with modern technologies in the field. By building an end-to-end real-time data streaming pipeline, I aimed to understand the intricacies of data ingestion, processing, and storage in a distributed environment.

---

## How to Use

1. Clone the repository:
   ```bash
   git clone https://github.com/sob0o/Realtime-Data-Streaming-End-To-End-Data-Engineering-Project.git