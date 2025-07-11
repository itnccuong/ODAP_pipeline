# OLAP Pipeline for Credit Card Transaction Analysis

## 🏗️ Kiến trúc hệ thống

```
CSV Data → Kafka Producer → Kafka Topics → Spark Streaming → HDFS → Power BI
                                              ↓
                                         Airflow (Orchestration)
```

## 📁 Cấu trúc project

```
ODAP_pipeline/
├── docker-compose.yml          # Docker services configuration
├── hadoop.env                  # Hadoop environment variables
├── data/                      # CSV data files
├── spark-apps/                # Spark streaming applications
├── spark-data/                # Spark temporary data
├── dags/                      # Airflow DAGs
├── logs/                      # Airflow logs
├── plugins/                   # Airflow plugins
└── README.md                  # Project documentation
```

## 🚀 Services và Ports

### Kafka Ecosystem
- **Kafka Brokers**: 9092, 9093, 9094
- **Schema Registry**: 8081
- **Kafka Connect**: 8083
- **Kafka UI**: 8080

### Hadoop Ecosystem
- **HDFS NameNode**: 9870 (Web UI), 9000 (RPC)

### Spark Ecosystem
- **Spark Master**: 8081 (Web UI), 7077 (RPC)

### Airflow
- **Airflow Webserver**: 8082


# How to start
## Build the airflow Dockerfile
```
docker compose build airflow-webserver
```
## Build the docker compose
```
docker compose up -d

(if you want to down)
docker compose down -v
```

## Run the producer 
```
docker compose --profile producer up kafka-producer
```

## Run the Spark program
```
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/simple_kafka_consumer.py
```

## Trigger the csv_merge_for_powerbi dag on Airflow UI
- Go to http://localhost:8082/dags/csv_merge_for_powerbi
- Turn it on

## Cat the merge download csv file
```
chmod +x download_csv_from_hdfs.sh
./download_csv_from_hdfs.sh
```
