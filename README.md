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
