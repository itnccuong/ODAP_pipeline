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

## 📊 Phân tích yêu cầu

### Thời gian thực cần phân tích:
1. Khung giờ có nhiều giao dịch nhất
2. Thành phố có tổng giá trị giao dịch cao nhất
3. Merchant có số lượng/giá trị giao dịch cao nhất
4. Tỷ lệ fraud theo thành phố/merchant
5. Người dùng có nhiều giao dịch liên tiếp
6. Giao dịch giá trị lớn theo thời gian/địa điểm
7. Xu hướng fraud
8. So sánh ngày thường vs cuối tuần
9. Người dùng có nhiều lỗi/fraud
10. Đề xuất cải tiến hệ thống

## 🔄 Quy trình deployment

1. **Phase 1**: Setup infrastructure (Kafka, Hadoop, Spark, Airflow)
2. **Phase 2**: Kafka Producer cho CSV data
3. **Phase 3**: Spark Streaming applications
4. **Phase 4**: Data storage và partitioning
5. **Phase 5**: Airflow orchestration
6. **Phase 6**: Testing và monitoring

## 🧪 Testing từng phase

Mỗi phase sẽ có test riêng để đảm bảo hoạt động đúng trước khi chuyển sang phase tiếp theo.

## ⚡ Performance Improvements

### Recent Spark Configuration Optimizations:

#### Resource Allocation:
- **Spark Worker Memory**: Increased from 2G to 3G (configured in docker-compose.yml)
- **Executor Memory**: Increased from 1G to 2G for better processing performance
- **Driver Memory**: Increased from 512m to 1G for better coordination
- **Executor Cores**: Increased from 1 to 2 for parallel processing
- **Total Cores**: Optimized for better resource utilization

#### Memory Management:
- **Memory Fraction**: Optimized to 0.7 (from 0.6) for better memory usage
- **Storage Fraction**: Maintained at 0.3 for optimal caching
- **Adaptive Query Optimization**: Enhanced with parallelism-first coalescing

#### Simple Analytics Mode:
- **Console Output**: Simplified analytics output to console instead of Delta Lake
- **Reduced Complexity**: Avoids Delta Lake overhead for better performance
- **Faster Startup**: Optimized initialization to prevent hanging

### Scripts Updated:
- `submit_simple_spark_job.sh`: Updated with new resource configurations
- `simple_credit_card_analytics.py`: Enhanced with optimized Spark settings