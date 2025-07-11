#!/usr/bin/env python3
"""
Kafka Producer for Credit Card Transactions
Đọc CSV data và stream từng record đến Kafka topic theo interval ngẫu nhiên 1-3 giây
"""

import csv
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TransactionProducer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='credit-card-transactions'):
        """
        Initialize Kafka Producer
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            topic: Kafka topic name
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',  # Chờ tất cả replicas acknowledge
            retries=3,   # Retry 3 lần nếu fail
            batch_size=16384,  # Batch size for efficiency
            linger_ms=10,      # Wait 10ms to batch records
        )
        logger.info(f"Kafka Producer initialized for topic: {topic}")
    
    def csv_to_dict(self, csv_file_path):
        """
        Đọc CSV file và convert thành list of dictionaries
        
        Args:
            csv_file_path: Path to CSV file
            
        Returns:
            List of transaction dictionaries
        """
        transactions = []
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Add timestamp và processing metadata
                    transaction = {
                        'user_id': row['User'],
                        'card_number': row['Card'],
                        'year': int(row['Year']),
                        'month': int(row['Month']),
                        'day': int(row['Day']),
                        'time': row['Time'],
                        'amount': float(row['Amount']),
                        'use_chip': row['Use Chip'],
                        'merchant_name': row['Merchant Name'],
                        'merchant_city': row['Merchant City'],
                        'merchant_state': row['Merchant State'],
                        'zip_code': row['Zip'],
                        'mcc': row['MCC'],
                        'errors': row['Errors?'],
                        'is_fraud': row['Is Fraud?'],
                        # Metadata cho processing
                        'ingested_at': datetime.now().isoformat(),
                        'source': 'csv_producer'
                    }
                    transactions.append(transaction)
            
            logger.info(f"Loaded {len(transactions)} transactions from {csv_file_path}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return []
    
    def send_transaction(self, transaction):
        """
        Send single transaction to Kafka
        
        Args:
            transaction: Transaction dictionary
        """
        try:
            # Use user_id as key for partitioning
            key = transaction['user_id']
            
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                key=key,
                value=transaction
            )
            
            # Get result (blocking)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Sent transaction: User={transaction['user_id']}, "
                f"Amount=${transaction['amount']}, "
                f"Merchant={transaction['merchant_name']} "
                f"-> Partition={record_metadata.partition}, Offset={record_metadata.offset}"
            )
            
        except KafkaError as e:
            logger.error(f"Failed to send transaction: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
    
    def produce_from_csv(self, csv_file_path, loop=False):
        """
        Stream transactions từ CSV file đến Kafka
        
        Args:
            csv_file_path: Path to CSV file
            loop: Whether to loop infinitely through the data
        """
        transactions = self.csv_to_dict(csv_file_path)
        
        if not transactions:
            logger.error("No transactions to process")
            return
        
        logger.info(f"Starting to stream {len(transactions)} transactions...")
        
        try:
            while True:
                for i, transaction in enumerate(transactions):
                    # Send transaction
                    self.send_transaction(transaction)
                    
                    # Random delay between 1-3 seconds (as per requirement)
                    delay = random.uniform(1, 3)
                    logger.info(f"Waiting {delay:.2f}s before next transaction...")
                    time.sleep(delay)
                
                if not loop:
                    break
                    
                logger.info("Completed one cycle, starting over...")
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Credit Card Transaction Kafka Producer')
    parser.add_argument(
        '--csv-file',
        default='/opt/airflow/data/sample_transactions.csv',
        help='Path to CSV file'
    )
    parser.add_argument(
        '--topic',
        default='credit-card-transactions',
        help='Kafka topic name'
    )
    parser.add_argument(
        '--brokers',
        default='kafka-broker:29092',
        help='Kafka broker addresses (comma-separated)'
    )
    parser.add_argument(
        '--loop',
        action='store_true',
        help='Loop infinitely through the data'
    )
    
    args = parser.parse_args()
    
    # Parse brokers
    bootstrap_servers = [broker.strip() for broker in args.brokers.split(',')]
    
    # Create producer
    producer = TransactionProducer(
        bootstrap_servers=bootstrap_servers,
        topic=args.topic
    )
    
    # Start producing
    producer.produce_from_csv(args.csv_file, loop=args.loop)

if __name__ == "__main__":
    main()
