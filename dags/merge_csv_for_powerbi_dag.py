import os
import io
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import pyhdfs

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'hdfs'

HDFS_SOURCE_PATH = "/delta-lake/credit-card-analytics/simple_consumer/csv_output"
HDFS_POWERBI_PATH = "/powerBI_ready"
POWERBI_FILENAME = "credit_card_transactions_merged.csv"

def merge_csv_for_powerbi(**kwargs):
    import subprocess
    import tempfile
    import time
    
    hdfs_client = pyhdfs.HdfsClient(hosts=f'{HDFS_HOST}:{HDFS_PORT}', user_name=HDFS_USER)
    
    timestamp = int(time.time())
    
    temp_dir = f"/tmp/airflow_merge_{timestamp}"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        print(f"⏳ Starting CSV merge process for PowerBI at {datetime.now()}")
        
        try:
            if not hdfs_client.exists(HDFS_SOURCE_PATH):
                print(f"⚠️ Source directory {HDFS_SOURCE_PATH} does not exist in HDFS")
                return
                
            file_list = hdfs_client.listdir(HDFS_SOURCE_PATH)
            csv_files = [f for f in file_list if re.match(r'part-.*\.csv', f)]
            
            if not csv_files:
                print(f"⚠️ No CSV files found in {HDFS_SOURCE_PATH}")
                return
                
        except Exception as e:
            print(f"⚠️ Error listing files in HDFS: {str(e)}")
            return
        
        merged_lines = []
        header_written = False
        expected_columns = None
        
        for i, file_name in enumerate(csv_files):
            full_hdfs_path = f"{HDFS_SOURCE_PATH}/{file_name}"
            local_file_path = os.path.join(temp_dir, file_name)
            
            try:
                with open(local_file_path, 'wb') as local_file:
                    file_data = hdfs_client.open(full_hdfs_path)
                    local_file.write(file_data.read())
                
                print(f"📥 Downloaded {file_name}")
                
                with open(local_file_path, 'r', encoding='utf-8') as file:
                    lines = file.readlines()
                
                if not lines:
                    print(f"⚠️ File {file_name} is empty")
                    continue
                
                for line_num, line in enumerate(lines):
                    line = line.strip()
                    if not line:
                        continue
                    
                    columns = line.split(',')
                    
                    if line_num == 0:  # Header line
                        if not header_written:
                            # First file - use its header as the standard
                            expected_columns = len(columns)
                            merged_lines.append(line)
                            header_written = True
                            print(f"✅ Header set with {expected_columns} columns: {line[:100]}...")
                        else:
                            # Skip headers from subsequent files
                            print(f"⏭️ Skipping header from {file_name}")
                            continue
                    else:
                        # Data line - validate column count
                        if expected_columns and len(columns) == expected_columns:
                            merged_lines.append(line)
                        else:
                            print(f"⚠️ Skipping malformed line in {file_name} (expected {expected_columns} columns, got {len(columns)}): {line[:100]}...")
                
                print(f"✅ Processed {file_name} successfully")
                
            except Exception as e:
                print(f"⚠️ Error processing file {file_name}: {str(e)}")
                continue
        
        if len(merged_lines) <= 1:  # Only header or no data
            print("⚠️ No valid data was read from any CSV file")
            return
            
        merged_file = os.path.join(temp_dir, POWERBI_FILENAME)
        with open(merged_file, 'w', encoding='utf-8') as output_file:
            for line in merged_lines:
                output_file.write(line + '\n')
        
        print(f"✅ Successfully merged {len(csv_files)} CSV files into {len(merged_lines)} lines")
        
        if not hdfs_client.exists(HDFS_POWERBI_PATH):
            hdfs_client.mkdirs(HDFS_POWERBI_PATH)
        
        with open(merged_file, 'rb') as local_file:
            hdfs_client.create(f"{HDFS_POWERBI_PATH}/{POWERBI_FILENAME}", local_file.read(), overwrite=True)
        
        print(f"✅ Successfully uploaded merged CSV to {HDFS_POWERBI_PATH}/{POWERBI_FILENAME}")
        
        line_count = len(merged_lines)
        data_rows = line_count - 1 if line_count > 0 else 0  # Subtract header
        print(f"📊 Merged file contains {line_count} total lines ({data_rows} data rows + 1 header)")
        
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        print(f"🧹 Cleaned up temporary directory {temp_dir}")

dag = DAG(
    'csv_merge_for_powerbi',
    default_args=default_args,
    description='Merge CSV files from Spark streaming job for PowerBI',
    schedule_interval=timedelta(seconds=20),
    start_date=datetime(2025, 7, 11),
    catchup=False,
    max_active_runs=1,  # Only one instance can run at a time
    tags=['csv', 'merge', 'powerbi'],
)

def check_hdfs_connection(**kwargs):
    import pyhdfs
    
    try:
        hdfs_client = pyhdfs.HdfsClient(hosts=f'{HDFS_HOST}:{HDFS_PORT}', user_name=HDFS_USER)
        root_files = hdfs_client.listdir('/')
        print(f"✅ Successfully connected to HDFS. Root directory contains: {root_files}")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to HDFS: {str(e)}")
        raise

check_hdfs_task = PythonOperator(
    task_id='check_hdfs_connection',
    python_callable=check_hdfs_connection,
    dag=dag,
)

merge_csv_task = PythonOperator(
    task_id='merge_csv_files',
    python_callable=merge_csv_for_powerbi,
    dag=dag,
)

check_hdfs_task >> merge_csv_task
