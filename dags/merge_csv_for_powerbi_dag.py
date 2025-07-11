"""
Airflow DAG that merges CSV files from Spark streaming job every 20 seconds
and places them in a PowerBI ready format in HDFS
"""

import os
import io
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import pyhdfs

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# HDFS connection parameters
HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'hdfs'

# CSV paths
HDFS_SOURCE_PATH = "/delta-lake/credit-card-analytics/simple_consumer/csv_output"
HDFS_POWERBI_PATH = "/powerBI_ready"
POWERBI_FILENAME = "credit_card_transactions_merged.csv"

# Python function to merge CSV files in HDFS and save to PowerBI ready location
def merge_csv_for_powerbi(**kwargs):
    import subprocess
    import tempfile
    import time
    import pandas as pd
    
    # HDFS client configuration - use the namenode service running in the docker network
    hdfs_client = pyhdfs.HdfsClient(hosts=f'{HDFS_HOST}:{HDFS_PORT}', user_name=HDFS_USER)
    
    # Create a timestamp for the temp directory
    timestamp = int(time.time())
    
    # Create a temporary directory for this execution
    temp_dir = f"/tmp/airflow_merge_{timestamp}"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        print(f"⏳ Starting CSV merge process for PowerBI at {datetime.now()}")
        
        # Step 1: Get list of CSV files in the source directory
        try:
            # Check if directory exists
            if not hdfs_client.exists(HDFS_SOURCE_PATH):
                print(f"⚠️ Source directory {HDFS_SOURCE_PATH} does not exist in HDFS")
                return
                
            # List all files in the HDFS directory
            file_list = hdfs_client.listdir(HDFS_SOURCE_PATH)
            # Filter for CSV files that match the part-*.csv pattern
            csv_files = [f for f in file_list if re.match(r'part-.*\.csv', f)]
            
            if not csv_files:
                print(f"⚠️ No CSV files found in {HDFS_SOURCE_PATH}")
                return
                
        except Exception as e:
            print(f"⚠️ Error listing files in HDFS: {str(e)}")
            return
        
        all_dataframes = []
        
        # Step 2: Process each CSV file
        for i, file_name in enumerate(csv_files):
            full_hdfs_path = f"{HDFS_SOURCE_PATH}/{file_name}"
            local_file_path = os.path.join(temp_dir, file_name)
            
            try:
                # Download file from HDFS to the temporary directory
                with open(local_file_path, 'wb') as local_file:
                    file_data = hdfs_client.open(full_hdfs_path)
                    local_file.write(file_data.read())
                
                print(f"📥 Downloaded {file_name}")
                
                # Read the CSV file
                if i == 0:
                    # For the first file, keep the header
                    df = pd.read_csv(local_file_path)
                else:
                    # For subsequent files, skip the header
                    df = pd.read_csv(local_file_path, skiprows=1)
                
                all_dataframes.append(df)
                
            except Exception as e:
                print(f"⚠️ Error processing file {file_name}: {str(e)}")
                continue
        
        if not all_dataframes:
            print("⚠️ No data was read from any CSV file")
            return
            
        # Step 3: Concatenate all dataframes
        merged_df = pd.concat(all_dataframes, ignore_index=True)
        merged_file = os.path.join(temp_dir, POWERBI_FILENAME)
        
        # Save the merged dataframe to a CSV file
        merged_df.to_csv(merged_file, index=False)
        
        print(f"✅ Successfully merged {len(all_dataframes)} CSV files")
        
        # Step 4: Create PowerBI directory in HDFS if it doesn't exist
        if not hdfs_client.exists(HDFS_POWERBI_PATH):
            hdfs_client.mkdirs(HDFS_POWERBI_PATH)
        
        # Step 5: Upload merged file to HDFS PowerBI location
        with open(merged_file, 'rb') as local_file:
            hdfs_client.create(f"{HDFS_POWERBI_PATH}/{POWERBI_FILENAME}", local_file.read(), overwrite=True)
        
        print(f"✅ Successfully uploaded merged CSV to {HDFS_POWERBI_PATH}/{POWERBI_FILENAME}")
        
        # Optional: Count lines in the merged file
        line_count = len(merged_df) + 1  # +1 for the header
        print(f"📊 Merged file contains {line_count} lines")
        
    finally:
        # Clean up temporary files
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        print(f"🧹 Cleaned up temporary directory {temp_dir}")

# Create the DAG
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

# Function to check HDFS connection
def check_hdfs_connection(**kwargs):
    import pyhdfs
    
    try:
        # Try to connect to HDFS and list root directory
        hdfs_client = pyhdfs.HdfsClient(hosts=f'{HDFS_HOST}:{HDFS_PORT}', user_name=HDFS_USER)
        root_files = hdfs_client.listdir('/')
        print(f"✅ Successfully connected to HDFS. Root directory contains: {root_files}")
        return True
    except Exception as e:
        print(f"❌ Failed to connect to HDFS: {str(e)}")
        raise

# Create a task to check if we can connect to HDFS
check_hdfs_task = PythonOperator(
    task_id='check_hdfs_connection',
    python_callable=check_hdfs_connection,
    dag=dag,
)

# Create merge CSV task
merge_csv_task = PythonOperator(
    task_id='merge_csv_files',
    python_callable=merge_csv_for_powerbi,
    dag=dag,
)

# Task dependencies
check_hdfs_task >> merge_csv_task
