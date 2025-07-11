#!/bin/bash

# Script to download CSV files from HDFS to local machine

# Configuration
HDFS_DIR="/powerBI_ready"
LOCAL_DIR="./downloaded_csv_files"
HDFS_FILE="$HDFS_DIR/credit_card_transactions_merged.csv"
LOCAL_FILE="$LOCAL_DIR/credit_card_transactions_merged.csv"

# Create local directory if it doesn't exist
mkdir -p "$LOCAL_DIR"

echo "🔍 Downloading merged CSV file from HDFS..."
echo "Source: $HDFS_FILE"
echo "Destination: $LOCAL_FILE"
echo

# Check if the file exists in HDFS
echo "==== Checking if file exists in HDFS ===="
if ! docker exec namenode hdfs dfs -test -e $HDFS_FILE; then
    echo "❌ Error: File $HDFS_FILE does not exist in HDFS"
    echo "Available files in $HDFS_DIR:"
    docker exec namenode hdfs dfs -ls $HDFS_DIR
    exit 1
fi

echo "✅ File found in HDFS"

# Get file info
echo "==== File information ===="
docker exec namenode hdfs dfs -ls $HDFS_FILE

echo
echo "==== Downloading file ===="
echo "Downloading credit_card_transactions_merged.csv..."

# Download the file
docker exec namenode hdfs dfs -get $HDFS_FILE /tmp/credit_card_transactions_merged.csv
if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to download file from HDFS"
    exit 1
fi

# Copy from container to local machine
docker cp namenode:/tmp/credit_card_transactions_merged.csv "$LOCAL_FILE"
if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to copy file to local machine"
    exit 1
fi

# Clean up temporary file in container
docker exec namenode rm /tmp/credit_card_transactions_merged.csv

echo
echo "✅ Download completed!"
echo "File saved to: $LOCAL_FILE"
echo "You can open this file in your preferred spreadsheet application (Excel, LibreOffice Calc, etc.) or text editor."

# Show file information
echo
echo "==== Downloaded file information ===="
if [ -f "$LOCAL_FILE" ]; then
    echo "File: $LOCAL_FILE"
    echo "Size: $(ls -lh "$LOCAL_FILE" | awk '{print $5}')"
    echo "Rows: $(wc -l < "$LOCAL_FILE") lines"
    echo
    echo "First few lines:"
    head -5 "$LOCAL_FILE"
else
    echo "❌ Error: File was not downloaded successfully"
    exit 1
fi
