#!/bin/bash
set -e

echo "========================================"
echo "Lab5: Apache Iceberg with PySpark"
echo "========================================"

# Setup writable directories
export HOME=/tmp
export PYTHONUSERBASE=/tmp/pip_packages
export PATH=$PYTHONUSERBASE/bin:$PATH
mkdir -p /tmp/pip_packages

echo "Installing Python dependencies..."
pip install --user --quiet pandas openpyxl

mkdir -p /app/warehouse

ICEBERG_VERSION="1.7.1"
SPARK_VERSION="3.5"
ICEBERG_JAR="iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar"
ICEBERG_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_2.12/${ICEBERG_VERSION}/${ICEBERG_JAR}"

if [ ! -f "/tmp/${ICEBERG_JAR}" ]; then
    echo "Downloading Iceberg runtime..."
    curl -sL "${ICEBERG_URL}" -o "/tmp/${ICEBERG_JAR}"
fi

echo "Starting Spark application..."

/opt/spark/bin/spark-submit \
    --master local[*] \
    --jars "/tmp/${ICEBERG_JAR}" \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=/app/warehouse \
    --conf spark.sql.defaultCatalog=local \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=2g \
    /app/src/lab5/main.py

echo ""
echo "========================================"
echo "Execution completed!"
echo "========================================"
