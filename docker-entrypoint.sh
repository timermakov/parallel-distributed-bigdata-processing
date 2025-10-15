#!/bin/bash
set -e

# Create writable directories for pip installations and kaggle
mkdir -p /tmp/pip_packages
mkdir -p /tmp/kaggle_home

# Set up environment variables for user installations
export PYTHONUSERBASE=/tmp/pip_packages
export PATH=$PYTHONUSERBASE/bin:$PATH

# Set custom kaggle home directory to avoid permission issues
export KAGGLE_CONFIG_DIR=/tmp/kaggle_home
export HOME=/tmp

# Install kagglehub and any other required packages
echo "Installing kagglehub..."
pip install --user kagglehub

# Set PYTHONPATH to include site-packages and app directory
export PYTHONPATH=/app:/tmp/pip_packages/lib/python*/site-packages:$PYTHONPATH

# Execute the provided command
if [ "$1" = "spark-submit" ]; then
    echo "Running Spark submit with provided arguments..."
    exec /opt/spark/bin/spark-submit "${@:2}"
else
    exec "$@"
fi