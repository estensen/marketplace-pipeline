#!/bin/bash

# Configuration
CONTAINER_NAME="clickhouse-server"
IMAGE_NAME="clickhouse/clickhouse-server"
ULIMIT_NOFILE="262144:262144"
PORT_TCP=9000
PORT_HTTP=8123
SLEEP_TIME=10  # Increased sleep time to ensure ClickHouse is fully ready

# Function to delete existing ClickHouse container
delete_container() {
    if [ "$(docker ps -aq -f name=${CONTAINER_NAME})" ]; then
        echo "Stopping and removing existing ClickHouse container..."
        docker stop ${CONTAINER_NAME}
        docker rm ${CONTAINER_NAME}
    else
        echo "No existing ClickHouse container found. Proceeding to create a new one."
    fi
}

# Function to start a new ClickHouse container
start_container() {
    echo "Starting a new ClickHouse container..."
    docker run -d --name ${CONTAINER_NAME} --ulimit nofile=${ULIMIT_NOFILE} \
    -p ${PORT_TCP}:${PORT_TCP} -p ${PORT_HTTP}:${PORT_HTTP} ${IMAGE_NAME}
}

# Function to check if ClickHouse is running
check_clickhouse_running() {
    if [ "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
        echo "ClickHouse is running successfully on ports ${PORT_TCP} (TCP) and ${PORT_HTTP} (HTTP)."
        return 0
    else
        echo "Failed to start ClickHouse."
        return 1
    fi
}

# Function to wait until ClickHouse is ready to accept connections
wait_for_clickhouse() {
    echo "Waiting for ClickHouse server to be ready..."
    local retries=30
    while ! docker exec ${CONTAINER_NAME} clickhouse-client --query "SELECT 1" &>/dev/null; do
        sleep 1
        retries=$((retries-1))
        if [ $retries -le 0 ]; then
            echo "ClickHouse did not become ready in time."
            exit 1
        fi
    done
    echo "ClickHouse server is ready."
}

# Function to create or update tables
create_tables() {
    echo "Creating or updating ClickHouse tables..."

    # Create or replace 'marketplace_analytics' table
    docker exec -i ${CONTAINER_NAME} clickhouse-client --query="
    CREATE TABLE IF NOT EXISTS marketplace_analytics (
        date Date,
        project_id String,
        transaction_count UInt64,
        total_volume_usd Float64
    ) ENGINE = MergeTree()
    ORDER BY (date, project_id);
    "

    # Create or replace 'token_prices' table
    docker exec -i ${CONTAINER_NAME} clickhouse-client --query="
    CREATE TABLE IF NOT EXISTS token_prices (
        token String,
        date Date,
        average_price_usd Float64
    ) ENGINE = MergeTree()
    ORDER BY (token, date);
    "

    echo "Tables 'marketplace_analytics' and 'token_prices' have been created or verified."
}

# Main Script Execution

# Step 1: Delete existing container if it exists
delete_container

# Step 2: Start a new ClickHouse container
start_container

# Step 3: Verify if ClickHouse is running
if check_clickhouse_running; then
    # Step 4: Wait for ClickHouse to be fully ready
    wait_for_clickhouse

    # Step 5: Create or update the necessary tables
    create_tables
else
    echo "Exiting script due to ClickHouse startup failure."
    exit 1
fi

echo "ClickHouse setup completed successfully."