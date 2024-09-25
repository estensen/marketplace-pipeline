#!/bin/bash

# Configuration
CONTAINER_NAME="minio-server"
IMAGE_NAME="minio/minio"
PORT_API=9001   # Changed from 9000 to 9001 to avoid conflict
PORT_CONSOLE=9002
ACCESS_KEY="minioadmin"
SECRET_KEY="minioadmin"
BUCKET_NAME="currency-data"
SLEEP_TIME=10  # Increased sleep time to ensure MinIO is fully ready

# Function to delete existing MinIO container
delete_container() {
    if [ "$(docker ps -aq -f name=${CONTAINER_NAME})" ]; then
        echo "Stopping and removing existing MinIO container..."
        docker stop ${CONTAINER_NAME}
        docker rm ${CONTAINER_NAME}
    else
        echo "No existing MinIO container found. Proceeding to create a new one."
    fi
}

# Function to start a new MinIO container
start_container() {
    echo "Starting a new MinIO container..."
    docker run -d --name ${CONTAINER_NAME} \
        -p ${PORT_API}:9000 \
        -p ${PORT_CONSOLE}:9001 \
        -e "MINIO_ACCESS_KEY=${ACCESS_KEY}" \
        -e "MINIO_SECRET_KEY=${SECRET_KEY}" \
        -v minio_data:/data \
        ${IMAGE_NAME} server /data --console-address ":9001"
}

# Function to check if MinIO is running
check_minio_running() {
    if [ "$(docker ps -q -f name=${CONTAINER_NAME})" ]; then
        echo "MinIO is running successfully on ports ${PORT_API} (API) and ${PORT_CONSOLE} (Console)."
        return 0
    else
        echo "Failed to start MinIO."
        return 1
    fi
}

# Function to wait until MinIO is ready to accept connections
wait_for_minio() {
    echo "Waiting for MinIO server to be ready..."
    local retries=60  # Increased retries for better reliability
    while ! curl -s "http://localhost:${PORT_CONSOLE}/minio/health/ready" | grep "OK" &>/dev/null; do
        sleep 1
        retries=$((retries-1))
        if [ $retries -le 0 ]; then
            echo "MinIO did not become ready in time."
            exit 1
        fi
    done
    echo "MinIO server is ready."
}

# Function to create bucket using MinIO Client in Docker
create_bucket() {
    echo "Creating bucket '${BUCKET_NAME}' in MinIO..."
    
    # Use Docker to run mc commands without requiring mc installed on host
    docker run --rm --network container:${CONTAINER_NAME} \
        -e MINIO_ACCESS_KEY=${ACCESS_KEY} \
        -e MINIO_SECRET_KEY=${SECRET_KEY} \
        minio/mc \
        alias set myminio http://localhost:9000 ${ACCESS_KEY} ${SECRET_KEY} --api S3v4
    
    docker run --rm --network container:${CONTAINER_NAME} \
        -e MINIO_ACCESS_KEY=${ACCESS_KEY} \
        -e MINIO_SECRET_KEY=${SECRET_KEY} \
        minio/mc \
        mb myminio/${BUCKET_NAME} 2>/dev/null || echo "Bucket '${BUCKET_NAME}' already exists."
    
    echo "Bucket '${BUCKET_NAME}' is ready."
}

# Main Script Execution

# Step 1: Delete existing container if it exists
delete_container

# Step 2: Start a new MinIO container
start_container

# Step 3: Verify if MinIO is running
if check_minio_running; then
    # Step 4: Wait for MinIO to be fully ready
    #wait_for_minio

    # Step 5: Create the necessary bucket
    create_bucket
else
    echo "Exiting script due to MinIO startup failure."
    exit 1
fi

echo "MinIO setup completed successfully."