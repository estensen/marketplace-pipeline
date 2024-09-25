.PHONY: all setup_clickhouse setup_minio run api run-api clean

all: setup run

setup: setup_clickhouse setup_minio

setup_clickhouse:
	@echo "Setting up ClickHouse..."
	@bash scripts/setup_clickhouse.sh

setup_minio:
	@echo "Setting up MinIO..."
	@bash scripts/setup_minio.sh

run:
	@echo "Running the Go application..."
	go run cmd/main.go

api:
	@echo "Starting the API server..."
	go run cmd/main.go &

run-api:
	@echo "Running the API server..."
	go run cmd/main.go &

clean:
	@echo "Cleaning up Docker containers..."
	-docker stop clickhouse-server minio-server
	-docker rm clickhouse-server minio-server
	@echo "Cleaning up completed."
