# marketplace-pipeline

Pipeline for marketplace analytics.

## Project Overview

This project processes transaction data from a CSV file, fetches currency conversion rates from the CoinGecko API, calculates daily marketplace volume and transaction counts, and loads the data into ClickHouse for analytics. The entire pipeline is built using Go and can be run locally.

## Prerequisites

- **Go**: Version 1.23 or higher
- **Docker**: For running ClickHouse and MinIO
```

### Run Pipeline Locally

```bash
bash scripts/setup_clickhouse.sh
bash scripts/setup_minio.sh
go run cmd/main.go
...
Marketplace Analytics for 2024-04-02:
+------------+------------+-------------------+------------------+
| Date       | Project ID | Transaction Count | Total Volume USD |
+------------+------------+-------------------+------------------+
| 2024-04-02 | 0          | 104               | 38.91            |
| 2024-04-02 | 1609       | 9                 | 21.14            |
| 2024-04-02 | 4974       | 97                | 3.69             |
+------------+------------+-------------------+------------------+
```
