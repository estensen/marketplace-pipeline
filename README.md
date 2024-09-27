# marketplace-pipeline

Pipeline for marketplace analytics.

## Overview

Build a data pipeline that extracts transaction data from a CSV file, normalizes and aggregates the data, and stores the results in a ClickHouse database. The pipeline also fetches token prices from the CoinGecko API and provides an API endpoint for data visualization.

## Prerequisites

- **Go**: Version 1.23 or higher
- **Docker**: For running ClickHouse and MinIO
```

### Run Pipeline Locally

```bash
$ make all
...
Marketplace Analytics for 2024-04-02:
+------------+------------+-------------------+------------------+
| Date       | Project ID | Transaction Count | Total Volume USD |
+------------+------------+-------------------+------------------+
| 2024-04-02 | 0          | 104               | 38.91            |
| 2024-04-02 | 1609       | 9                 | 21.14            |
| 2024-04-02 | 4974       | 97                | 3.69             |
+------------+------------+-------------------+------------------+

$ curl "http://localhost:8080/metrics?date=2024-04-02"
[{"Date":"2024-04-02T00:00:00Z","ProjectID":"0","TransactionCount":104,"TotalVolumeUSD":38.90877259486244},{"Date":"2024-04-02T00:00:00Z","ProjectID":"4974","TransactionCount":97,"TotalVolumeUSD":3.686094245830159},{"Date":"2024-04-02T00:00:00Z","ProjectID":"1609","TransactionCount":9,"TotalVolumeUSD":21.13694068638653}]
```
