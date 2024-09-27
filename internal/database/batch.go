package database

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/estensen/marketplace-pipeline/internal/price"
	"github.com/estensen/marketplace-pipeline/internal/storage"
)

// BatchJob represents a job to fetch and store token prices.
type BatchJob struct {
	CoinAPI price.CoinAPI
	Conn    clickhouse.Conn
	Storage storage.Storage
}

// NewBatchJob creates a new BatchJob.
func NewBatchJob(coinAPI price.CoinAPI, conn clickhouse.Conn, storage storage.Storage) *BatchJob {
	return &BatchJob{
		CoinAPI: coinAPI,
		Conn:    conn,
		Storage: storage,
	}
}

// RunDailyBatchJob fetches prices and stores them in ClickHouse and MinIO.
func (b *BatchJob) RunDailyBatchJob(ctx context.Context, coinIDs []string, date time.Time) error {
	// Check if prices for the given date already exist in ClickHouse
	var count uint64
	query := "SELECT COUNT(*) FROM token_prices WHERE date = ?"
	if err := b.Conn.QueryRow(ctx, query, date).Scan(&count); err != nil {
		return fmt.Errorf("error querying ClickHouse: %v", err)
	}

	if count > 0 {
		return fmt.Errorf("prices for the date %s already exist, skipping batch insertion", date.Format("2006-01-02"))
	}

	// Fetch prices for all tokens
	prices, err := b.CoinAPI.GetHistoricalPrices(coinIDs, date)
	if err != nil {
		return fmt.Errorf("error fetching prices: %w", err)
	}

	// Prepare batch insertion
	batch, err := b.Conn.PrepareBatch(ctx, "INSERT INTO token_prices (token, date, average_price_usd)")
	if err != nil {
		return fmt.Errorf("error preparing ClickHouse batch: %w", err)
	}

	// Insert each token's price into ClickHouse
	for coinID, priceUSD := range prices {
		err := batch.Append(coinID, date, priceUSD)
		if err != nil {
			return fmt.Errorf("error appending to ClickHouse batch: %w", err)
		}
	}

	// Send the batch to ClickHouse
	if err := batch.Send(); err != nil {
		return fmt.Errorf("error sending batch to ClickHouse: %w", err)
	}

	// Store the prices in MinIO as a CSV file
	err = b.StorePricesInMinIO(prices, date)
	if err != nil {
		return fmt.Errorf("error storing prices in MinIO: %w", err)
	}

	return nil
}

// StorePricesInMinIO saves the token prices to MinIO in CSV format.
func (b *BatchJob) StorePricesInMinIO(prices map[string]float64, date time.Time) error {
	// Create a CSV in memory
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write the CSV header
	if err := writer.Write([]string{"token", "average_price_usd"}); err != nil {
		return fmt.Errorf("error writing CSV header: %w", err)
	}

	// Write the price records
	for token, price := range prices {
		record := []string{token, fmt.Sprintf("%.8f", price)}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("error writing CSV record: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("error flushing CSV writer: %w", err)
	}

	// Define the object name
	objectName := fmt.Sprintf("prices-%s.csv", date.Format("2006-01-02"))

	// Upload the CSV to MinIO
	err := b.Storage.UploadFile(objectName, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("error uploading file to MinIO: %w", err)
	}

	return nil
}
