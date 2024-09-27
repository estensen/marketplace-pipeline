package database

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/estensen/marketplace-pipeline/internal/models"
)

// ClickHouseLoader loads aggregated data into ClickHouse.
type ClickHouseLoader struct {
	Conn clickhouse.Conn
}

// NewClickHouseLoader creates a new ClickHouseLoader.
func NewClickHouseLoader(conn clickhouse.Conn) *ClickHouseLoader {
	return &ClickHouseLoader{
		Conn: conn,
	}
}

// Load inserts aggregated data into the database.
func (l *ClickHouseLoader) Load(data []models.AggregatedData) error {
	ctx := context.Background()
	batch, err := l.Conn.PrepareBatch(ctx, "INSERT INTO marketplace_analytics (date, project_id, transaction_count, total_volume_usd)")
	if err != nil {
		return err
	}

	for _, record := range data {
		err := batch.Append(record.Date, record.ProjectID, record.TransactionCount, record.TotalVolumeUSD)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}
