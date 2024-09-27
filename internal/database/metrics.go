package database

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/estensen/marketplace-pipeline/internal/models"
)

// FetchMetrics retrieves aggregated metrics from ClickHouse for the given date.
func FetchMetrics(ctx context.Context, conn clickhouse.Conn, date time.Time) ([]models.AggregatedData, error) {
	var metrics []models.AggregatedData
	query := `
        SELECT 
            date, 
            project_id, 
            transaction_count, 
            total_volume_usd 
        FROM marketplace_analytics 
        WHERE date = ?
        `

	if err := conn.Select(ctx, &metrics, query, date); err != nil {
		return nil, fmt.Errorf("error executing query '%s': %v", query, err)
	}

	return metrics, nil
}
