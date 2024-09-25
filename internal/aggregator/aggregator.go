package aggregator

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/estensen/marketplace-pipeline/internal/models"
)

type Aggregator interface {
	Aggregate(transactions []models.Transaction, prices map[string]float64) ([]models.AggregatedData, error)
	CalculateMetrics(conn driver.Conn, date time.Time) ([]models.AggregatedData, error)
}

type SimpleAggregator struct{}

func NewAggregator() *SimpleAggregator {
	return &SimpleAggregator{}
}

func (a *SimpleAggregator) Aggregate(transactions []models.Transaction, prices map[string]float64) ([]models.AggregatedData, error) {
	dataMap := make(map[string]*models.AggregatedData)

	for _, txn := range transactions {
		date := txn.Timestamp.Truncate(24 * time.Hour)
		key := date.Format("2006-01-02") + txn.ProjectID

		// Convert currency value to float
		currencyValue, err := strconv.ParseFloat(txn.Nums.CurrencyValueDecimal, 64)
		if err != nil {
			log.Printf("Error parsing currency value: %v", err)
			// Skip when unable to parse
			continue
		}

		// Convert from wei to the token's unit (assuming 18 decimals for Ethereum-based tokens)
		currencyValue = currencyValue / 1e18

		// Attempt to get USD price for the token
		priceUSD, found := prices[txn.Props.CurrencySymbol]
		if !found {
			// Attempt to normalize the symbol and retry
			normalizedSymbol := normalizeSymbol(txn.Props.CurrencySymbol)
			priceUSD, found = prices[normalizedSymbol]
			if !found {
				log.Printf("Price not found for currency symbol: %s (normalized: %s)", txn.Props.CurrencySymbol, normalizedSymbol)
				// If price still not found, skip this transaction
				continue
			}
		}

		// Only now create or update the aggregated data entry after validations
		aggData, exists := dataMap[key]
		if !exists {
			aggData = &models.AggregatedData{
				Date:             date,
				ProjectID:        txn.ProjectID,
				TransactionCount: 0,
				TotalVolumeUSD:   0,
			}
			dataMap[key] = aggData
		}

		// Increment transaction count and calculate total volume in USD
		aggData.TransactionCount++
		aggData.TotalVolumeUSD += currencyValue * priceUSD
	}

	// Collect and return aggregated data
	var aggregatedData []models.AggregatedData
	for _, data := range dataMap {
		aggregatedData = append(aggregatedData, *data)
	}

	return aggregatedData, nil
}

func normalizeSymbol(symbol string) string {
	return strings.ToUpper(strings.Split(symbol, ".")[0])
}

// CalculateMetrics fetches aggregated metrics from ClickHouse.
func (a *SimpleAggregator) CalculateMetrics(conn clickhouse.Conn, date time.Time) ([]models.AggregatedData, error) {
	query := `
    SELECT
        date,
        project_id,
        SUM(transaction_count) AS transaction_count,
        SUM(total_volume_usd) AS total_volume_usd
    FROM marketplace_analytics
    WHERE date = ?
    GROUP BY date, project_id
    `

	var aggregatedData []models.AggregatedData

	rows, err := conn.Query(context.Background(), query, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var data models.AggregatedData
		if err := rows.Scan(&data.Date, &data.ProjectID, &data.TransactionCount, &data.TotalVolumeUSD); err != nil {
			return nil, err
		}
		aggregatedData = append(aggregatedData, data)
	}

	return aggregatedData, nil
}
