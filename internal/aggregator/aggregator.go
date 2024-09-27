package aggregator

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/estensen/marketplace-pipeline/internal/models"
)

// Aggregator processes transactions and calculates aggregated data.
type Aggregator struct{}

// NewAggregator creates a new instance of Aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{}
}

// Aggregate processes transactions, applying token prices to calculate aggregated data for each project.
func (a *Aggregator) Aggregate(transactions []models.Transaction, prices map[string]float64) ([]models.AggregatedData, error) {
	dataMap := make(map[string]*models.AggregatedData)

	for _, txn := range transactions {
		date := txn.Timestamp.Truncate(24 * time.Hour)
		key := date.Format("2006-01-02") + txn.ProjectID

		currencyValue, err := a.parseCurrencyValue(txn.Nums.CurrencyValueDecimal)
		if err != nil {
			log.Printf("Error parsing currency value: %v", err)
			continue
		}

		priceUSD, err := a.getPriceUSD(txn.Props.CurrencySymbol, prices)
		if err != nil {
			log.Printf("Price not found for currency symbol: %s", txn.Props.CurrencySymbol)
			continue
		}

		a.updateAggregatedData(dataMap, key, date, txn.ProjectID, currencyValue*priceUSD)
	}

	return a.collectAggregatedData(dataMap), nil
}

// parseCurrencyValue converts a currency value from string to float, normalizing it from wei.
func (a *Aggregator) parseCurrencyValue(value string) (float64, error) {
	currencyValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}
	return currencyValue / 1e18, nil
}

// getPriceUSD retrieves the USD price of a token, normalizing the symbol if necessary.
func (a *Aggregator) getPriceUSD(symbol string, prices map[string]float64) (float64, error) {
	priceUSD, found := prices[symbol]
	if !found {
		priceUSD, found = prices[normalizeSymbol(symbol)]
		if !found {
			return 0, fmt.Errorf("price not found for symbol: %s", symbol)
		}
	}
	return priceUSD, nil
}

// updateAggregatedData updates the transaction count and total volume for a specific project.
func (a *Aggregator) updateAggregatedData(dataMap map[string]*models.AggregatedData, key string, date time.Time, projectID string, totalVolumeUSD float64) {
	if aggData, exists := dataMap[key]; exists {
		aggData.TransactionCount++
		aggData.TotalVolumeUSD += totalVolumeUSD
	} else {
		dataMap[key] = &models.AggregatedData{
			Date:             date,
			ProjectID:        projectID,
			TransactionCount: 1,
			TotalVolumeUSD:   totalVolumeUSD,
		}
	}
}

// collectAggregatedData compiles the aggregated data into a slice.
func (a *Aggregator) collectAggregatedData(dataMap map[string]*models.AggregatedData) []models.AggregatedData {
	aggregatedData := make([]models.AggregatedData, 0, len(dataMap))
	for _, data := range dataMap {
		aggregatedData = append(aggregatedData, *data)
	}
	return aggregatedData
}

// normalizeSymbol normalizes a token symbol by converting it to uppercase and splitting on periods.
func normalizeSymbol(symbol string) string {
	return strings.ToUpper(strings.Split(symbol, ".")[0])
}

// CalculateMetrics fetches aggregated metrics from ClickHouse.
func (a *Aggregator) CalculateMetrics(conn clickhouse.Conn, date time.Time) ([]models.AggregatedData, error) {
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
