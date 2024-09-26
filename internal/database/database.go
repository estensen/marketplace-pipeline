package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// NewClickHouseConnection initializes and returns a ClickHouse connection.
func NewClickHouseConnection(ctx context.Context) clickhouse.Conn {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
		},
	})
	if err != nil {
		log.Fatalf("Error connecting to ClickHouse: %v", err)
	}

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("ClickHouse ping failed: %v", err)
	}

	log.Println("Successfully connected to ClickHouse.")
	return conn
}

func FetchPrices(ctx context.Context, conn clickhouse.Conn, coinIDs []string, date time.Time) (map[string]float64, error) {
	prices := make(map[string]float64)
	query := "SELECT token, average_price_usd FROM token_prices WHERE token IN (?) AND date = ?"

	rows, err := conn.Query(ctx, query, coinIDs, date)
	if err != nil {
		return nil, fmt.Errorf("error executing price query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var token string
		var priceUSD float64
		if err := rows.Scan(&token, &priceUSD); err != nil {
			return nil, fmt.Errorf("error scanning price row: %w", err)
		}
		prices[token] = priceUSD
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating price rows: %w", err)
	}

	return prices, nil
}
