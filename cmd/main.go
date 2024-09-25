package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/estensen/marketplace-pipeline/internal/aggregator"
	"github.com/estensen/marketplace-pipeline/internal/api"
	"github.com/estensen/marketplace-pipeline/internal/batch"
	"github.com/estensen/marketplace-pipeline/internal/loader"
	"github.com/estensen/marketplace-pipeline/internal/models"
	"github.com/estensen/marketplace-pipeline/internal/parser"
	"github.com/estensen/marketplace-pipeline/internal/price"
	"github.com/estensen/marketplace-pipeline/internal/storage"
	"github.com/estensen/marketplace-pipeline/internal/token"
	"github.com/olekukonko/tablewriter"
)

func main() {
	// Initialize logging with timestamp
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	dateStr := "2024-04-02"
	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		log.Fatalf("Error parsing date: %v", err)
	}

	// Set up ClickHouse connection
	conn := setupClickhouseConnection(ctx)
	defer conn.Close()

	// Initialize MinIO storage
	minioStorage := setupMinIOStorage()

	// Initialize CoinGecko API
	coinAPI := price.NewCoinGeckoAPI()

	// Fetch coin list
	symbolToIDMap, err := coinAPI.FetchCoinsList()
	if err != nil {
		log.Fatalf("Error fetching coin list: %v", err)
	}

	// Parse CSV file to get the transactions
	parser := parser.NewCSVParser()
	transactions, err := parser.ParseCSV("data/sample.csv")
	if err != nil {
		log.Fatalf("Error parsing CSV: %v", err)
	}

	// Extract unique tokens from the transactions
	tokens := extractUniqueTokens(transactions)

	// Map tokens to CoinGecko IDs
	coinIDs := []string{}
	for _, t := range tokens {
		// Normalize token using the new token package
		normalizedToken := token.NormalizeTokenSymbol(strings.ToUpper(t))
		if coinID, found := symbolToIDMap[normalizedToken]; found {
			coinIDs = append(coinIDs, coinID)
		} else {
			log.Printf("No CoinGecko ID found for token: %s", t)
		}
	}

	if len(coinIDs) == 0 {
		log.Println("No valid CoinGecko IDs found, exiting.")
		return
	}

	// Run batch job to fetch and store token prices
	batchJob := batch.NewBatchJob(coinAPI, conn, minioStorage)
	err = batchJob.RunDailyBatchJob(ctx, coinIDs, date)
	if err != nil {
		log.Printf("Error running daily batch job: %v", err)
		// Continue with the rest of the pipeline even if some prices are missing
	} else {
		log.Println("Daily batch job completed successfully.")
	}

	// Fetch prices from ClickHouse
	prices, err := fetchPrices(ctx, conn, coinIDs, date)
	if err != nil {
		log.Fatalf("Error fetching prices from ClickHouse: %v", err)
	}

	// Map CoinGecko IDs back to symbols
	idToSymbolMap := invertMap(symbolToIDMap)

	// Map prices to symbols
	symbolPrices := make(map[string]float64)
	for coinID, priceUSD := range prices {
		symbol := idToSymbolMap[coinID]
		symbolPrices[symbol] = priceUSD
	}

	// Aggregate data
	aggregator := aggregator.NewAggregator()
	aggregatedData, err := aggregator.Aggregate(transactions, symbolPrices)
	if err != nil {
		log.Fatalf("Error aggregating data: %v", err)
	}

	// Load aggregated data into ClickHouse
	loader := loader.NewClickHouseLoader(conn)
	err = loader.Load(aggregatedData)
	if err != nil {
		log.Fatalf("Error loading data into ClickHouse: %v", err)
	}

	log.Println("Data pipeline completed successfully.")

	// Start the API server in a separate goroutine
	apiServer := api.NewServer(aggregator, conn)
	go api.StartServer(":8080", apiServer)

	// Fetch aggregated metrics
	metrics, err := fetchMetrics(ctx, conn, date)
	if err != nil {
		log.Fatalf("Error fetching metrics: %v", err)
	}

	// Display metrics in terminal
	displayMetrics(metrics)

	// Keep the main function running
	select {}
}

// setupClickhouseConnection initializes and returns a ClickHouse connection.
func setupClickhouseConnection(ctx context.Context) clickhouse.Conn {
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

// setupMinIOStorage initializes and returns MinIO storage.
func setupMinIOStorage() *storage.MinIOStorage {
	endpoint := "localhost:9001"
	accessKey := "minioadmin"
	secretKey := "minioadmin"
	bucket := "currency-data"
	useSSL := false

	storage, err := storage.NewMinIOStorage(endpoint, accessKey, secretKey, bucket, useSSL)
	if err != nil {
		log.Fatalf("Failed to initialize MinIO storage: %v", err)
	}
	log.Println("Initialized MinIO storage.")
	return storage
}

func fetchPrices(ctx context.Context, conn clickhouse.Conn, coinIDs []string, date time.Time) (map[string]float64, error) {
	prices := make(map[string]float64)
	for _, coinID := range coinIDs {
		var priceUSD float64
		query := "SELECT average_price_usd FROM token_prices WHERE token = ? AND date = ?"
		if err := conn.QueryRow(ctx, query, coinID, date).Scan(&priceUSD); err != nil {
			return nil, fmt.Errorf("error fetching price for token %s: %v", coinID, err)
		}
		prices[coinID] = priceUSD
	}
	return prices, nil
}

// Extract unique tokens from transactions
func extractUniqueTokens(transactions []models.Transaction) []string {
	tokenSet := make(map[string]struct{})
	for _, txn := range transactions {
		tokenSet[txn.Props.CurrencySymbol] = struct{}{}
	}

	tokens := make([]string, 0, len(tokenSet))
	for token := range tokenSet {
		tokens = append(tokens, token)
	}
	return tokens
}

func invertMap(originalMap map[string]string) map[string]string {
	invertedMap := make(map[string]string)
	for key, value := range originalMap {
		invertedMap[value] = key
	}
	return invertedMap
}

func fetchMetrics(ctx context.Context, conn clickhouse.Conn, date time.Time) ([]models.AggregatedData, error) {
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

func displayMetrics(metrics []models.AggregatedData) {
	if len(metrics) == 0 {
		fmt.Println("No data available for the specified date.")
		return
	}

	fmt.Printf("Marketplace Analytics for %s:\n", metrics[0].Date.Format("2006-01-02"))

	table := tablewriter.NewWriter(os.Stdout)
	// Update the table headers to match the correct fields
	table.SetHeader([]string{"Date", "Project ID", "Transaction Count", "Total Volume USD"})
	table.SetBorder(true)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)

	for _, metric := range metrics {
		row := []string{
			metric.Date.Format("2006-01-02"),
			metric.ProjectID,
			fmt.Sprintf("%d", metric.TransactionCount),
			fmt.Sprintf("%.2f", metric.TotalVolumeUSD),
		}
		table.Append(row)
	}

	table.Render()
}
