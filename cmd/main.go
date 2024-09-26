package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/estensen/marketplace-pipeline/internal/aggregator"
	"github.com/estensen/marketplace-pipeline/internal/api"
	"github.com/estensen/marketplace-pipeline/internal/batch"
	"github.com/estensen/marketplace-pipeline/internal/database"
	"github.com/estensen/marketplace-pipeline/internal/loader"
	"github.com/estensen/marketplace-pipeline/internal/metrics"
	"github.com/estensen/marketplace-pipeline/internal/parser"
	"github.com/estensen/marketplace-pipeline/internal/price"
	"github.com/estensen/marketplace-pipeline/internal/storage"
	"github.com/estensen/marketplace-pipeline/internal/token"
	"github.com/estensen/marketplace-pipeline/internal/utils"
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
	clickhouseConn := database.NewClickHouseConnection(ctx)
	defer clickhouseConn.Close()

	// Initialize MinIO storage
	minioStorage := storage.SetupMinIOStorage()

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
	tokens := utils.ExtractUniqueTokens(transactions)

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
	batchJob := batch.NewBatchJob(coinAPI, clickhouseConn, minioStorage)
	err = batchJob.RunDailyBatchJob(ctx, coinIDs, date)
	if err != nil {
		log.Printf("Error running daily batch job: %v", err)
		// Continue with the rest of the pipeline even if some prices are missing
	} else {
		log.Println("Daily batch job completed successfully.")
	}

	// Fetch prices from ClickHouse
	prices, err := database.FetchPrices(ctx, clickhouseConn, coinIDs, date)
	if err != nil {
		log.Fatalf("Error fetching prices from ClickHouse: %v", err)
	}

	// Map CoinGecko IDs back to symbols
	idToSymbolMap := utils.InvertMap(symbolToIDMap)

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
	loader := loader.NewClickHouseLoader(clickhouseConn)
	err = loader.Load(aggregatedData)
	if err != nil {
		log.Fatalf("Error loading data into ClickHouse: %v", err)
	}

	log.Println("Data pipeline completed successfully.")

	// Start the API server in a separate goroutine
	apiServer := api.NewServer(aggregator, clickhouseConn)
	go api.StartServer(":8080", apiServer)

	// Fetch aggregated metrics
	metrics, err := metrics.FetchMetrics(ctx, clickhouseConn, date)
	if err != nil {
		log.Fatalf("Error fetching metrics: %v", err)
	}

	// Display metrics in terminal
	utils.DisplayMetrics(metrics)

	// Keep the main function running so it's
	// possible to query API
	select {}
}
