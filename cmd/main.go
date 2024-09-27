package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/estensen/marketplace-pipeline/internal/aggregator"
	"github.com/estensen/marketplace-pipeline/internal/api"
	"github.com/estensen/marketplace-pipeline/internal/database"
	"github.com/estensen/marketplace-pipeline/internal/parser"
	"github.com/estensen/marketplace-pipeline/internal/price"
	"github.com/estensen/marketplace-pipeline/internal/storage"
	"github.com/estensen/marketplace-pipeline/internal/utils"
)

func main() {
	// Initialize logging with timestamp and file info
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ctx := context.Background()

	// Use a fixed date matching the sample data
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
	symbolToCoinID, err := coinAPI.FetchCoinsList()
	if err != nil {
		log.Fatalf("Error fetching coin list: %v", err)
	}

	// Parse CSV file to get the transactions
	csvParser := parser.NewCSVParser()
	transactions, err := csvParser.ParseCSV("data/sample.csv")
	if err != nil {
		log.Fatalf("Error parsing CSV: %v", err)
	}

	// Extract unique tokens from the transactions
	tokens := utils.ExtractUniqueTokens(transactions)

	// Map tokens to CoinGecko IDs
	coinIDs := []string{}
	for _, tokenSymbol := range tokens {
		// Normalize token symbol
		normalizedToken := utils.NormalizeTokenSymbol(strings.ToUpper(tokenSymbol))
		if coinID, found := symbolToCoinID[normalizedToken]; found {
			coinIDs = append(coinIDs, coinID)
		} else {
			log.Printf("No CoinGecko ID found for token: %s", tokenSymbol)
		}
	}

	if len(coinIDs) == 0 {
		log.Println("No valid CoinGecko IDs found, exiting.")
		return
	}

	// Run batch job to fetch and store token prices
	batchJob := database.NewBatchJob(coinAPI, clickhouseConn, minioStorage)
	err = batchJob.RunDailyBatchJob(ctx, coinIDs, date)
	if err != nil {
		log.Printf("Error running daily batch job: %v", err)
	} else {
		log.Println("Daily batch job completed successfully.")
	}

	// Fetch prices from ClickHouse
	prices, err := database.FetchPrices(ctx, clickhouseConn, coinIDs, date)
	if err != nil {
		log.Fatalf("Error fetching prices from ClickHouse: %v", err)
	}

	// Map CoinGecko IDs back to symbols
	coinIDToSymbol := utils.InvertMap(symbolToCoinID)

	// Map prices to symbols
	symbolPrices := make(map[string]float64)
	for coinID, priceUSD := range prices {
		symbol := coinIDToSymbol[coinID]
		symbolPrices[symbol] = priceUSD
	}

	// Aggregate data
	agg := aggregator.NewAggregator()
	aggregatedData, err := agg.Aggregate(transactions, symbolPrices)
	if err != nil {
		log.Fatalf("Error aggregating data: %v", err)
	}

	// Load aggregated data into ClickHouse
	dataLoader := database.NewClickHouseLoader(clickhouseConn)
	err = dataLoader.Load(aggregatedData)
	if err != nil {
		log.Fatalf("Error loading data into ClickHouse: %v", err)
	}

	log.Println("Data pipeline completed successfully.")

	// Start the API server in a separate goroutine
	apiServer := api.NewServer(agg, clickhouseConn)
	go api.StartServer(":8080", apiServer)

	// Fetch aggregated metrics
	aggregatedMetrics, err := database.FetchMetrics(ctx, clickhouseConn, date)
	if err != nil {
		log.Fatalf("Error fetching metrics: %v", err)
	}

	// Display metrics in terminal
	utils.DisplayMetrics(aggregatedMetrics)

	// Keep the main function running so it's possible to query the API
	select {}
}
