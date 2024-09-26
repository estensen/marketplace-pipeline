package utils

import (
	"fmt"

	"os"

	"github.com/estensen/marketplace-pipeline/internal/models"
	"github.com/olekukonko/tablewriter"
)

// ExtractUniqueTokens extracts unique tokens from transactions
func ExtractUniqueTokens(transactions []models.Transaction) []string {
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

// InvertMap inverts a map[string]string to map[string]string
func InvertMap(originalMap map[string]string) map[string]string {
	invertedMap := make(map[string]string)
	for key, value := range originalMap {
		invertedMap[value] = key
	}
	return invertedMap
}

// DisplayMetrics displays aggregated metrics in a table format
func DisplayMetrics(metrics []models.AggregatedData) {
	if len(metrics) == 0 {
		fmt.Println("No data available for the specified date.")
		return
	}

	fmt.Printf("Marketplace Analytics for %s:\n", metrics[0].Date.Format("2006-01-02"))

	table := tablewriter.NewWriter(os.Stdout)
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
