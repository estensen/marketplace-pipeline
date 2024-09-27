package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/estensen/marketplace-pipeline/internal/models"
)

// ExtractUniqueTokens extracts unique currency symbols from transactions.
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

// InvertMap inverts a map of string to string.
func InvertMap(m map[string]string) map[string]string {
	inverted := make(map[string]string)
	for key, value := range m {
		inverted[value] = key
	}
	return inverted
}

// DisplayMetrics prints aggregated metrics in a table format.
func DisplayMetrics(metrics []models.AggregatedData) {
	if len(metrics) == 0 {
		fmt.Println("No metrics to display.")
		return
	}

	fmt.Printf("Marketplace Analytics for %s:\n", metrics[0].Date.Format("2006-01-02"))
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Date", "Project ID", "Transaction Count", "Total Volume USD"})

	for _, data := range metrics {
		t.AppendRow(table.Row{
			data.Date.Format("2006-01-02"),
			data.ProjectID,
			data.TransactionCount,
			fmt.Sprintf("%.2f", data.TotalVolumeUSD),
		})
	}

	t.Render()
}

// NormalizeTokenSymbol normalizes token symbols by removing extensions like ".E".
func NormalizeTokenSymbol(symbol string) string {
	return strings.Split(symbol, ".")[0]
}
