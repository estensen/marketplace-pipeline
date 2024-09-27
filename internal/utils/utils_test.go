package utils

import (
	"testing"
	"time"

	"github.com/estensen/marketplace-pipeline/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestExtractUniqueTokens(t *testing.T) {
	transactions := []models.Transaction{
		{
			Timestamp: time.Now(),
			Event:     "event1",
			ProjectID: "proj1",
			Props: models.Props{
				CurrencySymbol:    "BTC",
				ChainID:           "chain1",
				CollectionAddress: "0x123",
				CurrencyAddress:   "0xabc",
			},
			Nums: models.Nums{
				CurrencyValueDecimal: "1.0",
			},
		},
		{
			Timestamp: time.Now(),
			Event:     "event2",
			ProjectID: "proj2",
			Props: models.Props{
				CurrencySymbol:    "ETH",
				ChainID:           "chain2",
				CollectionAddress: "0x456",
				CurrencyAddress:   "0xdef",
			},
			Nums: models.Nums{
				CurrencyValueDecimal: "2.0",
			},
		},
		{
			Timestamp: time.Now(),
			Event:     "event3",
			ProjectID: "proj3",
			Props: models.Props{
				CurrencySymbol:    "BTC",
				ChainID:           "chain1",
				CollectionAddress: "0x123",
				CurrencyAddress:   "0xabc",
			},
			Nums: models.Nums{
				CurrencyValueDecimal: "3.0",
			},
		},
	}

	expected := []string{"BTC", "ETH"}

	result := ExtractUniqueTokens(transactions)

	assert.ElementsMatch(t, expected, result, "ExtractUniqueTokens did not return expected unique tokens")
}

func TestInvertMap(t *testing.T) {
	input := map[string]string{
		"MATIC": "matic-network",
		"USDC":  "usd-coin",
	}

	expected := map[string]string{
		"matic-network": "MATIC",
		"usd-coin":      "USDC",
	}

	result := InvertMap(input)

	assert.Equal(t, expected, result, "InvertMap did not return the expected inverted map")
}

func TestDisplayMetrics(t *testing.T) {
	metrics := []models.AggregatedData{
		{
			Date:             time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC),
			ProjectID:        "137",
			TransactionCount: 10,
			TotalVolumeUSD:   1000.0,
		},
		{
			Date:             time.Date(2024, 4, 2, 0, 0, 0, 0, time.UTC),
			ProjectID:        "137",
			TransactionCount: 5,
			TotalVolumeUSD:   500.0,
		},
	}

	// Since DisplayMetrics prints to stdout, we'll just ensure it runs without panic.
	assert.NotPanics(t, func() {
		DisplayMetrics(metrics)
	}, "DisplayMetrics should not panic")
}

func TestNormalizeTokenSymbol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		symbol     string
		normalized string
	}{
		{
			symbol:     "USDC.E",
			normalized: "USDC",
		},
		{
			symbol:     "SFL",
			normalized: "SFL",
		},
		{
			symbol:     "matic",
			normalized: "MATIC",
		},
		{
			symbol:     "usdc",
			normalized: "USDC",
		},
	}

	for _, tc := range tests {
		t.Run(tc.symbol, func(t *testing.T) {
			normalized := NormalizeTokenSymbol(tc.symbol)
			assert.Equal(t, tc.normalized, normalized)
		})
	}
}
