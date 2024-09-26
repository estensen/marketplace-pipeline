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
