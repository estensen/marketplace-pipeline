package aggregator

import (
	"testing"
	"time"

	"github.com/estensen/marketplace-pipeline/internal/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		transactions  []models.Transaction
		prices        map[string]float64
		expected      []models.AggregatedData
		expectedError bool
	}{
		{
			name: "Valid aggregation with all prices available",
			transactions: []models.Transaction{
				{
					Timestamp: time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC),
					Props: models.Props{
						CurrencySymbol: "SFL",
						ChainID:        "137",
					},
					Nums: models.Nums{
						CurrencyValueDecimal: "1316777549196586000",
					},
				},
				{
					Timestamp: time.Date(2024, 4, 2, 0, 0, 0, 0, time.UTC),
					Props: models.Props{
						CurrencySymbol: "MATIC",
						ChainID:        "137",
					},
					Nums: models.Nums{
						CurrencyValueDecimal: "700000000000000000",
					},
				},
			},
			prices: map[string]float64{
				"MATIC": 0.408257,
				"SFL":   1.23,
			},
			expected: []models.AggregatedData{
				{
					Date:             time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC),
					TransactionCount: 1,
					TotalVolumeUSD:   (1.316777549196586) * 1.23, // Corrected for SFL conversion
				},
				{
					Date:             time.Date(2024, 4, 2, 0, 0, 0, 0, time.UTC),
					TransactionCount: 1,
					TotalVolumeUSD:   0.7 * 0.408257, // Corrected for MATIC conversion
				},
			},
			expectedError: false,
		},
		{
			name: "Missing price for a currency",
			transactions: []models.Transaction{
				{
					Timestamp: time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC),
					Props: models.Props{
						CurrencySymbol: "USDC",
					},
					Nums: models.Nums{
						CurrencyValueDecimal: "1000000",
					},
				},
			},
			prices: map[string]float64{
				"MATIC": 0.408257,
			},
			expected:      nil,
			expectedError: false, // Transaction skipped due to missing price
		},
		{
			name: "Invalid currency value",
			transactions: []models.Transaction{
				{
					Timestamp: time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC),
					Props: models.Props{
						CurrencySymbol: "MATIC",
					},
					Nums: models.Nums{
						CurrencyValueDecimal: "invalid_value",
					},
				},
			},
			prices: map[string]float64{
				"MATIC": 0.408257,
			},
			expected:      nil,
			expectedError: false, // Error logged but aggregation continues
		},
	}

	aggregator := NewAggregator()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregatedData, err := aggregator.Aggregate(tt.transactions, tt.prices)
			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.expected != nil {
				require.Len(t, aggregatedData, len(tt.expected))
				for i, agg := range aggregatedData {
					assert.Equal(t, tt.expected[i].TransactionCount, agg.TransactionCount)
					assert.InDelta(t, tt.expected[i].TotalVolumeUSD, agg.TotalVolumeUSD, 0.0001)
				}
			} else {
				assert.Empty(t, aggregatedData)
			}
		})
	}
}
