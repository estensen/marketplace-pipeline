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
					ProjectID:        "137",
					TransactionCount: 1,
					TotalVolumeUSD:   1.316777549196586 * 1.23,
				},
				{
					Date:             time.Date(2024, 4, 2, 0, 0, 0, 0, time.UTC),
					ProjectID:        "137",
					TransactionCount: 1,
					TotalVolumeUSD:   0.7 * 0.408257,
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
		{
			name: "Multiple transactions for the same date and project",
			transactions: []models.Transaction{
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
				{
					Timestamp: time.Date(2024, 4, 2, 10, 0, 0, 0, time.UTC),
					Props: models.Props{
						CurrencySymbol: "MATIC",
						ChainID:        "137",
					},
					Nums: models.Nums{
						CurrencyValueDecimal: "200000000000000000",
					},
				},
			},
			prices: map[string]float64{
				"MATIC": 0.408257,
			},
			expected: []models.AggregatedData{
				{
					Date:             time.Date(2024, 4, 2, 0, 0, 0, 0, time.UTC),
					ProjectID:        "137",
					TransactionCount: 2,
					TotalVolumeUSD:   (0.7 + 0.2) * 0.408257,
				},
			},
			expectedError: false,
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

func TestParseCurrencyValue(t *testing.T) {
	aggregator := NewAggregator()

	tests := []struct {
		name      string
		input     string
		expected  float64
		expectErr bool
	}{
		{
			name:      "Valid value",
			input:     "1316777549196586000",
			expected:  1.316777549196586, // normalized from wei
			expectErr: false,
		},
		{
			name:      "Invalid value",
			input:     "invalid_value",
			expected:  0,
			expectErr: true,
		},
		{
			name:      "Zero value",
			input:     "0",
			expected:  0,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := aggregator.parseCurrencyValue(tt.input)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.InDelta(t, tt.expected, value, 0.00000001)
			}
		})
	}
}

func TestGetPriceUSD(t *testing.T) {
	aggregator := NewAggregator()

	prices := map[string]float64{
		"MATIC": 0.408257,
		"SFL":   1.23,
	}

	tests := []struct {
		name      string
		symbol    string
		expected  float64
		expectErr bool
	}{
		{
			name:      "Valid symbol",
			symbol:    "MATIC",
			expected:  0.408257,
			expectErr: false,
		},
		{
			name:      "Valid normalized symbol",
			symbol:    "matic",
			expected:  0.408257,
			expectErr: false,
		},
		{
			name:      "Missing symbol",
			symbol:    "USDC",
			expected:  0,
			expectErr: true,
		},
		{
			name:      "Different case symbol",
			symbol:    "SFL",
			expected:  1.23,
			expectErr: false,
		},
		{
			name:      "Invalid symbol",
			symbol:    "INVALID",
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			price, err := aggregator.getPriceUSD(tt.symbol, prices)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, price)
			}
		})
	}
}

func TestUpdateAggregatedData(t *testing.T) {
	aggregator := NewAggregator()
	dataMap := make(map[string]*models.AggregatedData)

	tests := []struct {
		name           string
		key            string
		date           time.Time
		projectID      string
		totalVolumeUSD float64
		expectedCount  uint64
		expectedVolume float64
	}{
		{
			name:           "Initial update",
			key:            "2024-04-15-137",
			date:           time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC),
			projectID:      "137",
			totalVolumeUSD: 100.0,
			expectedCount:  1,
			expectedVolume: 100.0,
		},
		{
			name:           "Second update on the same key",
			key:            "2024-04-15-137",
			date:           time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC),
			projectID:      "137",
			totalVolumeUSD: 50.0,
			expectedCount:  2,
			expectedVolume: 150.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregator.updateAggregatedData(dataMap, tt.key, tt.date, tt.projectID, tt.totalVolumeUSD)
			assert.Len(t, dataMap, 1)
			assert.Equal(t, tt.expectedCount, dataMap[tt.key].TransactionCount)
			assert.Equal(t, tt.expectedVolume, dataMap[tt.key].TotalVolumeUSD)
		})
	}
}
