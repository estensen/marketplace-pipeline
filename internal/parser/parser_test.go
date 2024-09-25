package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/estensen/marketplace-pipeline/internal/models"
	"github.com/estensen/marketplace-pipeline/internal/parser"
)

func TestParseCSV(t *testing.T) {
	tests := []struct {
		name          string
		filePath      string
		expectedError bool
		expectedTxn   []models.Transaction
	}{
		{
			name:     "Valid CSV file",
			filePath: "../../data/sample.csv",
			expectedTxn: []models.Transaction{
				{
					Event:     "BUY_ITEMS",
					ProjectID: "4974",
					Props:     models.Props{CurrencySymbol: "SFL", ChainID: "137"},
					Nums:      models.Nums{CurrencyValueDecimal: "0.6136203411678249"},
				},
			},
			expectedError: false,
		},
		{
			name:          "Missing file",
			filePath:      "missing.csv",
			expectedTxn:   nil,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewCSVParser()
			transactions, err := p.ParseCSV(tt.filePath)
			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, transactions)
			for i, expected := range tt.expectedTxn {
				assert.Equal(t, expected.Event, transactions[i].Event)
				assert.Equal(t, expected.ProjectID, transactions[i].ProjectID)
				assert.Equal(t, expected.Props.CurrencySymbol, transactions[i].Props.CurrencySymbol)
				assert.Equal(t, expected.Props.ChainID, transactions[i].Props.ChainID)
				assert.Equal(t, expected.Nums.CurrencyValueDecimal, transactions[i].Nums.CurrencyValueDecimal)
			}
		})
	}
}

func TestParseRecord(t *testing.T) {
	tests := []struct {
		name          string
		record        []string
		expectedTxn   models.Transaction
		expectedError bool
	}{
		{
			name: "Valid record",
			record: []string{
				"seq-market",
				"2024-04-15 02:15:07.167",
				"BUY_ITEMS",
				"4974",
				"",
				"1",
				"0896ae95dcaeee38e83fa5c43bef99780d7b2be23bcab36214",
				"5d8afd8fec2fbf3e",
				"DE",
				"desktop",
				"linux",
				"x86_64",
				"chrome",
				"122.0.0.0",
				`{"currencySymbol":"SFL", "chainId":"137"}`,
				`{"currencyValueDecimal":"0.6136203411678249"}`,
			},
			expectedTxn: models.Transaction{
				Event:     "BUY_ITEMS",
				ProjectID: "4974",
				Props:     models.Props{CurrencySymbol: "SFL", ChainID: "137"},
				Nums:      models.Nums{CurrencyValueDecimal: "0.6136203411678249"},
			},
			expectedError: false,
		},
		{
			name: "Invalid timestamp",
			record: []string{
				"seq-market",
				"invalid-timestamp",
				"BUY_ITEMS",
				"4974",
				"",
				"1",
				"0896ae95dcaeee38e83fa5c43bef99780d7b2be23bcab36214",
				"5d8afd8fec2fbf3e",
				"DE",
				"desktop",
				"linux",
				"x86_64",
				"chrome",
				"122.0.0.0",
				`{"currencySymbol":"SFL", "chainId":"137"}`,
				`{"currencyValueDecimal":"0.6136203411678249"}`,
			},
			expectedTxn:   models.Transaction{},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn, err := parser.ParseRecord(tt.record)
			if tt.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedTxn.Event, txn.Event)
			assert.Equal(t, tt.expectedTxn.ProjectID, txn.ProjectID)
			assert.Equal(t, tt.expectedTxn.Props.CurrencySymbol, txn.Props.CurrencySymbol)
			assert.Equal(t, tt.expectedTxn.Props.ChainID, txn.Props.ChainID)
			assert.Equal(t, tt.expectedTxn.Nums.CurrencyValueDecimal, txn.Nums.CurrencyValueDecimal)
		})
	}
}
