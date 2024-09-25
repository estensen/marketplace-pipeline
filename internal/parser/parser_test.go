package parser_test

import (
	"testing"

	"github.com/estensen/marketplace-pipeline/internal/models"
	"github.com/estensen/marketplace-pipeline/internal/parser"
)

func TestParseCSV(t *testing.T) {
	p := parser.NewCSVParser()
	transactions, err := p.ParseCSV("../../data/sample.csv")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(transactions) == 0 {
		t.Fatalf("Expected transactions, got none")
	}

	expectedFirstTxn := models.Transaction{
		Event:     "BUY_ITEMS",
		ProjectID: "4974",
		Props:     models.Props{CurrencySymbol: "SFL", ChainID: "137"},
	}

	if transactions[0].Event != expectedFirstTxn.Event {
		t.Errorf("Expected event %s, got %s", expectedFirstTxn.Event, transactions[0].Event)
	}

	if transactions[0].ProjectID != expectedFirstTxn.ProjectID {
		t.Errorf("Expected project_id %s, got %s", expectedFirstTxn.ProjectID, transactions[0].ProjectID)
	}

	if transactions[0].Props.CurrencySymbol != expectedFirstTxn.Props.CurrencySymbol {
		t.Errorf("Expected currencySymbol %s, got %s", expectedFirstTxn.Props.CurrencySymbol, transactions[0].Props.CurrencySymbol)
	}

	if transactions[0].Props.ChainID != expectedFirstTxn.Props.ChainID {
		t.Errorf("Expected chainId %s, got %s", expectedFirstTxn.Props.ChainID, transactions[0].Props.ChainID) // Assert ChainID
	}
}
func TestParseRecord(t *testing.T) {
	record := []string{
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
	}

	expectedTxn := models.Transaction{
		Event:     "BUY_ITEMS",
		ProjectID: "4974",
		Props:     models.Props{CurrencySymbol: "SFL", ChainID: "137"},
		Nums:      models.Nums{CurrencyValueDecimal: "0.6136203411678249"},
	}

	txn, err := parser.ParseRecord(record)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if txn.Event != expectedTxn.Event {
		t.Errorf("Expected event %s, got %s", expectedTxn.Event, txn.Event)
	}

	if txn.ProjectID != expectedTxn.ProjectID {
		t.Errorf("Expected project_id %s, got %s", expectedTxn.ProjectID, txn.ProjectID)
	}

	if txn.Props.CurrencySymbol != expectedTxn.Props.CurrencySymbol {
		t.Errorf("Expected currencySymbol %s, got %s", expectedTxn.Props.CurrencySymbol, txn.Props.CurrencySymbol)
	}

	if txn.Props.ChainID != expectedTxn.Props.ChainID {
		t.Errorf("Expected chainId %s, got %s", expectedTxn.Props.ChainID, txn.Props.ChainID)
	}

	if txn.Nums.CurrencyValueDecimal != expectedTxn.Nums.CurrencyValueDecimal {
		t.Errorf("Expected currencyValueDecimal %s, got %s", expectedTxn.Nums.CurrencyValueDecimal, txn.Nums.CurrencyValueDecimal)
	}
}
