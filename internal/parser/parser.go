package parser

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"time"

	"github.com/estensen/marketplace-pipeline/internal/models"
)

type Parser interface {
	ParseCSV(filePath string) ([]models.Transaction, error)
}

type CSVParser struct{}

func NewCSVParser() *CSVParser {
	return &CSVParser{}
}

func (p *CSVParser) ParseCSV(filePath string) ([]models.Transaction, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var transactions []models.Transaction
	for i, record := range records {
		if i == 0 {
			continue // Skip header
		}
		txn, err := ParseRecord(record)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, txn)
	}
	return transactions, nil
}

func ParseRecord(record []string) (models.Transaction, error) {
	var txn models.Transaction
	var err error

	txn.Timestamp, err = time.Parse("2006-01-02 15:04:05.000", record[1])
	if err != nil {
		return txn, err
	}
	txn.Event = record[2]
	txn.ProjectID = record[3]

	err = json.Unmarshal([]byte(record[14]), &txn.Props)
	if err != nil {
		return txn, err
	}

	err = json.Unmarshal([]byte(record[15]), &txn.Nums)
	if err != nil {
		return txn, err
	}

	return txn, nil
}
