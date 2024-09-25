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

// NewCSVParser creates a new instance of CSVParser.
func NewCSVParser() *CSVParser {
	return &CSVParser{}
}

// ParseCSV parses the CSV file at the given path into a slice of transactions.
func (p *CSVParser) ParseCSV(filePath string) ([]models.Transaction, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	records, err := p.readCSV(file)
	if err != nil {
		return nil, err
	}

	return p.parseRecords(records)
}

// readCSV reads the CSV content from a file and returns the records.
func (p *CSVParser) readCSV(file *os.File) ([][]string, error) {
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields
	return reader.ReadAll()
}

// parseRecords parses the CSV records into transactions.
func (p *CSVParser) parseRecords(records [][]string) ([]models.Transaction, error) {
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

// ParseRecord parses a single CSV record into a Transaction model.
func ParseRecord(record []string) (models.Transaction, error) {
	var txn models.Transaction
	var err error

	txn.Timestamp, err = parseTimestamp(record[1])
	if err != nil {
		return txn, err
	}

	txn.Event = record[2]
	txn.ProjectID = record[3]

	err = parseProps(record[14], &txn.Props)
	if err != nil {
		return txn, err
	}

	err = parseNums(record[15], &txn.Nums)
	if err != nil {
		return txn, err
	}

	return txn, nil
}

// parseTimestamp parses the timestamp from the CSV record.
func parseTimestamp(timestamp string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05.000", timestamp)
}

// parseProps unmarshals the JSON-encoded Props from the record.
func parseProps(data string, props *models.Props) error {
	return json.Unmarshal([]byte(data), props)
}

// parseNums unmarshals the JSON-encoded Nums from the record.
func parseNums(data string, nums *models.Nums) error {
	return json.Unmarshal([]byte(data), nums)
}
