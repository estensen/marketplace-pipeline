package models

import "time"

type Transaction struct {
	Timestamp time.Time
	Event     string
	ProjectID string
	Props     Props
	Nums      Nums
}

type Props struct {
	CurrencySymbol    string `json:"currencySymbol"`
	ChainID           string `json:"chainId"`
	CollectionAddress string `json:"collectionAddress"`
	CurrencyAddress   string `json:"currencyAddress"`
}

type Nums struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal"`
}

type AggregatedData struct {
	Date             time.Time `ch:"date"`
	ProjectID        string    `ch:"project_id"`
	TransactionCount uint64    `ch:"transaction_count"`
	TotalVolumeUSD   float64   `ch:"total_volume_usd"`
}
