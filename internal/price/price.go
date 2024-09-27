package price

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// Predefined errors for better error handling.
var (
	ErrHTTPResponse      = errors.New("error in HTTP response")
	ErrInvalidResponse   = errors.New("invalid CoinGecko response")
	ErrMissingMarketData = errors.New("missing market data in CoinGecko response")
	ErrMissingUSDPrice   = errors.New("missing USD price in CoinGecko response")
)

// CoinAPI defines the interface for fetching historical price data.
type CoinAPI interface {
	GetHistoricalPrice(coinID string, date time.Time) (float64, error)
	GetHistoricalPrices(coinIDs []string, date time.Time) (map[string]float64, error)
	FetchCoinsList() (map[string]string, error)
}

// CoinGeckoAPI implements the CoinAPI interface using the CoinGecko API.
type CoinGeckoAPI struct {
	fetchFunc func(url string) (*http.Response, error)
}

// NewCoinGeckoAPI creates a new instance of CoinGeckoAPI.
func NewCoinGeckoAPI() *CoinGeckoAPI {
	return &CoinGeckoAPI{
		fetchFunc: fetchResponse,
	}
}

// CoinInfo represents the structure of a coin's information from CoinGecko.
type CoinInfo struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
}

// FetchCoinsList retrieves the list of all coins from CoinGecko and maps symbols to their IDs.
func (c *CoinGeckoAPI) FetchCoinsList() (map[string]string, error) {
	resp, err := c.fetchFunc("https://api.coingecko.com/api/v3/coins/list")
	if err != nil {
		return nil, fmt.Errorf("error fetching coins list: %v", err)
	}
	defer resp.Body.Close()

	coins, err := decodeCoinsList(resp)
	if err != nil {
		return nil, err
	}

	return mapCoinsList(coins), nil
}

// decodeCoinsList decodes the CoinGecko response into a slice of CoinInfo.
func decodeCoinsList(resp *http.Response) ([]CoinInfo, error) {
	var coins []CoinInfo
	if err := json.NewDecoder(resp.Body).Decode(&coins); err != nil {
		return nil, fmt.Errorf("error decoding coins list: %v", err)
	}
	return coins, nil
}

// mapCoinsList maps a slice of CoinInfo to a map of symbol to CoinGecko ID.
// Prioritizes canonical symbols like "MATIC".
func mapCoinsList(coins []CoinInfo) map[string]string {
	symbolToIDMap := make(map[string]string)
	for _, coin := range coins {
		symbol := strings.ToUpper(coin.Symbol)

		// Prioritize canonical symbols like "MATIC" with specific IDs
		if symbol == "MATIC" && coin.ID != "matic-network" {
			continue
		}

		if _, exists := symbolToIDMap[symbol]; !exists {
			symbolToIDMap[symbol] = coin.ID
		}
	}
	return symbolToIDMap
}

// GetHistoricalPrice fetches the historical USD price of a cryptocurrency for a given date.
func (c *CoinGeckoAPI) GetHistoricalPrice(coinID string, date time.Time) (float64, error) {
	url := buildCoinGeckoURL(coinID, date)

	resp, err := c.fetchFunc(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// Skip tokens if market_data is not available
	price, err := parsePriceFromResponse(resp)
	if err != nil {
		if errors.Is(err, ErrMissingMarketData) {
			log.Printf("No market data for token: %s", coinID)
			return 0, nil // Return 0 and continue processing
		}
		return 0, err
	}

	return price, nil
}

// GetHistoricalPrices fetches the historical USD prices of multiple cryptocurrencies for a given date.
func (c *CoinGeckoAPI) GetHistoricalPrices(coinIDs []string, date time.Time) (map[string]float64, error) {
	prices := make(map[string]float64)
	for _, coinID := range coinIDs {
		price, err := c.GetHistoricalPrice(coinID, date)
		if err != nil {
			return nil, fmt.Errorf("error fetching price for coin %s: %v", coinID, err)
		}
		prices[coinID] = price
	}
	return prices, nil
}

// buildCoinGeckoURL constructs the API URL for fetching historical price data.
func buildCoinGeckoURL(coinID string, date time.Time) string {
	formattedDate := date.Format("02-01-2006")
	return fmt.Sprintf("https://api.coingecko.com/api/v3/coins/%s/history?date=%s", coinID, formattedDate)
}

// fetchResponse performs an HTTP GET request and returns the response.
func fetchResponse(url string) (*http.Response, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error fetching price from CoinGecko: %v", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("received non-OK status code: %d", resp.StatusCode)
	}

	return resp, nil
}

// parsePriceFromResponse extracts the USD price from the CoinGecko API response.
func parsePriceFromResponse(resp *http.Response) (float64, error) {
	if resp.Body == nil {
		return 0, fmt.Errorf("%w: response body is empty", ErrInvalidResponse)
	}

	var result map[string]interface{}
	decoder := json.NewDecoder(resp.Body)

	if err := decoder.Decode(&result); err != nil {
		return 0, fmt.Errorf("%w: error decoding response body", ErrInvalidResponse)
	}

	// Check if the "market_data" field is present
	marketData, ok := result["market_data"].(map[string]interface{})
	if !ok {
		log.Printf("Missing market data for token %s", result["id"])
		return 0, fmt.Errorf("%w: market_data field not found", ErrMissingMarketData)
	}

	currentPrice, ok := marketData["current_price"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("%w: current_price field not found", ErrMissingMarketData)
	}

	usdPrice, ok := currentPrice["usd"].(float64)
	if !ok {
		return 0, fmt.Errorf("%w: USD price not found or invalid", ErrMissingUSDPrice)
	}

	if usdPrice <= 0 {
		return 0, fmt.Errorf("%w: USD price must be positive", ErrMissingUSDPrice)
	}

	return usdPrice, nil
}
