package price

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

var (
	ErrHTTPResponse      = errors.New("error in HTTP response")
	ErrInvalidResponse   = errors.New("invalid CoinGecko response")
	ErrMissingMarketData = errors.New("missing market data in CoinGecko response")
	ErrMissingUSDPrice   = errors.New("missing USD price in CoinGecko response")
)

// PriceAPI is an interface for fetching historical price data.
type PriceAPI interface {
	GetHistoricalPrice(symbol string, date time.Time) (float64, error)
}

// CoinGeckoAPI implements the PriceAPI interface using the CoinGecko API.
type CoinGeckoAPI struct {
	fetchFunc func(url string) (*http.Response, error)
}

// NewCoinGeckoAPI returns a new instance of CoinGeckoAPI with a default fetch function.
func NewCoinGeckoAPI() *CoinGeckoAPI {
	return &CoinGeckoAPI{
		fetchFunc: fetchResponse,
	}
}

// GetHistoricalPrice fetches the historical price of a cryptocurrency symbol for a given date.
func (c *CoinGeckoAPI) GetHistoricalPrice(symbol string, date time.Time) (float64, error) {
	url := buildCoinGeckoURL(symbol, date)

	resp, err := c.fetchFunc(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	price, err := parsePriceFromResponse(resp)
	if err != nil {
		return 0, err
	}

	return price, nil
}

// buildCoinGeckoURL constructs the CoinGecko API URL for fetching historical price data.
func buildCoinGeckoURL(symbol string, date time.Time) string {
	formattedDate := date.Format("02-01-2006")
	return fmt.Sprintf("https://api.coingecko.com/api/v3/coins/%s/history?date=%s", symbol, formattedDate)
}

// fetchResponse sends a GET request to the given URL and returns the HTTP response.
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

// parsePriceFromResponse parses the response body and extracts the USD price from the CoinGecko API response.
func parsePriceFromResponse(resp *http.Response) (float64, error) {
	if resp.Body == nil {
		return 0, fmt.Errorf("%w: response body is empty", ErrInvalidResponse)
	}

	var result map[string]any
	decoder := json.NewDecoder(resp.Body)

	// Check for decoding errors, including malformed JSON
	if err := decoder.Decode(&result); err != nil {
		return 0, fmt.Errorf("%w: error decoding response body", ErrInvalidResponse)
	}

	// Ensure the market_data field exists
	marketData, ok := result["market_data"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("%w: market_data field not found", ErrMissingMarketData)
	}

	// Ensure the current_price field exists
	currentPrice, ok := marketData["current_price"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("%w: current_price field not found", ErrMissingMarketData)
	}

	// Ensure the USD price exists and is a valid float
	usdPrice, ok := currentPrice["usd"].(float64)
	if !ok {
		return 0, fmt.Errorf("%w: USD price not found or invalid", ErrMissingUSDPrice)
	}

	// Ensure the price is positive
	if usdPrice <= 0 {
		return 0, fmt.Errorf("%w: USD price must be positive", ErrMissingUSDPrice)
	}

	return usdPrice, nil
}
