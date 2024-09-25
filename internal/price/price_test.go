package price

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchCoinsList(t *testing.T) {
	tests := []struct {
		name          string
		mockResponse  string
		expectedCoins map[string]string
		expectedErr   bool
	}{
		{
			name: "Valid response",
			mockResponse: `[{
				"id":"bridged-matic-manta-pacific","symbol":"matic","name":"Bridged MATIC (Manta Pacific)"
			},
			{
				"id": "matic-network", "symbol": "matic", "name": "Polygon"
			},
			{
				"id": "usd-coin", "symbol": "usdc", "name": "USD Coin"
			}]`,
			expectedCoins: map[string]string{
				"MATIC": "matic-network",
				"USDC":  "usd-coin",
			},
			expectedErr: false,
		},
		{
			name:         "Invalid JSON response",
			mockResponse: `invalid json`,
			expectedErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(tt.mockResponse))
			}))
			defer mockServer.Close()

			api := NewCoinGeckoAPI()
			api.fetchFunc = func(url string) (*http.Response, error) {
				return http.Get(mockServer.URL)
			}

			coins, err := api.FetchCoinsList()

			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCoins, coins)
			}
		})
	}
}

func TestBuildCoinGeckoURL(t *testing.T) {
	tests := []struct {
		name        string
		symbol      string
		date        time.Time
		expectedURL string
	}{
		{
			name:        "valid url",
			symbol:      "SFL",
			date:        time.Date(2024, time.April, 15, 0, 0, 0, 0, time.UTC),
			expectedURL: "https://api.coingecko.com/api/v3/coins/SFL/history?date=15-04-2024",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			url := buildCoinGeckoURL(tc.symbol, tc.date)
			assert.Equal(t, tc.expectedURL, url)
		})
	}
}

func TestParsePriceFromResponse(t *testing.T) {
	tests := []struct {
		name          string
		mockResponse  string
		expectedErr   error
		expectedPrice float64
	}{
		{
			name:          "valid response",
			mockResponse:  `{"market_data": {"current_price": {"usd": 123.45}}}`,
			expectedErr:   nil,
			expectedPrice: 123.45,
		},
		{
			name:         "empty response body",
			mockResponse: "",
			expectedErr:  ErrInvalidResponse,
		},
		{
			name:         "malformed JSON",
			mockResponse: `{"market_data": { "current_price": {"usd": "123.45}`, // incomplete JSON
			expectedErr:  ErrInvalidResponse,
		},
		{
			name:         "missing market data",
			mockResponse: `{"something_else": {}}`,
			expectedErr:  ErrMissingMarketData,
		},
		{
			name:         "missing current_price",
			mockResponse: `{"market_data": {"something_else": {}}}`,
			expectedErr:  ErrMissingMarketData,
		},
		{
			name:         "missing USD price",
			mockResponse: `{"market_data": {"current_price": {"eur": 123.45}}}`,
			expectedErr:  ErrMissingUSDPrice,
		},
		{
			name:         "USD price not a float",
			mockResponse: `{"market_data": {"current_price": {"usd": "invalid"}}}`,
			expectedErr:  ErrMissingUSDPrice,
		},
		{
			name:         "negative USD price",
			mockResponse: `{"market_data": {"current_price": {"usd": -50.00}}}`,
			expectedErr:  ErrMissingUSDPrice,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{
				Body: io.NopCloser(strings.NewReader(tc.mockResponse)),
			}

			price, err := parsePriceFromResponse(resp)
			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, tc.expectedErr), "expected error type does not match")
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedPrice, price)
			}
		})
	}
}

func TestGetHistoricalPrice(t *testing.T) {
	tests := []struct {
		name          string
		mockResponse  string
		expectedPrice float64
		expectedErr   bool
		statusCode    int
	}{
		{
			name: "valid response",
			mockResponse: `{
				"market_data": {
					"current_price": {
						"usd": 45000.34
					}
				}
			}`,
			expectedPrice: 45000.34,
			expectedErr:   false,
			statusCode:    http.StatusOK,
		},
		{
			name:          "404 error",
			mockResponse:  ``,
			expectedPrice: 0,
			expectedErr:   true,
			statusCode:    http.StatusNotFound,
		},
		{
			name:          "invalid JSON",
			mockResponse:  `{invalid json}`,
			expectedPrice: 0,
			expectedErr:   true,
			statusCode:    http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
				w.Write([]byte(tc.mockResponse))
			}))
			defer mockServer.Close()

			api := &CoinGeckoAPI{
				fetchFunc: func(url string) (*http.Response, error) {
					return http.Get(mockServer.URL)
				},
			}

			price, err := api.GetHistoricalPrice("BTC", time.Now())

			if tc.expectedErr {
				assert.Error(t, err)
				assert.Equal(t, 0.0, price)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedPrice, price)
			}
		})
	}
}

func TestGetHistoricalPrices(t *testing.T) {
	tests := []struct {
		name           string
		symbols        []string
		mockResponses  map[string]string
		expectedErr    bool
		expectedPrices map[string]float64
	}{
		{
			name:    "multiple valid responses",
			symbols: []string{"SFL", "ETH"},
			mockResponses: map[string]string{
				"SFL": `{"market_data": {"current_price": {"usd": 123.45}}}`,
				"ETH": `{"market_data": {"current_price": {"usd": 2345.67}}}`,
			},
			expectedErr:    false,
			expectedPrices: map[string]float64{"SFL": 123.45, "ETH": 2345.67},
		},
		{
			name:    "one valid one invalid",
			symbols: []string{"SFL", "ETH"},
			mockResponses: map[string]string{
				"SFL": `{"market_data": {"current_price": {"usd": 123.45}}}`,
				"ETH": ``,
			},
			expectedErr:    true,
			expectedPrices: map[string]float64{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Extract the symbol from the URL to decide which mock response to return
				t.Logf("Request URL: %s", r.URL.String())
				if strings.Contains(r.URL.String(), "SFL") {
					t.Logf("Returning mock response for SFL")
					w.Write([]byte(tc.mockResponses["SFL"]))
				} else if strings.Contains(r.URL.String(), "ETH") {
					t.Logf("Returning mock response for ETH")
					w.Write([]byte(tc.mockResponses["ETH"]))
				}
			}))
			defer mockServer.Close()

			// Create a new CoinGeckoAPI with a custom fetchFunc to use the mock server
			api := &CoinGeckoAPI{
				fetchFunc: func(url string) (*http.Response, error) {
					// Append the actual token symbol to the mock server URL
					mockURL := mockServer.URL + url[strings.Index(url, "/coins"):]
					return http.Get(mockURL)
				},
			}

			// Call GetHistoricalPrices with the test case symbols
			prices, err := api.GetHistoricalPrices(tc.symbols, time.Now())

			if tc.expectedErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Assert the prices match the expected values
			for symbol, expectedPrice := range tc.expectedPrices {
				actualPrice, ok := prices[symbol]
				require.True(t, ok, "price for symbol %s not found", symbol)
				assert.Equal(t, expectedPrice, actualPrice, "price for symbol %s did not match", symbol)
			}
		})
	}
}

func TestFetchResponse(t *testing.T) {
	tests := []struct {
		name        string
		statusCode  int
		expectedErr bool
	}{
		{
			name:        "200 OK",
			statusCode:  http.StatusOK,
			expectedErr: false,
		},
		{
			name:        "500 Internal Server Error",
			statusCode:  http.StatusInternalServerError,
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
			}))
			defer mockServer.Close()

			resp, err := fetchResponse(mockServer.URL)
			if tc.expectedErr {
				assert.Error(t, err)
				assert.Nil(t, resp)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, resp)
			}
		})
	}
}
