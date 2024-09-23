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
	}{
		{
			name: "valid response",
			mockResponse: `{
				"market_data": {
					"current_price": {
						"usd": 123.45
					}
				}
			}`,
			expectedPrice: 123.45,
			expectedErr:   false,
		},
		{
			name:          "error response",
			mockResponse:  "",
			expectedPrice: 0.0,
			expectedErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.mockResponse != "" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(tc.mockResponse))
				} else {
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			defer mockServer.Close()

			api := &CoinGeckoAPI{
				fetchFunc: func(url string) (*http.Response, error) {
					return http.Get(mockServer.URL)
				},
			}

			price, err := api.GetHistoricalPrice("SFL", time.Now())
			if tc.expectedErr {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedPrice, price)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedPrice, price)
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
