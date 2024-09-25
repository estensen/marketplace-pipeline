package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeTokenSymbol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		symbol     string
		normalized string
	}{
		{
			symbol:     "USDC.E",
			normalized: "USDC",
		},
		{
			symbol:     "SFL",
			normalized: "SFL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.symbol, func(t *testing.T) {
			normalized := NormalizeTokenSymbol(tc.symbol)
			assert.Equal(t, tc.normalized, normalized)
		})
	}
}
