package token

import "strings"

// NormalizeTokenSymbol normalizes token symbols by converting them to their canonical forms.
// For example, "USDC.E" is normalized to "USDC".
func NormalizeTokenSymbol(symbol string) string {
	switch symbol {
	case "USDC.E":
		return "USDC"
	default:
		return strings.ToUpper(symbol)
	}
}
