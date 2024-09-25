package token

// NormalizeTokenSymbol normalizes token symbols
// like "USDC.E" to their canonical equivalents (e.g., "USDC").
func NormalizeTokenSymbol(symbol string) string {
	switch symbol {
	case "USDC.E":
		return "USDC"
	default:
		return symbol
	}
}
