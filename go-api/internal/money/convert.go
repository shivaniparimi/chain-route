package money

import (
	"fmt"
	"math/big"
	"strings"
)

// DecimalToBaseUnits converts an exact decimal amount string (already
// validated by the caller against a pattern like Phase 5's
// ^\d{1,20}(\.\d{1,18})?$) into the token's integer base units, using
// big.Int exclusively -- never float64 or big.Float.
//
// If amount's fractional part is shorter than decimals, it is right-padded
// with zeros (always exact). If it is LONGER than decimals, the amount
// cannot be represented exactly at that precision: this returns an error
// rather than truncating the excess digits, since silently dropping a
// fractional digit would move real value without the caller's knowledge.
func DecimalToBaseUnits(amount string, decimals uint8) (*big.Int, error) {
	intPart, fracPart, hasFrac := amount, "", false
	if idx := strings.IndexByte(amount, '.'); idx >= 0 {
		intPart, fracPart, hasFrac = amount[:idx], amount[idx+1:], true
	}
	_ = hasFrac

	if len(fracPart) > int(decimals) {
		return nil, fmt.Errorf("amount %q has %d fractional digits, exceeding the token's %d-decimal precision -- refusing to truncate", amount, len(fracPart), decimals)
	}

	padded := fracPart + strings.Repeat("0", int(decimals)-len(fracPart))
	digits := intPart + padded

	result, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return nil, fmt.Errorf("amount %q could not be parsed as an integer after base-unit conversion", amount)
	}
	return result, nil
}

// BaseUnitsToDecimal converts an integer base-units amount back into an
// exact decimal string at the given token precision -- the inverse of
// DecimalToBaseUnits, using big.Int exclusively. Trailing fractional
// zeros are trimmed (e.g. 18-decimal "1000000000000000000" becomes "1",
// not "1.000000000000000000"); a decimals of 0 produces the integer
// string unchanged.
func BaseUnitsToDecimal(amount *big.Int, decimals uint8) string {
	neg := amount.Sign() < 0
	s := new(big.Int).Abs(amount).String()
	for len(s) <= int(decimals) {
		s = "0" + s
	}
	splitAt := len(s) - int(decimals)
	intPart, fracPart := s[:splitAt], strings.TrimRight(s[splitAt:], "0")

	result := intPart
	if fracPart != "" {
		result += "." + fracPart
	}
	if neg && result != "0" {
		result = "-" + result
	}
	return result
}
