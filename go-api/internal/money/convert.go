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
