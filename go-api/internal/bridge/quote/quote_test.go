package quote

import (
	"math/big"
	"testing"
	"time"
)

func TestQuote_FeeIsInputMinusOutput_ByConvention(t *testing.T) {
	// This test documents the convention (design doc §4) that callers
	// constructing a Quote must uphold: FeeBaseUnits = input - output.
	// It is a convention enforced by provider implementations (Task 6),
	// not by this struct -- this test exists so a reader of quote_test.go
	// sees the invariant spelled out even though the struct can't enforce
	// it itself.
	input := big.NewInt(1_000_000_000_000_000)
	output := big.NewInt(997_592_172_330_233)
	q := Quote{
		InputAmountBaseUnits:  input,
		OutputAmountBaseUnits: output,
		FeeBaseUnits:          new(big.Int).Sub(input, output),
		QuotedAt:              time.Now(),
	}
	want := new(big.Int).Sub(input, output)
	if q.FeeBaseUnits.Cmp(want) != 0 {
		t.Fatalf("FeeBaseUnits = %s, want %s", q.FeeBaseUnits, want)
	}
}
