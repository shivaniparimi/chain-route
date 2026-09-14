package money

import (
	"math/big"
	"testing"
)

func TestDecimalToBaseUnits_ExactFit(t *testing.T) {
	got, err := DecimalToBaseUnits("1.123456", 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := big.NewInt(1123456)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_RejectsExcessPrecision(t *testing.T) {
	_, err := DecimalToBaseUnits("1.1234567", 6)
	if err == nil {
		t.Fatal("expected an error for 7 fractional digits against a 6-decimal token, got none")
	}
}

func TestDecimalToBaseUnits_RightPadsShortFractional(t *testing.T) {
	got, err := DecimalToBaseUnits("1.5", 18)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, _ := new(big.Int).SetString("1500000000000000000", 10)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_NoFractionalPart(t *testing.T) {
	got, err := DecimalToBaseUnits("42", 18)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, _ := new(big.Int).SetString("42000000000000000000", 10)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_MaxDigitsWETH(t *testing.T) {
	// 20 integer digits, 18 fractional digits -- the maximum Phase 5's
	// amount pattern (^\d{1,20}(\.\d{1,18})?$) allows.
	got, err := DecimalToBaseUnits("99999999999999999999.999999999999999999", 18)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_TrailingZerosExactFit(t *testing.T) {
	got, err := DecimalToBaseUnits("2.500000", 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := big.NewInt(2500000)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDecimalToBaseUnits_RejectionProducesNoValue(t *testing.T) {
	got, err := DecimalToBaseUnits("1.1234567", 6)
	if err == nil {
		t.Fatal("expected error")
	}
	if got != nil {
		t.Fatalf("expected a nil result alongside the rejection error, got %v", got)
	}
}
