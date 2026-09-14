package evm

import "testing"

func TestLoadWallet_ValidKey(t *testing.T) {
	// A well-known, publicly-documented go-ethereum test private key --
	// never a real funded key. Address is its deterministic derivation,
	// verified by running crypto.HexToECDSA + crypto.PubkeyToAddress
	// directly (see Task 8 report for details).
	const testKey = "b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291"
	w, err := LoadWallet(testKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Address.Hex() != "0x71562b71999873DB5b286dF957af199Ec94617F7" {
		t.Fatalf("unexpected derived address: %s", w.Address.Hex())
	}
}

func TestLoadWallet_AcceptsHexPrefix(t *testing.T) {
	const testKey = "0xb71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291"
	w, err := LoadWallet(testKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Address.Hex() != "0x71562b71999873DB5b286dF957af199Ec94617F7" {
		t.Fatalf("unexpected derived address: %s", w.Address.Hex())
	}
}

func TestLoadWallet_RejectsInvalidKey(t *testing.T) {
	if _, err := LoadWallet("not-hex"); err == nil {
		t.Fatal("expected an error for a malformed key")
	}
}
