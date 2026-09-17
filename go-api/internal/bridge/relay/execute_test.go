package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

func TestBuildTransaction_DecodesEnvelopeFromRawPayload(t *testing.T) {
	wallet := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	payload := QuotePayload{To: "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3", Data: "0xdeadbeef", Value: "1000000000000000", ChainID: 11155111, Recipient: wallet.Hex()}
	raw, _ := json.Marshal(payload)
	fresh := quote.Quote{RawProviderPayload: raw, InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}

	p := &Provider{WalletAddress: wallet}
	env, err := p.BuildTransaction(context.Background(), fresh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env.To != common.HexToAddress(payload.To) {
		t.Errorf("To = %s, want %s", env.To.Hex(), payload.To)
	}
	if env.Value.String() != "1000000000000000" {
		t.Errorf("Value = %s, want 1000000000000000", env.Value)
	}
	if env.ChainID != 11155111 {
		t.Errorf("ChainID = %d, want 11155111", env.ChainID)
	}
	if len(env.Data) != 4 { // 0xdeadbeef = 4 bytes
		t.Errorf("Data length = %d, want 4", len(env.Data))
	}
}

func TestBuildTransaction_RecipientMismatchIsHardError(t *testing.T) {
	wallet := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	otherRecipient := common.HexToAddress("0x1111111111111111111111111111111111111111")
	payload := QuotePayload{To: "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3", Data: "0xdead", Value: "1000000000000000", ChainID: 11155111, Recipient: otherRecipient.Hex()}
	raw, _ := json.Marshal(payload)
	fresh := quote.Quote{RawProviderPayload: raw, InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}

	p := &Provider{WalletAddress: wallet}
	env, err := p.BuildTransaction(context.Background(), fresh)
	if err == nil {
		t.Fatal("expected an error when the quoted recipient does not match this executor's wallet")
	}
	// Strengthen the brief's check: prove the function did not go on to decode
	// value/data past the recipient check -- the returned envelope must be the
	// zero value, not a partially-populated one.
	if env.To != (common.Address{}) || env.Value != nil || env.Data != nil || env.ChainID != 0 {
		t.Errorf("expected zero-value TxEnvelope on hard error, got %+v", env)
	}
}

// TestBuildTransaction_EmptyRecipientIsHardError proves the recipient check
// fails CLOSED, not open, when the payload has no recipient to verify at
// all -- a malformed provider response, a future Relay response shape that
// omits the field, or a tampered payload must never be treated as "nothing
// to check" and allowed to build a fully spendable envelope.
func TestBuildTransaction_EmptyRecipientIsHardError(t *testing.T) {
	wallet := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	payload := QuotePayload{To: "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3", Data: "0xdead", Value: "1000000000000000", ChainID: 11155111, Recipient: ""}
	raw, _ := json.Marshal(payload)
	fresh := quote.Quote{RawProviderPayload: raw, InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}

	p := &Provider{WalletAddress: wallet}
	env, err := p.BuildTransaction(context.Background(), fresh)
	if err == nil {
		t.Fatal("expected an error when the quote payload has no verifiable recipient")
	}
	if env.To != (common.Address{}) || env.Value != nil || env.Data != nil || env.ChainID != 0 {
		t.Errorf("expected zero-value TxEnvelope on hard error, got %+v", env)
	}
}

// TestBuildTransaction_OddLengthHexDataIsHardError covers the odd-length
// calldata edge case: an odd-length hex string is malformed, unverifiable,
// provider-opaque calldata about to be signed and broadcast with real funds
// behind it -- guessing a leading-zero padding could silently sign a
// different payload than the provider actually intended, so this must be
// rejected outright rather than "corrected" (M1).
func TestBuildTransaction_OddLengthHexDataIsHardError(t *testing.T) {
	wallet := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	payload := QuotePayload{To: "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3", Data: "0xabc", Value: "1000000000000000", ChainID: 11155111, Recipient: wallet.Hex()}
	raw, _ := json.Marshal(payload)
	fresh := quote.Quote{RawProviderPayload: raw, InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}

	p := &Provider{WalletAddress: wallet}
	env, err := p.BuildTransaction(context.Background(), fresh)
	if err == nil {
		t.Fatal("expected an error for odd-length hex calldata, not a guessed zero-padding")
	}
	if env.To != (common.Address{}) || env.Value != nil || env.Data != nil || env.ChainID != 0 {
		t.Errorf("expected zero-value TxEnvelope on hard error, got %+v", env)
	}
}

// TestBuildTransaction_UppercaseHexPrefixIsTrimmed covers M2: the "0x"
// prefix trim must be case-insensitive, since an "0X"-prefixed hex string is
// equally valid hex and must not be misinterpreted as having no prefix at
// all (which would then also misalign the odd/even length check above).
func TestBuildTransaction_UppercaseHexPrefixIsTrimmed(t *testing.T) {
	wallet := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	payload := QuotePayload{To: "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3", Data: "0XDEADBEEF", Value: "1000000000000000", ChainID: 11155111, Recipient: wallet.Hex()}
	raw, _ := json.Marshal(payload)
	fresh := quote.Quote{RawProviderPayload: raw, InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}

	p := &Provider{WalletAddress: wallet}
	env, err := p.BuildTransaction(context.Background(), fresh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []byte{0xde, 0xad, 0xbe, 0xef}
	if !bytes.Equal(env.Data, want) {
		t.Errorf("Data = %x, want %x (uppercase \"0X\" prefix should be trimmed like \"0x\")", env.Data, want)
	}
}
