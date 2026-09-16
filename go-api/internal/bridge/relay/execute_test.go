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

// TestBuildTransaction_OddLengthHexDataIsZeroPadded covers the leading-zero
// calldata edge case: an odd-length hex string (one that lost a leading "0"
// somewhere upstream) must be zero-padded to an even length BEFORE decoding,
// not silently misaligned into the wrong bytes -- this calldata ends up
// signed and broadcast with real funds behind it.
func TestBuildTransaction_OddLengthHexDataIsZeroPadded(t *testing.T) {
	wallet := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	payload := QuotePayload{To: "0x5feaB8db4534f9F7e2669bb260C57A01aD1c12E3", Data: "0xabc", Value: "1000000000000000", ChainID: 11155111, Recipient: wallet.Hex()}
	raw, _ := json.Marshal(payload)
	fresh := quote.Quote{RawProviderPayload: raw, InputAmountBaseUnits: big.NewInt(1_000_000_000_000_000)}

	p := &Provider{WalletAddress: wallet}
	env, err := p.BuildTransaction(context.Background(), fresh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []byte{0x0a, 0xbc}
	if !bytes.Equal(env.Data, want) {
		t.Errorf("Data = %x, want %x (i.e. \"0xabc\" zero-padded to \"0xabc\" -> \"0abc\")", env.Data, want)
	}
}
