package relay

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"chainroute/go-api/internal/bridge/quote"
)

func (p *Provider) BuildTransaction(ctx context.Context, freshQuote quote.Quote) (quote.TxEnvelope, error) {
	var payload QuotePayload
	if err := json.Unmarshal(freshQuote.RawProviderPayload, &payload); err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("relay: decode quote payload: %w", err)
	}

	if payload.Recipient == "" || common.HexToAddress(payload.Recipient) != p.WalletAddress {
		return quote.TxEnvelope{}, fmt.Errorf("relay: quote payload has no verifiable recipient (got %q) -- refusing to build a transaction without recipient verification", payload.Recipient)
	}

	value, ok := new(big.Int).SetString(payload.Value, 10)
	if !ok {
		return quote.TxEnvelope{}, fmt.Errorf("relay: value %q is not a valid integer", payload.Value)
	}
	dataHex := strings.TrimPrefix(payload.Data, "0x")
	if len(dataHex)%2 != 0 {
		dataHex = "0" + dataHex
	}
	data, err := hex.DecodeString(dataHex)
	if err != nil {
		return quote.TxEnvelope{}, fmt.Errorf("relay: data %q is not valid hex: %w", payload.Data, err)
	}

	return quote.TxEnvelope{To: common.HexToAddress(payload.To), Value: value, Data: data, ChainID: payload.ChainID}, nil
}
