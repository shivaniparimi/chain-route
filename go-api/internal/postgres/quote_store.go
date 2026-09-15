package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"chainroute/go-api/internal/payment"
)

func insertPaymentQuote(ctx context.Context, tx *sql.Tx, paymentID string, q *payment.Quote) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO payment_quotes
			(payment_id, provider, origin_chain_id, destination_chain_id, asset,
			 input_amount, output_amount, fee_amount, estimated_fill_time_sec,
			 quoted_at, expires_at, raw_provider_payload)
		VALUES ($1, $2, $3, $4, $5, $6::NUMERIC, $7::NUMERIC, $8::NUMERIC, $9, $10, $11, $12::JSONB)
	`, paymentID, q.Provider, q.OriginChainID, q.DestinationChainID, q.Asset,
		q.InputAmount, q.OutputAmount, q.FeeAmount, q.EstimatedFillTimeSec,
		q.QuotedAt, q.ExpiresAt, string(q.RawProviderPayload))
	if err != nil {
		return fmt.Errorf("insert payment_quotes: %w", err)
	}
	return nil
}

// GetQuoteByPaymentID returns the (at most one, per UNIQUE(payment_id))
// quote row for paymentID. found=false means this is a simulated-mode
// payment, or a testnet-mode payment somehow created without one (should
// be unreachable once Task 10 is wired -- CreateOrGetPayment always
// writes both together for testnet mode).
func (s *Store) GetQuoteByPaymentID(ctx context.Context, paymentID string) (payment.Quote, bool, error) {
	var q payment.Quote
	var rawPayload []byte
	row := s.db.QueryRowContext(ctx, `
		SELECT id, payment_id, provider, origin_chain_id, destination_chain_id, asset,
		       input_amount::text, output_amount::text, fee_amount::text, estimated_fill_time_sec,
		       quoted_at, expires_at, raw_provider_payload, created_at
		FROM payment_quotes
		WHERE payment_id = $1
	`, paymentID)
	err := row.Scan(&q.ID, &q.PaymentID, &q.Provider, &q.OriginChainID, &q.DestinationChainID, &q.Asset,
		&q.InputAmount, &q.OutputAmount, &q.FeeAmount, &q.EstimatedFillTimeSec,
		&q.QuotedAt, &q.ExpiresAt, &rawPayload, &q.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Quote{}, false, nil
	}
	if err != nil {
		return payment.Quote{}, false, fmt.Errorf("get quote by payment id: %w", err)
	}
	q.RawProviderPayload = rawPayload
	return q, true, nil
}

// MarkProcessingFailed transitions paymentID from PROCESSING to FAILED
// with a recorded reason, guarded by the same conditional-UPDATE pattern
// every other status transition in this package uses. ok=false means the
// payment was not PROCESSING (already terminal, or still ROUTED) -- a
// safe no-op the caller must not treat as an error, mirroring
// CompletePayment/MarkSubmitted's own convention.
func (s *Store) MarkProcessingFailed(ctx context.Context, paymentID, reason string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE payments
		SET status = 'FAILED', failure_reason = $2, updated_at = now()
		WHERE id = $1 AND status = 'PROCESSING'
	`, paymentID, reason)
	if err != nil {
		return false, fmt.Errorf("mark processing failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark processing failed: rows affected: %w", err)
	}
	return n == 1, nil
}
