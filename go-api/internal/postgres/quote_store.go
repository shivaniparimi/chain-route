package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"chainroute/go-api/internal/payment"
)

func insertPaymentQuotes(ctx context.Context, tx *sql.Tx, paymentID string, quotes []payment.Quote) error {
	for _, q := range quotes {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO payment_quotes
				(payment_id, provider, origin_chain_id, destination_chain_id, asset,
				 input_amount, output_amount, fee_amount, estimated_fill_time_sec,
				 quoted_at, expires_at, raw_provider_payload, selected)
			VALUES ($1, $2, $3, $4, $5, $6::NUMERIC, $7::NUMERIC, $8::NUMERIC, $9, $10, $11, $12::JSONB, $13)
		`, paymentID, q.Provider, q.OriginChainID, q.DestinationChainID, q.Asset,
			q.InputAmount, q.OutputAmount, q.FeeAmount, q.EstimatedFillTimeSec,
			q.QuotedAt, q.ExpiresAt, string(q.RawProviderPayload), q.Selected); err != nil {
			return fmt.Errorf("insert payment_quotes (provider=%s): %w", q.Provider, err)
		}
	}
	return nil
}

// GetQuoteByPaymentID returns the single SELECTED quote row for paymentID
// -- the payment_quotes_one_selected_per_payment partial unique index
// (migration 0007) guarantees there is at most one such row, so this
// keeps returning exactly what it always returned pre-migration: the
// winning quote the Executor validates against and executes. found=false
// means this is a simulated-mode payment, or a testnet-mode payment
// somehow created without one (should be unreachable).
func (s *Store) GetQuoteByPaymentID(ctx context.Context, paymentID string) (payment.Quote, bool, error) {
	var q payment.Quote
	var rawPayload []byte
	row := s.db.QueryRowContext(ctx, `
		SELECT id, payment_id, provider, origin_chain_id, destination_chain_id, asset,
		       input_amount::text, output_amount::text, fee_amount::text, estimated_fill_time_sec,
		       quoted_at, expires_at, raw_provider_payload, created_at, selected
		FROM payment_quotes
		WHERE payment_id = $1 AND selected = true
	`, paymentID)
	err := row.Scan(&q.ID, &q.PaymentID, &q.Provider, &q.OriginChainID, &q.DestinationChainID, &q.Asset,
		&q.InputAmount, &q.OutputAmount, &q.FeeAmount, &q.EstimatedFillTimeSec,
		&q.QuotedAt, &q.ExpiresAt, &rawPayload, &q.CreatedAt, &q.Selected)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Quote{}, false, nil
	}
	if err != nil {
		return payment.Quote{}, false, fmt.Errorf("get quote by payment id: %w", err)
	}
	q.RawProviderPayload = rawPayload
	return q, true, nil
}

// GetQuotesByPaymentID returns every quote row for paymentID (winning and
// losing), selected-first -- consumed only by the dashboard read path
// (Task 3), never by the execution path. Returns an empty (non-nil)
// slice, not an error, when the payment exists but has no quotes
// (simulated mode, or predates migration 0007).
func (s *Store) GetQuotesByPaymentID(ctx context.Context, paymentID string) ([]payment.Quote, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, payment_id, provider, origin_chain_id, destination_chain_id, asset,
		       input_amount::text, output_amount::text, fee_amount::text, estimated_fill_time_sec,
		       quoted_at, expires_at, raw_provider_payload, created_at, selected
		FROM payment_quotes
		WHERE payment_id = $1
		ORDER BY selected DESC, provider
	`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("get quotes by payment id: %w", err)
	}
	defer rows.Close()

	quotes := []payment.Quote{}
	for rows.Next() {
		var q payment.Quote
		var rawPayload []byte
		if err := rows.Scan(&q.ID, &q.PaymentID, &q.Provider, &q.OriginChainID, &q.DestinationChainID, &q.Asset,
			&q.InputAmount, &q.OutputAmount, &q.FeeAmount, &q.EstimatedFillTimeSec,
			&q.QuotedAt, &q.ExpiresAt, &rawPayload, &q.CreatedAt, &q.Selected); err != nil {
			return nil, fmt.Errorf("scan payment_quotes row: %w", err)
		}
		q.RawProviderPayload = rawPayload
		quotes = append(quotes, q)
	}
	return quotes, rows.Err()
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
