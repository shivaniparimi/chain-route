package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/payment"
)

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// findByIdempotencyKey looks up an existing payment by idempotency key,
// reporting via Postgres's own NUMERIC equality whether it matches the
// given candidate's request fields. found=false means no row exists.
func (s *Store) findByIdempotencyKey(ctx context.Context, p payment.Payment) (existing payment.Payment, matches bool, found bool, err error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_chain, destination_chain, asset, amount::text,
		       total_fee, status, created_at, updated_at,
		       (source_chain = $2 AND destination_chain = $3
		        AND asset = $4 AND amount = $5::NUMERIC) AS request_matches
		FROM payments
		WHERE idempotency_key = $1
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount)

	var status string
	err = row.Scan(&existing.ID, &existing.SourceChain, &existing.DestinationChain,
		&existing.Asset, &existing.Amount, &existing.TotalFee, &status,
		&existing.CreatedAt, &existing.UpdatedAt, &matches)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Payment{}, false, false, nil
	}
	if err != nil {
		return payment.Payment{}, false, false, fmt.Errorf("find by idempotency key: %w", err)
	}
	existing.Status = payment.Status(status)
	existing.IdempotencyKey = p.IdempotencyKey
	return existing, matches, true, nil
}

func (s *Store) hopsForPayment(ctx context.Context, paymentID string) ([]payment.Hop, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT hop_index, from_chain, to_chain, bridge_name, fee, latency_ms, liquidity, reliability
		FROM payment_route_hops
		WHERE payment_id = $1
		ORDER BY hop_index
	`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("query hops: %w", err)
	}
	defer rows.Close()

	var hops []payment.Hop
	for rows.Next() {
		var h payment.Hop
		if err := rows.Scan(&h.HopIndex, &h.FromChain, &h.ToChain, &h.BridgeName,
			&h.Fee, &h.LatencyMs, &h.Liquidity, &h.Reliability); err != nil {
			return nil, fmt.Errorf("scan hop: %w", err)
		}
		hops = append(hops, h)
	}
	return hops, rows.Err()
}

func (s *Store) GetPayment(ctx context.Context, id string) (payment.Payment, bool, error) {
	var p payment.Payment
	var status string
	row := s.db.QueryRowContext(ctx, `
		SELECT id, idempotency_key, source_chain, destination_chain, asset, amount::text,
		       total_fee, status, created_at, updated_at
		FROM payments
		WHERE id = $1
	`, id)
	err := row.Scan(&p.ID, &p.IdempotencyKey, &p.SourceChain, &p.DestinationChain, &p.Asset,
		&p.Amount, &p.TotalFee, &status, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Payment{}, false, nil
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "22P02" { // invalid_text_representation: malformed UUID
			return payment.Payment{}, false, nil
		}
		return payment.Payment{}, false, fmt.Errorf("get payment: %w", err)
	}
	p.Status = payment.Status(status)

	hops, err := s.hopsForPayment(ctx, p.ID)
	if err != nil {
		return payment.Payment{}, false, err
	}
	p.Hops = hops

	return p, true, nil
}
