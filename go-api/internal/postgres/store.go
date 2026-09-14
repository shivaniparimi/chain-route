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

// LookupByIdempotencyKey checks whether a payment already exists for this
// idempotency key WITHOUT calling the routing service. This lets a caller
// resolve a retry (Replayed or Conflict) before paying for a routing RPC --
// the same optimization CreateOrGetPayment performs internally, exposed
// separately so the HTTP handler can check before routing rather than
// after. found=false means no row exists yet and the caller should proceed
// to routing.
func (s *Store) LookupByIdempotencyKey(ctx context.Context, p payment.Payment) (result payment.Payment, outcome payment.CreateResult, found bool, err error) {
	existing, matches, found, err := s.findByIdempotencyKey(ctx, p)
	if err != nil {
		return payment.Payment{}, 0, false, err
	}
	if !found {
		return payment.Payment{}, 0, false, nil
	}
	if !matches {
		return payment.Payment{}, payment.Conflict, true, nil
	}
	hops, err := s.hopsForPayment(ctx, existing.ID)
	if err != nil {
		return payment.Payment{}, 0, false, err
	}
	existing.Hops = hops
	return existing, payment.Replayed, true, nil
}

func (s *Store) CreateOrGetPayment(ctx context.Context, p payment.Payment) (payment.Payment, payment.CreateResult, error) {
	// Optimization only, not correctness-critical: skip the routing RPC's
	// result entirely for the common retry case. If this races with a
	// concurrent insert and misses it, nothing breaks -- the INSERT below
	// is what actually enforces correctness.
	if existing, matches, found, err := s.findByIdempotencyKey(ctx, p); err != nil {
		return payment.Payment{}, 0, err
	} else if found {
		if !matches {
			return payment.Payment{}, payment.Conflict, nil
		}
		hops, err := s.hopsForPayment(ctx, existing.ID)
		if err != nil {
			return payment.Payment{}, 0, err
		}
		existing.Hops = hops
		return existing, payment.Replayed, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return payment.Payment{}, 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var created payment.Payment
	row := tx.QueryRowContext(ctx, `
		INSERT INTO payments (idempotency_key, source_chain, destination_chain, asset, amount, total_fee)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, amount::text, created_at, updated_at
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount, p.TotalFee)

	err = row.Scan(&created.ID, &created.Amount, &created.CreatedAt, &created.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		// Lost the race: a concurrent request already created this key.
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return payment.Payment{}, 0, fmt.Errorf("rollback after lost race: %w", rbErr)
		}
		existing, matches, found, ferr := s.findByIdempotencyKey(ctx, p)
		if ferr != nil {
			return payment.Payment{}, 0, ferr
		}
		if !found {
			return payment.Payment{}, 0, errors.New("idempotency key conflicted but no row found on re-read")
		}
		if !matches {
			return payment.Payment{}, payment.Conflict, nil
		}
		hops, herr := s.hopsForPayment(ctx, existing.ID)
		if herr != nil {
			return payment.Payment{}, 0, herr
		}
		existing.Hops = hops
		return existing, payment.Replayed, nil
	}
	if err != nil {
		return payment.Payment{}, 0, fmt.Errorf("insert payment: %w", err)
	}

	for _, h := range p.Hops {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO payment_route_hops
				(payment_id, hop_index, from_chain, to_chain, bridge_name, fee, latency_ms, liquidity, reliability)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`, created.ID, h.HopIndex, h.FromChain, h.ToChain, h.BridgeName, h.Fee, h.LatencyMs, h.Liquidity, h.Reliability); err != nil {
			return payment.Payment{}, 0, fmt.Errorf("insert hop %d: %w", h.HopIndex, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return payment.Payment{}, 0, fmt.Errorf("commit: %w", err)
	}

	created.IdempotencyKey = p.IdempotencyKey
	created.SourceChain = p.SourceChain
	created.DestinationChain = p.DestinationChain
	created.Asset = p.Asset
	created.Status = payment.StatusRouted
	created.TotalFee = p.TotalFee
	created.Hops = p.Hops

	return created, payment.Created, nil
}
