package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/payment"
)

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// normalizeExecutionMode maps the Go zero value "" (which every Phase 1-6
// call site produces, since ExecutionMode didn't exist before Phase 7) to
// ExecutionModeSimulated before it ever reaches SQL. The payments.execution_mode
// column is NOT NULL with a CHECK constraint, so inserting "" directly would
// violate it.
func normalizeExecutionMode(m payment.ExecutionMode) payment.ExecutionMode {
	if m == "" {
		return payment.ExecutionModeSimulated
	}
	return m
}

// findByIdempotencyKey looks up an existing payment by idempotency key,
// reporting via Postgres's own NUMERIC equality whether it matches the
// given candidate's request fields. found=false means no row exists.
func (s *Store) findByIdempotencyKey(ctx context.Context, p payment.Payment) (existing payment.Payment, matches bool, found bool, err error) {
	mode := normalizeExecutionMode(p.ExecutionMode)
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_chain, destination_chain, asset, amount::text,
		       total_fee, status, execution_mode, bridge_provider, failure_reason, completed_at, created_at, updated_at,
		       (source_chain = $2 AND destination_chain = $3
		        AND asset = $4 AND amount = $5::NUMERIC AND execution_mode = $6) AS request_matches
		FROM payments
		WHERE idempotency_key = $1
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount, string(mode))

	var status, execMode string
	var bridgeProvider sql.NullString
	var failureReason sql.NullString
	var completedAt sql.NullTime
	err = row.Scan(&existing.ID, &existing.SourceChain, &existing.DestinationChain,
		&existing.Asset, &existing.Amount, &existing.TotalFee, &status, &execMode, &bridgeProvider,
		&failureReason, &completedAt, &existing.CreatedAt, &existing.UpdatedAt, &matches)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Payment{}, false, false, nil
	}
	if err != nil {
		return payment.Payment{}, false, false, fmt.Errorf("find by idempotency key: %w", err)
	}
	existing.Status = payment.Status(status)
	existing.ExecutionMode = payment.ExecutionMode(execMode)
	if bridgeProvider.Valid {
		existing.BridgeProvider = &bridgeProvider.String
	}
	if failureReason.Valid {
		existing.FailureReason = &failureReason.String
	}
	if completedAt.Valid {
		existing.CompletedAt = &completedAt.Time
	}
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
	var status, execMode string
	var bridgeProvider sql.NullString
	var failureReason sql.NullString
	var completedAt sql.NullTime
	row := s.db.QueryRowContext(ctx, `
		SELECT id, idempotency_key, source_chain, destination_chain, asset, amount::text,
		       total_fee, status, execution_mode, bridge_provider, failure_reason, completed_at, created_at, updated_at
		FROM payments
		WHERE id = $1
	`, id)
	err := row.Scan(&p.ID, &p.IdempotencyKey, &p.SourceChain, &p.DestinationChain, &p.Asset,
		&p.Amount, &p.TotalFee, &status, &execMode, &bridgeProvider, &failureReason, &completedAt, &p.CreatedAt, &p.UpdatedAt)
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
	p.ExecutionMode = payment.ExecutionMode(execMode)
	if bridgeProvider.Valid {
		p.BridgeProvider = &bridgeProvider.String
	}
	if failureReason.Valid {
		p.FailureReason = &failureReason.String
	}
	if completedAt.Valid {
		p.CompletedAt = &completedAt.Time
	}

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
	p.ExecutionMode = normalizeExecutionMode(p.ExecutionMode)

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
		INSERT INTO payments (idempotency_key, source_chain, destination_chain, asset, amount, total_fee, execution_mode, bridge_provider)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, amount::text, created_at, updated_at
	`, p.IdempotencyKey, p.SourceChain, p.DestinationChain, p.Asset, p.Amount, p.TotalFee, string(p.ExecutionMode), p.BridgeProvider)

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

	if p.Quote != nil {
		if err := insertPaymentQuote(ctx, tx, created.ID, p.Quote); err != nil {
			return payment.Payment{}, 0, err
		}
	}

	outboxPayload, err := json.Marshal(events.RoutedPayment{
		PaymentID:  created.ID,
		EventType:  events.RoutedPaymentEventType,
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return payment.Payment{}, 0, fmt.Errorf("marshal outbox payload: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events (payment_id, event_type, payload)
		VALUES ($1, $2, $3)
	`, created.ID, events.RoutedPaymentEventType, outboxPayload); err != nil {
		return payment.Payment{}, 0, fmt.Errorf("insert outbox event: %w", err)
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
	created.ExecutionMode = p.ExecutionMode
	created.BridgeProvider = p.BridgeProvider

	return created, payment.Created, nil
}

// ClaimPayment atomically transitions a payment from ROUTED to PROCESSING,
// returning its execution_mode in the same round trip. claimed=false means
// the payment was not ROUTED (already claimed by another delivery, or in
// some other state) -- a safe no-op, not an error; mode is the zero value
// in that case and must not be used. This is the sole mechanism preventing
// duplicate claims (see the Phase 6 design spec, §7 case 2).
func (s *Store) ClaimPayment(ctx context.Context, paymentID string) (claimed bool, mode payment.ExecutionMode, err error) {
	var modeStr string
	row := s.db.QueryRowContext(ctx, `
		UPDATE payments SET status = $2, updated_at = now()
		WHERE id = $1 AND status = $3
		RETURNING execution_mode
	`, paymentID, payment.StatusProcessing, payment.StatusRouted)
	err = row.Scan(&modeStr)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("claim payment: %w", err)
	}
	return true, payment.ExecutionMode(modeStr), nil
}

// CompletePayment atomically transitions a payment from PROCESSING to the
// given terminal status. completed=false means the payment was not
// PROCESSING -- a safe no-op. This is the sole mechanism guaranteeing
// exactly one terminal outcome is ever persisted per payment (Phase 6
// design spec, §7 case 4), regardless of how many times the caller's
// execution logic itself ran.
func (s *Store) CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (completed bool, err error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE payments SET status = $2, completed_at = now(), updated_at = now()
		WHERE id = $1 AND status = $3
	`, paymentID, terminal, payment.StatusProcessing)
	if err != nil {
		return false, fmt.Errorf("complete payment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("complete payment rows affected: %w", err)
	}
	return rows == 1, nil
}

// StalePaymentIDs returns the IDs of payments that have been PROCESSING for
// longer than staleness. The caller (the worker's recovery sweep) is
// responsible for re-running execution and calling CompletePayment for
// each -- this method only identifies candidates. Only simulated-mode
// payments are returned: Recovery calls the pure, deterministic
// execution.Execute, which has no meaning for a real testnet transaction
// (Phase 7 design spec §15), so testnet-mode payments must never be swept
// here.
func (s *Store) StalePaymentIDs(ctx context.Context, staleness time.Duration) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM payments
		WHERE status = $1
		  AND execution_mode = $3
		  AND updated_at < now() - make_interval(secs => $2)
	`, payment.StatusProcessing, staleness.Seconds(), string(payment.ExecutionModeSimulated))
	if err != nil {
		return nil, fmt.Errorf("query stale payments: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan stale payment id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// OutboxEvent is one row from outbox_events, as seen by a publisher.
type OutboxEvent struct {
	ID        string
	PaymentID string
	EventType string
	Payload   []byte
}

// PublishNextOutboxEvent claims the oldest unpublished outbox row via
// SELECT ... FOR UPDATE SKIP LOCKED, invokes publish with it while the
// claiming transaction is held open, and marks the row published only if
// publish succeeds. published=false, err=nil means there was nothing to
// claim. If publish returns an error, the transaction rolls back, the row
// stays unpublished, and a later call will retry it -- this is the
// accepted at-least-once mechanism (Phase 6 design spec §5, §6): a
// duplicate publish is possible and is why consumers must be idempotent.
//
// The transaction deliberately spans the publish call (an accepted
// tradeoff, not an oversight -- see the design spec §5): a slow or
// unavailable Kafka broker will hold one row lock and one pooled
// connection for the duration of that call. No claimed_at/lease columns
// are added to decouple this, since SKIP LOCKED already provides mutual
// exclusion and automatically releases the lock if this process crashes
// mid-transaction.
func (s *Store) PublishNextOutboxEvent(ctx context.Context, publish func(OutboxEvent) error) (published bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var evt OutboxEvent
	row := tx.QueryRowContext(ctx, `
		SELECT id, payment_id, event_type, payload
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`)
	if err := row.Scan(&evt.ID, &evt.PaymentID, &evt.EventType, &evt.Payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("claim outbox event: %w", err)
	}

	if err := publish(evt); err != nil {
		return false, fmt.Errorf("publish outbox event: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = $1`, evt.ID); err != nil {
		return false, fmt.Errorf("mark outbox event published: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit outbox publish: %w", err)
	}

	return true, nil
}
