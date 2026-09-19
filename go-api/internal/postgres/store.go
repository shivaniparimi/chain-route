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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/observability"
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
	ctx, span := observability.Tracer("db").Start(ctx, "db.GetPayment")
	defer span.End()

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
	ctx, span := observability.Tracer("db").Start(ctx, "db.CreateOrGetPayment")
	defer span.End()

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

	if len(p.Quotes) > 0 {
		if err := insertPaymentQuotes(ctx, tx, created.ID, p.Quotes); err != nil {
			return payment.Payment{}, 0, err
		}
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	outboxPayload, err := json.Marshal(events.RoutedPayment{
		PaymentID:    created.ID,
		EventType:    events.RoutedPaymentEventType,
		OccurredAt:   time.Now().UTC(),
		TraceCarrier: carrier,
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

// ListPayments returns a page of payments (payment-level columns only, no
// quotes/hops -- callers needing those use GetPayment for a single
// payment), newest-first, matching every non-nil filter dimension, plus an
// opaque cursor for the next page ("" when this is the last page). This is
// a single query -- no per-row follow-up queries -- to avoid N+1 query
// patterns on a list endpoint.
//
// The query text is built up dynamically to append zero or more optional
// WHERE clauses, but every filter VALUE is passed as a $N placeholder
// argument to QueryContext, never concatenated into the query text itself
// -- nextArg only ever splices the placeholder's positional name ("$3")
// into the SQL string, and the actual value goes into args, which
// QueryContext binds out-of-band. This is standard parameterized-query
// construction and is not vulnerable to SQL injection: there is no code
// path here where a filter value (status, provider, chain name, cursor
// component, ...) is formatted directly into query via fmt.Sprintf with a
// %s that holds the value itself, only ones that hold "$N".
func (s *Store) ListPayments(ctx context.Context, filter payment.ListFilter) ([]payment.Payment, string, error) {
	ctx, span := observability.Tracer("db").Start(ctx, "db.ListPayments")
	defer span.End()

	query := `
		SELECT id, source_chain, destination_chain, asset, amount::text, status,
		       total_fee, execution_mode, bridge_provider, created_at
		FROM payments
		WHERE 1=1
	`
	args := []any{}
	argN := 0
	nextArg := func(v any) string {
		argN++
		args = append(args, v)
		return fmt.Sprintf("$%d", argN)
	}

	if filter.Cursor != "" {
		createdAt, id, err := payment.DecodeCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		query += fmt.Sprintf(" AND (created_at, id) < (%s, %s)", nextArg(createdAt), nextArg(id))
	}
	if filter.Status != nil {
		query += fmt.Sprintf(" AND status = %s", nextArg(*filter.Status))
	}
	if filter.Provider != nil {
		query += fmt.Sprintf(" AND bridge_provider = %s", nextArg(*filter.Provider))
	}
	if filter.SourceChain != nil {
		query += fmt.Sprintf(" AND source_chain = %s", nextArg(*filter.SourceChain))
	}
	if filter.DestinationChain != nil {
		query += fmt.Sprintf(" AND destination_chain = %s", nextArg(*filter.DestinationChain))
	}
	if filter.ExecutionMode != nil {
		query += fmt.Sprintf(" AND execution_mode = %s", nextArg(*filter.ExecutionMode))
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 25
	}
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT %s", nextArg(limit+1))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list payments: %w", err)
	}
	defer rows.Close()

	var results []payment.Payment
	for rows.Next() {
		var p payment.Payment
		var status, execMode string
		var bridgeProvider sql.NullString
		if err := rows.Scan(&p.ID, &p.SourceChain, &p.DestinationChain, &p.Asset, &p.Amount,
			&status, &p.TotalFee, &execMode, &bridgeProvider, &p.CreatedAt); err != nil {
			return nil, "", fmt.Errorf("scan payment row: %w", err)
		}
		p.Status = payment.Status(status)
		p.ExecutionMode = payment.ExecutionMode(execMode)
		if bridgeProvider.Valid {
			p.BridgeProvider = &bridgeProvider.String
		}
		results = append(results, p)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextCursor := ""
	if len(results) > limit {
		last := results[limit-1]
		nextCursor = payment.EncodeCursor(last.CreatedAt.UTC().Format(time.RFC3339Nano), last.ID)
		results = results[:limit]
	}
	return results, nextCursor, nil
}

// GetDashboardStats computes the read-only dashboard overview counters in
// exactly two aggregation queries -- both aggregating in SQL, never
// fetch-all-then-aggregate-in-Go, to keep this endpoint free of N+1 query
// patterns regardless of table size. The first query aggregates payment
// counts by status plus the average total_fee in one pass; the second
// groups by bridge_provider for the provider-usage breakdown (a variable
// number of rows, so it can't be folded into the fixed-shape first query).
func (s *Store) GetDashboardStats(ctx context.Context) (payment.DashboardStats, error) {
	var stats payment.DashboardStats
	stats.ProviderUsage = map[string]int64{}

	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed,
			COUNT(*) FILTER (WHERE status IN ('ROUTED', 'PROCESSING', 'SUBMITTED')) AS processing,
			COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
			COALESCE(AVG(total_fee), 0) AS avg_fee
		FROM payments
	`)
	if err := row.Scan(&stats.TotalPayments, &stats.CompletedPayments, &stats.ProcessingPayments, &stats.FailedPayments, &stats.AverageRoutingCost); err != nil {
		return payment.DashboardStats{}, fmt.Errorf("get dashboard stats: %w", err)
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT bridge_provider, COUNT(*)
		FROM payments
		WHERE bridge_provider IS NOT NULL
		GROUP BY bridge_provider
	`)
	if err != nil {
		return payment.DashboardStats{}, fmt.Errorf("get provider usage: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var provider string
		var count int64
		if err := rows.Scan(&provider, &count); err != nil {
			return payment.DashboardStats{}, fmt.Errorf("scan provider usage row: %w", err)
		}
		stats.ProviderUsage[provider] = count
	}
	return stats, rows.Err()
}

// GetTimeseries buckets payments by created_at (truncated to the given
// interval, "hour" or "day") over the trailing `days` days, returning one
// row per non-empty bucket with the payment count and the requested
// metric's aggregate ("volume" -> COUNT(*), "routing_cost" ->
// COALESCE(AVG(total_fee), 0)) -- a single aggregation query, never
// fetch-all-then-aggregate-in-Go, to keep this endpoint free of N+1 query
// patterns.
//
// aggregateExpr is spliced into the query TEXT via fmt.Sprintf, but it is
// chosen from a fixed, code-controlled 2-value set based on metric, which
// the caller (handler.GetDashboardTimeseries) has already validated
// against validTimeseriesMetrics ({"volume", "routing_cost"}) before this
// method is ever reachable -- metric's raw string value is never itself
// interpolated into the query, only used as a Go switch key that selects
// one of the two literal SQL fragments below. interval and days, the
// actual caller/user-influenced values, are passed as $1/$2 query
// arguments, which QueryContext binds out-of-band -- never concatenated
// into the query text. This mirrors ListPayments' nextArg convention
// (see its doc comment) and is not vulnerable to SQL injection.
func (s *Store) GetTimeseries(ctx context.Context, metric, interval string, days int) ([]payment.TimeseriesPoint, error) {
	aggregateExpr := "COUNT(*)"
	if metric == "routing_cost" {
		aggregateExpr = "COALESCE(AVG(total_fee), 0)"
	}
	// The brief's sketch built the trailing-window cutoff as
	// (($2 || ' days')::interval, i.e. concatenating the $2 placeholder
	// (a Go int) with a text literal via ||. That leaves Postgres/pgx
	// unable to infer $2's type ("failed to encode args[1] ... cannot
	// find encode plan" at runtime) -- caught by this package's own
	// integration tests. make_interval(days => $2) sidesteps the
	// ambiguity by taking the integer directly, matching the same fix
	// StalePaymentIDs already applies to an identical problem via
	// make_interval(secs => $2) above.
	query := fmt.Sprintf(`
		SELECT date_trunc($1, created_at) AS bucket, COUNT(*), %s
		FROM payments
		WHERE created_at > now() - make_interval(days => $2)
		GROUP BY bucket
		ORDER BY bucket
	`, aggregateExpr)

	rows, err := s.db.QueryContext(ctx, query, interval, days)
	if err != nil {
		return nil, fmt.Errorf("get timeseries: %w", err)
	}
	defer rows.Close()

	points := []payment.TimeseriesPoint{}
	for rows.Next() {
		var p payment.TimeseriesPoint
		if err := rows.Scan(&p.Bucket, &p.Count, &p.Value); err != nil {
			return nil, fmt.Errorf("scan timeseries row: %w", err)
		}
		points = append(points, p)
	}
	return points, rows.Err()
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
// given terminal status, and returns the payment's created_at timestamp
// (needed by callers to compute end-to-end payment-duration metrics).
// completed=false means the payment was not PROCESSING -- a safe no-op. This
// is the sole mechanism guaranteeing exactly one terminal outcome is ever
// persisted per payment (Phase 6 design spec, §7 case 4), regardless of how
// many times the caller's execution logic itself ran.
func (s *Store) CompletePayment(ctx context.Context, paymentID string, terminal payment.Status) (completed bool, createdAt time.Time, err error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE payments SET status = $2, completed_at = now(), updated_at = now()
		WHERE id = $1 AND status = $3
		RETURNING created_at
	`, paymentID, terminal, payment.StatusProcessing)
	if err := row.Scan(&createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, time.Time{}, nil
		}
		return false, time.Time{}, fmt.Errorf("complete payment: %w", err)
	}
	return true, createdAt, nil
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
