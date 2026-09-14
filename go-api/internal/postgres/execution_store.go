package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"chainroute/go-api/internal/payment"
)

// SeedWalletNonce inserts the wallet's starting nonce exactly once. A
// second call for the same wallet is a safe no-op -- this is intended to
// be called at every worker startup with the chain's own
// eth_getTransactionCount(wallet, "pending") value; only the very first
// call across the wallet's lifetime actually takes effect (Phase 7 design
// spec §7): wallet_nonces.next_nonce is never overwritten from a chain
// read after that.
func (s *Store) SeedWalletNonce(ctx context.Context, walletAddress string, initialNonce int64) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO wallet_nonces (wallet_address, next_nonce)
		VALUES ($1, $2)
		ON CONFLICT (wallet_address) DO NOTHING
	`, walletAddress, initialNonce); err != nil {
		return fmt.Errorf("seed wallet nonce: %w", err)
	}
	return nil
}

// CreateExecutionParams is everything TryCreateExecution needs to allocate
// a nonce and create a payment's execution row. Phase 7 has exactly one
// wallet/bridge/route, so these are typically the same constants on every
// call; they are still parameters (not hardcoded) so tests can use
// isolated wallet addresses without colliding.
type CreateExecutionParams struct {
	PaymentID          string
	WalletAddress      string
	BridgeProvider     string
	OriginChainID      int64
	DestinationChainID int64
}

// executionExistsConstraint is Postgres's default auto-generated name for
// an inline UNIQUE(payment_id) column constraint on payment_executions
// (verified against the actual migrated schema in Task 4, Step 2).
const executionExistsConstraint = "payment_executions_payment_id_key"

// TryCreateExecution atomically allocates the next nonce for
// p.WalletAddress and creates p.PaymentID's payment_executions row, in one
// Postgres transaction (Phase 7 design spec §8, §15): the row is only ever
// inserted already carrying its allocated nonce, so there is no durable
// intermediate state where a nonce is allocated but no execution row
// exists for it.
//
// created=false, err=nil means UNIQUE(payment_id) rejected this attempt
// because another actor (the original executor, or a concurrent
// reconciler tick) already created the execution row for this payment
// first -- this is the expected, safe outcome of the executor/reconciler
// race described in the design spec §15, not an error. Because the nonce
// UPDATE and the row INSERT are one transaction, losing this race rolls
// back the nonce allocation too: a lost race never burns a nonce.
func (s *Store) TryCreateExecution(ctx context.Context, p CreateExecutionParams) (exec payment.Execution, created bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return payment.Execution{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var nonce int64
	nonceRow := tx.QueryRowContext(ctx, `
		UPDATE wallet_nonces SET next_nonce = next_nonce + 1
		WHERE wallet_address = $1
		RETURNING next_nonce - 1
	`, p.WalletAddress)
	if err := nonceRow.Scan(&nonce); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return payment.Execution{}, false, fmt.Errorf("wallet %s has no seeded wallet_nonces row -- SeedWalletNonce must run at worker startup before any execution is attempted", p.WalletAddress)
		}
		return payment.Execution{}, false, fmt.Errorf("allocate nonce: %w", err)
	}

	var externalStatus string
	insertRow := tx.QueryRowContext(ctx, `
		INSERT INTO payment_executions
			(payment_id, bridge_provider, origin_chain_id, destination_chain_id, wallet_address, nonce)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, external_status, created_at, updated_at
	`, p.PaymentID, p.BridgeProvider, p.OriginChainID, p.DestinationChainID, p.WalletAddress, nonce)
	if err := insertRow.Scan(&exec.ID, &externalStatus, &exec.CreatedAt, &exec.UpdatedAt); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == executionExistsConstraint {
			return payment.Execution{}, false, nil
		}
		return payment.Execution{}, false, fmt.Errorf("insert execution: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return payment.Execution{}, false, fmt.Errorf("commit: %w", err)
	}

	exec.PaymentID = p.PaymentID
	exec.BridgeProvider = p.BridgeProvider
	exec.OriginChainID = p.OriginChainID
	exec.DestinationChainID = p.DestinationChainID
	exec.WalletAddress = p.WalletAddress
	exec.Nonce = nonce
	exec.ExternalStatus = payment.ExternalStatus(externalStatus)
	return exec, true, nil
}

// GetExecutionByPaymentID returns the (at most one, per UNIQUE(payment_id))
// execution row for paymentID. found=false means testnet execution has not
// started for this payment yet, or it is a simulated-mode payment.
func (s *Store) GetExecutionByPaymentID(ctx context.Context, paymentID string) (payment.Execution, bool, error) {
	var e payment.Execution
	var signedTxHash, acrossDepositID sql.NullString
	var broadcastAt, confirmedAt sql.NullTime
	var externalStatus string
	row := s.db.QueryRowContext(ctx, `
		SELECT id, payment_id, bridge_provider, origin_chain_id, destination_chain_id,
		       wallet_address, nonce, signed_tx_hash, raw_signed_tx, broadcast_at,
		       across_deposit_id, external_status, confirmed_at, created_at, updated_at
		FROM payment_executions
		WHERE payment_id = $1
	`, paymentID)
	err := row.Scan(&e.ID, &e.PaymentID, &e.BridgeProvider, &e.OriginChainID, &e.DestinationChainID,
		&e.WalletAddress, &e.Nonce, &signedTxHash, &e.RawSignedTx, &broadcastAt,
		&acrossDepositID, &externalStatus, &confirmedAt, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return payment.Execution{}, false, nil
	}
	if err != nil {
		return payment.Execution{}, false, fmt.Errorf("get execution by payment id: %w", err)
	}
	if signedTxHash.Valid {
		e.SignedTxHash = &signedTxHash.String
	}
	if acrossDepositID.Valid {
		e.AcrossDepositID = &acrossDepositID.String
	}
	if broadcastAt.Valid {
		e.BroadcastAt = &broadcastAt.Time
	}
	if confirmedAt.Valid {
		e.ConfirmedAt = &confirmedAt.Time
	}
	e.ExternalStatus = payment.ExternalStatus(externalStatus)
	return e, true, nil
}

// PersistSignedExecution durably persists the signed transaction bytes and
// its deterministic hash BEFORE any broadcast attempt -- this ordering is
// the core of Phase 7's crash-safety (design spec §8 step 2).
func (s *Store) PersistSignedExecution(ctx context.Context, executionID string, rawTx []byte, txHash string) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payment_executions SET raw_signed_tx = $2, signed_tx_hash = $3, updated_at = now()
		WHERE id = $1
	`, executionID, rawTx, txHash); err != nil {
		return fmt.Errorf("persist signed execution: %w", err)
	}
	return nil
}

// MarkExecutionBroadcast records that eth_sendRawTransaction was actually
// attempted for this execution -- called only after PersistSignedExecution
// has already committed.
func (s *Store) MarkExecutionBroadcast(ctx context.Context, executionID string) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payment_executions SET broadcast_at = now(), updated_at = now()
		WHERE id = $1
	`, executionID); err != nil {
		return fmt.Errorf("mark execution broadcast: %w", err)
	}
	return nil
}

// UpdateExecutionExternalStatus records the reconciler's observed terminal
// (or still-pending) outcome for one execution.
func (s *Store) UpdateExecutionExternalStatus(ctx context.Context, executionID string, status payment.ExternalStatus, confirmedAt *sql.NullTime) error {
	if _, err := s.db.ExecContext(ctx, `
		UPDATE payment_executions SET external_status = $2, confirmed_at = $3, updated_at = now()
		WHERE id = $1
	`, executionID, string(status), confirmedAt); err != nil {
		return fmt.Errorf("update execution external status: %w", err)
	}
	return nil
}

// MarkSubmitted transitions a testnet-mode payment from PROCESSING to
// SUBMITTED once its transaction has actually been broadcast (design spec
// §7, §10). submitted=false means the payment was not PROCESSING -- a safe
// no-op, mirroring ClaimPayment/CompletePayment's own guard pattern.
func (s *Store) MarkSubmitted(ctx context.Context, paymentID string) (submitted bool, err error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE payments SET status = $2, updated_at = now()
		WHERE id = $1 AND status = $3
	`, paymentID, payment.StatusSubmitted, payment.StatusProcessing)
	if err != nil {
		return false, fmt.Errorf("mark submitted: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("mark submitted rows affected: %w", err)
	}
	return rows == 1, nil
}

// CompleteSubmittedPayment atomically transitions a testnet-mode payment
// from SUBMITTED to a terminal status, mirroring CompletePayment's
// PROCESSING->terminal guard exactly, but for the SUBMITTED->terminal edge
// that only testnet-mode payments ever traverse.
func (s *Store) CompleteSubmittedPayment(ctx context.Context, paymentID string, terminal payment.Status) (completed bool, err error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE payments SET status = $2, completed_at = now(), updated_at = now()
		WHERE id = $1 AND status = $3
	`, paymentID, terminal, payment.StatusSubmitted)
	if err != nil {
		return false, fmt.Errorf("complete submitted payment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("complete submitted payment rows affected: %w", err)
	}
	return rows == 1, nil
}

// StaleTestnetProcessingWithoutExecutionIDs returns testnet-mode payments
// stuck at crash point A (design spec §12): PROCESSING, stale, with no
// payment_executions row at all. Nothing else in the system revisits such
// a payment (Kafka redelivery no-ops per ClaimPayment's ROUTED-only guard;
// Recovery is simulated-mode-only per Task 5) -- this is the sole recovery
// path for it (design spec §15).
func (s *Store) StaleTestnetProcessingWithoutExecutionIDs(ctx context.Context, staleness time.Duration) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM payments
		WHERE execution_mode = $1
		  AND status = $2
		  AND updated_at < now() - make_interval(secs => $3)
		  AND NOT EXISTS (
		      SELECT 1 FROM payment_executions WHERE payment_executions.payment_id = payments.id
		  )
	`, string(payment.ExecutionModeTestnet), string(payment.StatusProcessing), staleness.Seconds())
	if err != nil {
		return nil, fmt.Errorf("query stale testnet processing: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan stale testnet processing id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ReconciliationCandidates returns every payment_executions row the
// reconciler should consider: already-broadcast-but-unconfirmed rows
// (checked every tick, no staleness needed), and not-yet-broadcast rows
// that have sat without progress longer than staleness (design spec §15).
func (s *Store) ReconciliationCandidates(ctx context.Context, staleness time.Duration) ([]payment.Execution, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, payment_id, bridge_provider, origin_chain_id, destination_chain_id,
		       wallet_address, nonce, signed_tx_hash, raw_signed_tx, broadcast_at,
		       across_deposit_id, external_status, confirmed_at, created_at, updated_at
		FROM payment_executions
		WHERE (broadcast_at IS NOT NULL AND confirmed_at IS NULL)
		   OR (broadcast_at IS NULL AND updated_at < now() - make_interval(secs => $1))
	`, staleness.Seconds())
	if err != nil {
		return nil, fmt.Errorf("query reconciliation candidates: %w", err)
	}
	defer rows.Close()

	var out []payment.Execution
	for rows.Next() {
		var e payment.Execution
		var signedTxHash, acrossDepositID sql.NullString
		var broadcastAt, confirmedAt sql.NullTime
		var externalStatus string
		if err := rows.Scan(&e.ID, &e.PaymentID, &e.BridgeProvider, &e.OriginChainID, &e.DestinationChainID,
			&e.WalletAddress, &e.Nonce, &signedTxHash, &e.RawSignedTx, &broadcastAt,
			&acrossDepositID, &externalStatus, &confirmedAt, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan reconciliation candidate: %w", err)
		}
		if signedTxHash.Valid {
			e.SignedTxHash = &signedTxHash.String
		}
		if acrossDepositID.Valid {
			e.AcrossDepositID = &acrossDepositID.String
		}
		if broadcastAt.Valid {
			e.BroadcastAt = &broadcastAt.Time
		}
		if confirmedAt.Valid {
			e.ConfirmedAt = &confirmedAt.Time
		}
		e.ExternalStatus = payment.ExternalStatus(externalStatus)
		out = append(out, e)
	}
	return out, rows.Err()
}

// LowestUnconfirmedNonce returns the lowest nonce among walletAddress's
// unconfirmed executions (confirmed_at IS NULL, regardless of broadcast
// state) -- used both for the reconciler's lowest-nonce-first
// prioritization and its read-only chain-divergence check (design spec
// §15). found=false means the wallet has no unconfirmed executions.
func (s *Store) LowestUnconfirmedNonce(ctx context.Context, walletAddress string) (nonce int64, found bool, err error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT MIN(nonce) FROM payment_executions
		WHERE wallet_address = $1 AND confirmed_at IS NULL
	`, walletAddress)
	var n sql.NullInt64
	if err := row.Scan(&n); err != nil {
		return 0, false, fmt.Errorf("lowest unconfirmed nonce: %w", err)
	}
	if !n.Valid {
		return 0, false, nil
	}
	return n.Int64, true, nil
}
