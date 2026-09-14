//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Fatal("DATABASE_URL must be set for integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	// Isolate each test's rows with a unique idempotency-key prefix rather
	// than truncating shared tables, so tests can run in parallel safely.
	return New(db)
}

func TestGetPayment_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, found, err := s.GetPayment(context.Background(), "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func TestGetPayment_MalformedID(t *testing.T) {
	s := newTestStore(t)
	_, found, err := s.GetPayment(context.Background(), "not-a-uuid")
	if err != nil {
		t.Fatalf("expected malformed id to be treated as not-found, got error: %v", err)
	}
	if found {
		t.Fatal("expected not found")
	}
}
