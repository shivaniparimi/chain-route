#!/usr/bin/env bash
# Applies every go-api/migrations/*.sql file against DATABASE_URL,
# idempotently -- each migration is skipped if its guard predicate shows
# it was already applied. Portable: uses `psql` from PATH by default, or
# the path given in PSQL_BIN, rather than a hardcoded Homebrew location --
# this is what lets the exact same script run inside a container (where
# `psql` is simply on PATH via the postgres:16 image) and natively on a
# developer's machine (where it must already be resolvable, e.g. via
# `brew link postgresql@16` or PSQL_BIN=/opt/homebrew/opt/postgresql@16/bin/psql).
set -euo pipefail

DATABASE_URL="${1:?usage: apply_migrations.sh <DATABASE_URL>}"
PSQL_BIN="${PSQL_BIN:-psql}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$SCRIPT_DIR/../go-api/migrations}"

apply_if_missing() {
	local guard_sql="$1" migration_file="$2"
	if "$PSQL_BIN" "$DATABASE_URL" -tAc "$guard_sql" | grep -q 1; then
		echo "apply_migrations: $(basename "$migration_file") already applied, skipping"
		return 0
	fi
	echo "apply_migrations: applying $(basename "$migration_file")"
	"$PSQL_BIN" "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration_file"
}

apply_if_missing \
	"SELECT 1 FROM information_schema.tables WHERE table_name='payments'" \
	"$MIGRATIONS_DIR/0001_create_payments.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.tables WHERE table_name='outbox_events'" \
	"$MIGRATIONS_DIR/0002_payment_processing.sql"

apply_if_missing \
	"SELECT 1 FROM pg_constraint WHERE conname = 'outbox_events_payment_id_fkey' AND confdeltype = 'c'" \
	"$MIGRATIONS_DIR/0003_outbox_events_cascade_delete.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.tables WHERE table_name='payment_executions'" \
	"$MIGRATIONS_DIR/0004_across_testnet_execution.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.tables WHERE table_name='payment_quotes'" \
	"$MIGRATIONS_DIR/0005_realtime_bridge_routing.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.columns WHERE table_name='payment_executions' AND column_name='provider_reference_id'" \
	"$MIGRATIONS_DIR/0006_multi_provider_bridge_routing.sql"

apply_if_missing \
	"SELECT 1 FROM information_schema.columns WHERE table_name='payment_quotes' AND column_name='selected'" \
	"$MIGRATIONS_DIR/0007_dashboard_payment_analytics.sql"

echo "apply_migrations: all migrations up to date"
