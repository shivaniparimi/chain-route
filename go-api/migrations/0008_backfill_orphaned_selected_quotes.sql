-- Corrective migration, safety net for 0007's backfill: apply_migrations.sh's
-- guard for 0007 only checks that the `selected` column exists, not that it
-- was backfilled -- so a database that somehow already recorded an
-- unbackfilled version of 0007 as applied would skip 0007 forever and never
-- get fixed by editing that file. This migration is idempotent and safe to
-- run on any database (fresh or already-correctly-backfilled): it is
-- restricted to payment_id groups with exactly one quote row and zero
-- selected rows -- the exact shape every pre-0007 row has (the old
-- payment_quotes_payment_id_key UNIQUE(payment_id) constraint made more
-- than one row per payment physically impossible before 0007 dropped it) --
-- so backfilling it as selected=true is unambiguous. A payment with
-- multiple quotes and none selected is deliberately left untouched: that
-- shape can only arise from an application bug introduced after 0007
-- shipped, and this migration must never guess which quote the router
-- actually selected.
UPDATE payment_quotes
SET selected = true
WHERE payment_id IN (
    SELECT payment_id
    FROM payment_quotes
    GROUP BY payment_id
    HAVING COUNT(*) = 1 AND COUNT(*) FILTER (WHERE selected) = 0
);
