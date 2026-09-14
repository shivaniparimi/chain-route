ALTER TABLE payments
    ADD COLUMN execution_mode TEXT NOT NULL DEFAULT 'simulated'
        CHECK (execution_mode IN ('simulated', 'testnet')),
    ADD COLUMN bridge_provider TEXT NULL;

ALTER TABLE payments
    DROP CONSTRAINT payments_status_check,
    ADD CONSTRAINT payments_status_check
        CHECK (status IN ('ROUTED', 'PROCESSING', 'SUBMITTED', 'COMPLETED', 'FAILED'));

CREATE TABLE payment_executions (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id           UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    bridge_provider      TEXT NOT NULL,
    origin_chain_id      BIGINT NOT NULL,
    destination_chain_id BIGINT NOT NULL,
    wallet_address       TEXT NOT NULL,
    nonce                BIGINT NOT NULL,
    unsigned_tx_params   JSONB NULL,
    signed_tx_hash       TEXT NULL,
    raw_signed_tx        BYTEA NULL,
    broadcast_at         TIMESTAMPTZ NULL,
    across_deposit_id    TEXT NULL,
    external_status      TEXT NOT NULL DEFAULT 'pending'
        CHECK (external_status IN ('pending', 'filled', 'expired', 'refunded', 'reverted')),
    confirmed_at         TIMESTAMPTZ NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wallet_address, nonce),
    UNIQUE (payment_id)
);

CREATE INDEX payment_executions_pending_idx
    ON payment_executions (updated_at)
    WHERE external_status = 'pending';

CREATE TABLE wallet_nonces (
    wallet_address TEXT PRIMARY KEY,
    next_nonce     BIGINT NOT NULL
);
