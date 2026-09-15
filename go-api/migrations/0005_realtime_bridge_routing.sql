CREATE TABLE payment_quotes (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id              UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    provider                TEXT NOT NULL,
    origin_chain_id         BIGINT NOT NULL,
    destination_chain_id    BIGINT NOT NULL,
    asset                   TEXT NOT NULL,
    input_amount            NUMERIC(38,0) NOT NULL,
    output_amount           NUMERIC(38,0) NOT NULL,
    fee_amount              NUMERIC(38,0) NOT NULL,
    estimated_fill_time_sec INTEGER NOT NULL,
    quoted_at               TIMESTAMPTZ NOT NULL,
    expires_at              TIMESTAMPTZ NOT NULL,
    raw_provider_payload    JSONB NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (payment_id)
);

ALTER TABLE payments
    ADD COLUMN failure_reason TEXT NULL;
