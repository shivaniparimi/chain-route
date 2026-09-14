CREATE TABLE payments (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key    TEXT NOT NULL,
    source_chain       TEXT NOT NULL,
    destination_chain  TEXT NOT NULL,
    asset              TEXT NOT NULL,
    amount             NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    status             TEXT NOT NULL DEFAULT 'ROUTED' CHECK (status IN ('ROUTED')),
    total_fee          DOUBLE PRECISION NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_chain <> destination_chain),
    UNIQUE (idempotency_key)
);

CREATE TABLE payment_route_hops (
    payment_id   UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    hop_index    INTEGER NOT NULL,
    from_chain   TEXT NOT NULL,
    to_chain     TEXT NOT NULL,
    bridge_name  TEXT NOT NULL,
    fee          DOUBLE PRECISION NOT NULL,
    latency_ms   DOUBLE PRECISION NOT NULL,
    liquidity    DOUBLE PRECISION NOT NULL,
    reliability  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (payment_id, hop_index)
);
