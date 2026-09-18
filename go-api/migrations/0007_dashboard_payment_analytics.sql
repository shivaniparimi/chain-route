ALTER TABLE payment_quotes DROP CONSTRAINT payment_quotes_payment_id_key;
ALTER TABLE payment_quotes ADD COLUMN selected BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE payment_quotes ADD CONSTRAINT payment_quotes_payment_id_provider_key UNIQUE (payment_id, provider);

CREATE UNIQUE INDEX payment_quotes_one_selected_per_payment
    ON payment_quotes (payment_id) WHERE selected;

CREATE INDEX payments_created_at_idx ON payments (created_at DESC);
CREATE INDEX payments_status_idx ON payments (status);
CREATE INDEX payments_execution_mode_idx ON payments (execution_mode);
CREATE INDEX payments_bridge_provider_idx ON payments (bridge_provider) WHERE bridge_provider IS NOT NULL;
CREATE INDEX payments_source_dest_idx ON payments (source_chain, destination_chain);
