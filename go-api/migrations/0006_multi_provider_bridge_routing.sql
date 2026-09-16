ALTER TABLE payment_executions
    RENAME COLUMN across_deposit_id TO provider_reference_id;

ALTER TABLE payment_executions
    ADD COLUMN raw_external_status TEXT NULL;

ALTER TABLE payment_executions
    DROP CONSTRAINT payment_executions_external_status_check,
    ADD CONSTRAINT payment_executions_external_status_check
        CHECK (external_status IN ('pending', 'filled', 'expired', 'refunded', 'reverted', 'fill_failed'));
