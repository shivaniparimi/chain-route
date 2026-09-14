ALTER TABLE outbox_events DROP CONSTRAINT outbox_events_payment_id_fkey;
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_payment_id_fkey
    FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE;
