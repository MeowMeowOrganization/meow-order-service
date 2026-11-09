-- CEX 19.4. Apply exactly once after 000004. Existing immutable PLACE_LIMIT
-- commands and their delivery state remain untouched.
BEGIN;

ALTER TABLE order_service.matching_outbox DROP CONSTRAINT outbox_command_type;
ALTER TABLE order_service.matching_outbox ADD CONSTRAINT outbox_command_type
    CHECK (command_type IN ('PLACE_LIMIT', 'CANCEL_ORDER'));
ALTER TABLE order_service.matching_outbox DROP CONSTRAINT outbox_command_id;
ALTER TABLE order_service.matching_outbox ADD CONSTRAINT outbox_command_id CHECK (
    (command_type = 'PLACE_LIMIT' AND command_id = ('place:' || order_id::text)) OR
    (command_type = 'CANCEL_ORDER' AND command_id = ('cancel:' || order_id::text))
);
CREATE UNIQUE INDEX matching_outbox_one_cancel_per_order_idx
    ON order_service.matching_outbox (order_id) WHERE command_type = 'CANCEL_ORDER';

-- One stable logical cancellation request per order; never mutate a completed
-- command. Application and matcher projection lock the lifecycle row first.
CREATE TABLE order_service.cancel_requests (
    order_id BIGINT PRIMARY KEY REFERENCES order_service.orders(order_id),
    command_id VARCHAR(128) NOT NULL UNIQUE,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    matcher_rejection_reason VARCHAR(64),
    completed_market_sequence NUMERIC(20,0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT cancel_request_command CHECK (command_id = ('cancel:' || order_id::text)),
    CONSTRAINT cancel_request_outcome CHECK (
        (status = 'PENDING' AND matcher_rejection_reason IS NULL AND completed_market_sequence IS NULL AND completed_at IS NULL) OR
        (status = 'CONFIRMED' AND matcher_rejection_reason IS NULL AND completed_market_sequence BETWEEN 1 AND 18446744073709551615 AND completed_at IS NOT NULL) OR
        (status = 'REJECTED' AND matcher_rejection_reason IN ('UNKNOWN_ORDER','ALREADY_FILLED','ALREADY_CANCELLED','ALREADY_REJECTED') AND
         completed_market_sequence BETWEEN 1 AND 18446744073709551615 AND completed_at IS NOT NULL)
    )
);

CREATE FUNCTION order_service.enforce_cancel_request_monotonicity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'cancellation requests cannot be deleted' USING ERRCODE = '23514';
    END IF;
    IF (OLD.status <> 'PENDING') OR
       (NEW.order_id,NEW.command_id,NEW.created_at) IS DISTINCT FROM
       (OLD.order_id,OLD.command_id,OLD.created_at) OR
       NEW.status NOT IN ('CONFIRMED','REJECTED') THEN
        RAISE EXCEPTION 'cancellation requests are immutable after completion' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER cancel_request_monotonic BEFORE UPDATE OR DELETE ON order_service.cancel_requests
    FOR EACH ROW EXECUTE FUNCTION order_service.enforce_cancel_request_monotonicity();

-- Financial handoff only. Never call ReleaseReservation until ALL prior fills
-- have a trusted settlement-finality proof, which is not present in the current
-- Order Service contracts. The operation ID is stable across eventual retries.
CREATE TABLE order_service.reservation_release_intents (
    order_id BIGINT PRIMARY KEY REFERENCES order_service.orders(order_id),
    reservation_id VARCHAR(128) NOT NULL,
    operation_id VARCHAR(128) NOT NULL UNIQUE,
    cause VARCHAR(32) NOT NULL DEFAULT 'MATCHER_CANCELLED',
    state VARCHAR(32) NOT NULL DEFAULT 'AWAITING_SETTLEMENT',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT release_intent_identity CHECK (operation_id = ('release:' || order_id::text)),
    CONSTRAINT release_intent_cause CHECK (cause = 'MATCHER_CANCELLED'),
    CONSTRAINT release_intent_state CHECK (state = 'AWAITING_SETTLEMENT')
);
CREATE TRIGGER reservation_release_intent_append_only
    BEFORE UPDATE OR DELETE ON order_service.reservation_release_intents
    FOR EACH ROW EXECUTE FUNCTION order_service.reject_lifecycle_provenance_change();

COMMENT ON TABLE order_service.cancel_requests IS
    'Durable client cancellation intent; accepted request is not matcher-confirmed cancellation.';
COMMENT ON TABLE order_service.reservation_release_intents IS
    'Unexecuted settlement-fenced release obligations; do not directly credit spendable funds.';
COMMIT;
