-- CEX 19.2. Apply once after 000001..000003. No existing admission
-- identity/snapshot or outbox command constraints are relaxed.
BEGIN;

CREATE TABLE order_service.market_result_cursors (
    market_numeric_id BIGINT PRIMARY KEY CHECK (market_numeric_id BETWEEN 1 AND 4294967295),
    last_sequence NUMERIC(20,0) NOT NULL DEFAULT 0
        CHECK (last_sequence BETWEEN 0 AND 18446744073709551615),
    last_state_hash BYTEA,
    last_hash_version BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cursor_hash_pair CHECK ((last_state_hash IS NULL) = (last_hash_version IS NULL)),
    CONSTRAINT cursor_hash_size CHECK (last_state_hash IS NULL OR octet_length(last_state_hash) = 32),
    CONSTRAINT cursor_hash_version CHECK (last_hash_version IS NULL OR last_hash_version > 0),
    CONSTRAINT cursor_genesis CHECK ((last_sequence = 0) = (last_state_hash IS NULL))
);

CREATE TABLE order_service.order_lifecycle (
    order_id BIGINT PRIMARY KEY REFERENCES order_service.orders(order_id),
    -- Duplicated immutable admission snapshot permits row-local CHECK constraints.
    total_lots BIGINT NOT NULL CHECK (total_lots > 0),
    status VARCHAR(24) NOT NULL DEFAULT 'ACCEPTED',
    filled_lots BIGINT NOT NULL DEFAULT 0 CHECK (filled_lots >= 0),
    cancelled_lots BIGINT NOT NULL DEFAULT 0 CHECK (cancelled_lots >= 0),
    last_market_sequence NUMERIC(20,0) NOT NULL DEFAULT 0
        CHECK (last_market_sequence BETWEEN 0 AND 18446744073709551615),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    matcher_rejection_reason VARCHAR(96),
    opened_at TIMESTAMPTZ,
    terminal_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT lifecycle_quantities CHECK (filled_lots::numeric + cancelled_lots::numeric <= total_lots),
    CONSTRAINT lifecycle_state CHECK (
        (status = 'ACCEPTED' AND filled_lots = 0 AND cancelled_lots = 0 AND terminal_at IS NULL AND matcher_rejection_reason IS NULL)
        OR (status = 'OPEN' AND filled_lots = 0 AND cancelled_lots = 0 AND terminal_at IS NULL AND matcher_rejection_reason IS NULL)
        OR (status = 'PARTIALLY_FILLED' AND filled_lots > 0 AND filled_lots < total_lots AND cancelled_lots = 0 AND terminal_at IS NULL AND matcher_rejection_reason IS NULL)
        OR (status = 'FILLED' AND filled_lots = total_lots AND cancelled_lots = 0 AND terminal_at IS NOT NULL AND matcher_rejection_reason IS NULL)
        OR (status = 'CANCELED' AND cancelled_lots > 0 AND filled_lots::numeric + cancelled_lots::numeric = total_lots AND terminal_at IS NOT NULL AND matcher_rejection_reason IS NULL)
        OR (status = 'REJECTED' AND filled_lots = 0 AND cancelled_lots = 0 AND terminal_at IS NOT NULL AND matcher_rejection_reason IS NOT NULL)
    )
);
CREATE INDEX order_lifecycle_active_idx ON order_service.order_lifecycle (status, order_id)
    WHERE status IN ('OPEN', 'PARTIALLY_FILLED');

-- Atomic TX B hook: an accepted admission gets its lifecycle row in the SAME
-- transaction without altering the CEX 18 Finalize implementation. Historical
-- accepted rows are also backfilled on migration. Their matcher histories are
-- NOT fabricated; replay must start from the verified beginning of each market.
CREATE FUNCTION order_service.initialize_accepted_lifecycle()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.admission_status = 'ACCEPTED' AND
       (TG_OP = 'INSERT' OR OLD.admission_status IS DISTINCT FROM 'ACCEPTED') THEN
        INSERT INTO order_service.order_lifecycle (order_id, total_lots)
        VALUES (NEW.order_id, NEW.quantity_lots);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER orders_initialize_lifecycle
    AFTER INSERT OR UPDATE OF admission_status ON order_service.orders
    FOR EACH ROW EXECUTE FUNCTION order_service.initialize_accepted_lifecycle();
INSERT INTO order_service.order_lifecycle (order_id, total_lots)
SELECT order_id, quantity_lots FROM order_service.orders WHERE admission_status = 'ACCEPTED';

CREATE TABLE order_service.applied_matching_results (
    market_numeric_id BIGINT NOT NULL CHECK (market_numeric_id BETWEEN 1 AND 4294967295),
    market_sequence NUMERIC(20,0) NOT NULL
        CHECK (market_sequence BETWEEN 1 AND 18446744073709551615),
    command_id VARCHAR(128) NOT NULL UNIQUE,
    command_type VARCHAR(32) NOT NULL CHECK (command_type IN ('PLACE_LIMIT', 'CANCEL_ORDER')),
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    hash_encoding_version BIGINT NOT NULL CHECK (hash_encoding_version > 0),
    state_sha256 BYTEA NOT NULL CHECK (octet_length(state_sha256) = 32),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market_numeric_id, market_sequence)
);

CREATE TABLE order_service.private_executions (
    market_numeric_id BIGINT NOT NULL CHECK (market_numeric_id BETWEEN 1 AND 4294967295),
    market_sequence NUMERIC(20,0) NOT NULL CHECK (market_sequence > 0),
    event_ordinal NUMERIC(20,0) NOT NULL CHECK (event_ordinal >= 0),
    maker_order_id BIGINT NOT NULL REFERENCES order_service.orders(order_id),
    taker_order_id BIGINT NOT NULL REFERENCES order_service.orders(order_id),
    price_ticks BIGINT NOT NULL CHECK (price_ticks > 0),
    quantity_lots BIGINT NOT NULL CHECK (quantity_lots > 0),
    taker_side SMALLINT NOT NULL CHECK (taker_side IN (1, 2)),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market_numeric_id, market_sequence, event_ordinal),
    FOREIGN KEY (market_numeric_id, market_sequence)
        REFERENCES order_service.applied_matching_results (market_numeric_id, market_sequence),
    CONSTRAINT private_execution_distinct_orders CHECK (maker_order_id <> taker_order_id)
);
CREATE INDEX private_exec_maker_idx ON order_service.private_executions (maker_order_id, market_sequence DESC, event_ordinal DESC);
CREATE INDEX private_exec_taker_idx ON order_service.private_executions (taker_order_id, market_sequence DESC, event_ordinal DESC);

-- Immutable financial/event provenance; no UPDATE/DELETE path for replay logs.
CREATE FUNCTION order_service.reject_lifecycle_provenance_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'matching result and execution provenance is immutable' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER applied_results_append_only BEFORE UPDATE OR DELETE ON order_service.applied_matching_results
    FOR EACH ROW EXECUTE FUNCTION order_service.reject_lifecycle_provenance_change();
CREATE TRIGGER private_executions_append_only BEFORE UPDATE OR DELETE ON order_service.private_executions
    FOR EACH ROW EXECUTE FUNCTION order_service.reject_lifecycle_provenance_change();
CREATE TRIGGER order_lifecycle_no_delete BEFORE DELETE ON order_service.order_lifecycle
    FOR EACH ROW EXECUTE FUNCTION order_service.reject_lifecycle_provenance_change();

-- Reconciliation must never rewrite admitted lot counts or undo a terminal
-- lifecycle result, even when a caller bypasses the Go projector.
CREATE FUNCTION order_service.enforce_lifecycle_monotonicity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.total_lots IS DISTINCT FROM OLD.total_lots OR
       NEW.filled_lots < OLD.filled_lots OR
       NEW.cancelled_lots < OLD.cancelled_lots OR
       NEW.version <> OLD.version + 1 OR
       NEW.last_market_sequence <= OLD.last_market_sequence OR
       (OLD.opened_at IS NOT NULL AND NEW.opened_at IS DISTINCT FROM OLD.opened_at) OR
       (OLD.terminal_at IS NOT NULL AND NEW.terminal_at IS DISTINCT FROM OLD.terminal_at) OR
       (OLD.status IN ('FILLED','CANCELED','REJECTED') AND
        (NEW.status,NEW.filled_lots,NEW.cancelled_lots,NEW.matcher_rejection_reason)
        IS DISTINCT FROM
        (OLD.status,OLD.filled_lots,OLD.cancelled_lots,OLD.matcher_rejection_reason)) THEN
        RAISE EXCEPTION 'matching lifecycle cannot regress or rewrite terminal provenance' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER lifecycle_monotonic BEFORE UPDATE ON order_service.order_lifecycle
    FOR EACH ROW EXECUTE FUNCTION order_service.enforce_lifecycle_monotonicity();

CREATE FUNCTION order_service.enforce_cursor_contiguity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.last_sequence <> 0 OR NEW.last_state_hash IS NOT NULL THEN
            RAISE EXCEPTION 'new market cursor must start at pristine sequence zero' USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.last_sequence <> OLD.last_sequence + 1 THEN
        RAISE EXCEPTION 'market cursor must advance by exactly one result' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER cursor_contiguous BEFORE INSERT OR UPDATE ON order_service.market_result_cursors
    FOR EACH ROW EXECUTE FUNCTION order_service.enforce_cursor_contiguity();

COMMENT ON TABLE order_service.applied_matching_results IS
    'One atomic committed MatchingResult per market sequence; duplicates must match payload SHA-256.';
COMMENT ON TABLE order_service.order_lifecycle IS
    'Current matching lifecycle; never rewrite order admission_status.';
COMMENT ON TABLE order_service.market_result_cursors IS
    'Contiguous per-market matching result cursor; never fast-forward missing results.';
COMMIT;
