-- CEX 18.1: Order Service-owned admission state and matching-command outbox.
-- Apply exactly once, in order, with psql -v ON_ERROR_STOP=1 -f <this file>.
-- The whole migration is atomic. Do not point tests at a production database.
BEGIN;

CREATE SCHEMA order_service;

CREATE TABLE order_service.orders (
    -- PostgreSQL BIGINT identity is intentionally restricted to 1..MaxInt64.
    -- It is represented as an exact decimal string in the public order contract.
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id VARCHAR(128) NOT NULL,
    client_order_id VARCHAR(128) NOT NULL,
    request_fingerprint BYTEA NOT NULL,
    fingerprint_version SMALLINT NOT NULL DEFAULT 1,
    market_id VARCHAR(64) NOT NULL,
    side SMALLINT NOT NULL,
    time_in_force SMALLINT NOT NULL,
    price_decimal VARCHAR(128) NOT NULL,
    quantity_decimal VARCHAR(128) NOT NULL,

    -- Internal admission state is separate from future matcher lifecycle state.
    admission_status VARCHAR(16) NOT NULL,
    rejection_code VARCHAR(64),

    -- Frozen market snapshot after TX A succeeds. A rejected order may not
    -- have a snapshot when validation fails before market resolution.
    market_numeric_id BIGINT,
    market_config_version BIGINT,
    base_asset VARCHAR(32),
    quote_asset VARCHAR(32),
    price_scale SMALLINT,
    base_scale SMALLINT,
    quote_scale SMALLINT,
    price_tick_units NUMERIC(20,0),
    quantity_step_base_atoms NUMERIC(20,0),
    price_units NUMERIC(20,0),
    quantity_base_atoms NUMERIC(20,0),
    notional_quote_atoms NUMERIC(20,0),
    price_ticks BIGINT,
    quantity_lots BIGINT,

    -- TX A persists a fully calculated reservation intent before Accounting.
    -- TX B records confirmed reservation provenance and commits the outbox.
    reservation_asset VARCHAR(32),
    reservation_atoms NUMERIC(20,0),
    reservation_id VARCHAR(128),
    reservation_confirmed_at TIMESTAMPTZ,

    -- Fenced ownership for admission retry/recovery. No unleased writer should
    -- finalize an admission once recovery workers are activated.
    admission_lease_generation BIGINT NOT NULL DEFAULT 0,
    admission_lease_owner UUID,
    admission_lease_until TIMESTAMPTZ,

    accepted_at TIMESTAMPTZ,
    rejected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT orders_canonical_identity UNIQUE (account_id, client_order_id),
    CONSTRAINT orders_valid_id CHECK (order_id > 0),
    CONSTRAINT orders_nonempty_identifiers CHECK (
        length(account_id) BETWEEN 1 AND 128 AND
        length(client_order_id) BETWEEN 1 AND 128 AND
        length(market_id) BETWEEN 1 AND 64
    ),
    CONSTRAINT orders_fingerprint CHECK (
        octet_length(request_fingerprint) = 32 AND fingerprint_version = 1
    ),
    CONSTRAINT orders_supported_enums CHECK (side IN (1, 2) AND time_in_force = 1),
    CONSTRAINT orders_canonical_decimals CHECK (
        price_decimal ~ '^(0|[1-9][0-9]*)(\.[0-9]*[1-9])?$' AND
        quantity_decimal ~ '^(0|[1-9][0-9]*)(\.[0-9]*[1-9])?$'
    ),
    CONSTRAINT orders_admission_state CHECK (admission_status IN ('PENDING', 'ACCEPTED', 'REJECTED')),
    CONSTRAINT orders_market_snapshot_bounds CHECK (
        (market_numeric_id IS NULL OR market_numeric_id BETWEEN 1 AND 4294967295) AND
        (market_config_version IS NULL OR market_config_version > 0) AND
        (price_scale IS NULL OR price_scale BETWEEN 0 AND 18) AND
        (base_scale IS NULL OR base_scale BETWEEN 0 AND 18) AND
        (quote_scale IS NULL OR quote_scale BETWEEN 0 AND 18) AND
        (price_tick_units IS NULL OR price_tick_units BETWEEN 1 AND 18446744073709551615) AND
        (quantity_step_base_atoms IS NULL OR quantity_step_base_atoms BETWEEN 1 AND 18446744073709551615) AND
        (price_units IS NULL OR price_units BETWEEN 1 AND 18446744073709551615) AND
        (quantity_base_atoms IS NULL OR quantity_base_atoms BETWEEN 1 AND 18446744073709551615) AND
        (notional_quote_atoms IS NULL OR notional_quote_atoms BETWEEN 1 AND 18446744073709551615) AND
        (price_ticks IS NULL OR price_ticks > 0) AND
        (quantity_lots IS NULL OR quantity_lots > 0) AND
        (reservation_atoms IS NULL OR reservation_atoms BETWEEN 1 AND 18446744073709551615)
    ),
    CONSTRAINT orders_ready_to_reserve CHECK (
        admission_status = 'REJECTED' OR (
            market_numeric_id IS NOT NULL AND market_config_version IS NOT NULL AND
            base_asset IS NOT NULL AND quote_asset IS NOT NULL AND
            price_scale IS NOT NULL AND base_scale IS NOT NULL AND quote_scale IS NOT NULL AND
            price_tick_units IS NOT NULL AND quantity_step_base_atoms IS NOT NULL AND
            price_units IS NOT NULL AND quantity_base_atoms IS NOT NULL AND
            notional_quote_atoms IS NOT NULL AND price_ticks IS NOT NULL AND quantity_lots IS NOT NULL AND
            reservation_asset IS NOT NULL AND reservation_atoms IS NOT NULL
        )
    ),
    CONSTRAINT orders_reservation_evidence CHECK (
        (reservation_id IS NULL) = (reservation_confirmed_at IS NULL)
    ),
    CONSTRAINT orders_terminal_metadata CHECK (
        (admission_status = 'PENDING' AND accepted_at IS NULL AND rejected_at IS NULL AND rejection_code IS NULL AND reservation_id IS NULL)
        OR
        (admission_status = 'ACCEPTED' AND accepted_at IS NOT NULL AND rejected_at IS NULL AND rejection_code IS NULL AND reservation_id IS NOT NULL)
        OR
        (admission_status = 'REJECTED' AND rejected_at IS NOT NULL AND accepted_at IS NULL AND rejection_code IS NOT NULL)
    ),
    CONSTRAINT orders_lease_metadata CHECK (
        admission_lease_generation >= 0 AND
        ((admission_lease_owner IS NULL AND admission_lease_until IS NULL) OR
         (admission_status = 'PENDING' AND admission_lease_owner IS NOT NULL AND admission_lease_until IS NOT NULL))
    )
);

COMMENT ON TABLE order_service.orders IS
    'Order Service admission truth; matching lifecycle is separate from the PENDING/ACCEPTED/REJECTED admission decision.';
COMMENT ON COLUMN order_service.orders.request_fingerprint IS
    'SHA-256 over versioned, length-delimited canonical business fields; client_order_id is the unique lookup key.';
COMMENT ON COLUMN order_service.orders.market_numeric_id IS
    'Stable uint32 matcher MarketId; not derived by hashing the public market ID.';

-- Persisted admission identity and canonical values must never change. The
-- only mutable columns are operational admission/reservation/lease metadata.
CREATE FUNCTION order_service.enforce_order_admission_immutability()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.account_id, NEW.client_order_id, NEW.request_fingerprint,
        NEW.fingerprint_version, NEW.market_id, NEW.side,
        NEW.time_in_force, NEW.price_decimal, NEW.quantity_decimal,
        NEW.market_numeric_id, NEW.market_config_version, NEW.base_asset,
        NEW.quote_asset, NEW.price_scale, NEW.base_scale, NEW.quote_scale,
        NEW.price_tick_units, NEW.quantity_step_base_atoms, NEW.price_units,
        NEW.quantity_base_atoms, NEW.notional_quote_atoms, NEW.price_ticks,
        NEW.quantity_lots, NEW.reservation_asset, NEW.reservation_atoms,
        NEW.created_at)
       IS DISTINCT FROM
       (OLD.account_id, OLD.client_order_id, OLD.request_fingerprint,
        OLD.fingerprint_version, OLD.market_id, OLD.side,
        OLD.time_in_force, OLD.price_decimal, OLD.quantity_decimal,
        OLD.market_numeric_id, OLD.market_config_version, OLD.base_asset,
        OLD.quote_asset, OLD.price_scale, OLD.base_scale, OLD.quote_scale,
        OLD.price_tick_units, OLD.quantity_step_base_atoms, OLD.price_units,
        OLD.quantity_base_atoms, OLD.notional_quote_atoms, OLD.price_ticks,
        OLD.quantity_lots, OLD.reservation_asset, OLD.reservation_atoms,
        OLD.created_at) THEN
        RAISE EXCEPTION 'immutable order-admission identity or canonical fields changed'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.admission_status IN ('ACCEPTED', 'REJECTED') AND
       NEW.admission_status IS DISTINCT FROM OLD.admission_status THEN
        RAISE EXCEPTION 'terminal order admission cannot transition'
            USING ERRCODE = '23514';
    END IF;
    IF OLD.admission_status IN ('ACCEPTED', 'REJECTED') AND
       (NEW.reservation_id, NEW.reservation_confirmed_at,
        NEW.accepted_at, NEW.rejected_at, NEW.rejection_code)
       IS DISTINCT FROM
       (OLD.reservation_id, OLD.reservation_confirmed_at,
        OLD.accepted_at, OLD.rejected_at, OLD.rejection_code) THEN
        RAISE EXCEPTION 'terminal reservation/rejection provenance cannot change'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER orders_immutable_admission BEFORE UPDATE ON order_service.orders
FOR EACH ROW EXECUTE FUNCTION order_service.enforce_order_admission_immutability();

CREATE INDEX orders_pending_recovery_idx
    ON order_service.orders (admission_lease_until, order_id)
    WHERE admission_status = 'PENDING';

CREATE TABLE order_service.matching_outbox (
    outbox_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES order_service.orders(order_id),
    command_id VARCHAR(128) NOT NULL UNIQUE,
    command_type VARCHAR(32) NOT NULL DEFAULT 'PLACE_LIMIT',
    market_numeric_id BIGINT NOT NULL,
    kafka_topic VARCHAR(128) NOT NULL,
    kafka_key VARCHAR(32) NOT NULL,
    payload BYTEA NOT NULL,
    payload_version SMALLINT NOT NULL DEFAULT 1,
    publication_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_generation BIGINT NOT NULL DEFAULT 0,
    lease_owner UUID,
    lease_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT outbox_identity CHECK (outbox_id > 0 AND order_id > 0),
    CONSTRAINT outbox_command_type CHECK (command_type = 'PLACE_LIMIT'),
    CONSTRAINT outbox_market_id CHECK (market_numeric_id BETWEEN 1 AND 4294967295),
    CONSTRAINT outbox_payload CHECK (octet_length(payload) > 0 AND payload_version = 1),
    CONSTRAINT outbox_command_id CHECK (command_id = ('place:' || order_id::text)),
    CONSTRAINT outbox_partition_key CHECK (kafka_key = market_numeric_id::text),
    CONSTRAINT outbox_publication CHECK (
        publication_status IN ('PENDING', 'IN_FLIGHT', 'SENT') AND
        attempt_count >= 0 AND lease_generation >= 0 AND
        length(kafka_topic) > 0 AND
        (
            (publication_status = 'PENDING' AND lease_owner IS NULL AND lease_until IS NULL AND published_at IS NULL)
            OR (publication_status = 'IN_FLIGHT' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL AND published_at IS NULL)
            OR (publication_status = 'SENT' AND lease_owner IS NULL AND lease_until IS NULL AND published_at IS NOT NULL)
        )
    )
);

-- Reserve exactly one PLACE_LIMIT command per admitted order while leaving
-- room to add different command types in later milestones.
CREATE UNIQUE INDEX matching_outbox_one_place_per_order_idx
    ON order_service.matching_outbox (order_id) WHERE command_type = 'PLACE_LIMIT';
CREATE INDEX matching_outbox_publishable_idx
    ON order_service.matching_outbox (next_attempt_at, outbox_id)
    WHERE publication_status <> 'SENT';

CREATE FUNCTION order_service.enforce_outbox_immutability()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.order_id, NEW.command_id, NEW.command_type, NEW.market_numeric_id,
        NEW.kafka_topic, NEW.kafka_key, NEW.payload, NEW.payload_version,
        NEW.created_at)
       IS DISTINCT FROM
       (OLD.order_id, OLD.command_id, OLD.command_type, OLD.market_numeric_id,
        OLD.kafka_topic, OLD.kafka_key, OLD.payload, OLD.payload_version,
        OLD.created_at) THEN
        RAISE EXCEPTION 'immutable matching command changed' USING ERRCODE = '23514';
    END IF;
    IF OLD.publication_status = 'SENT' AND
       (NEW.publication_status, NEW.published_at, NEW.attempt_count,
        NEW.lease_generation, NEW.next_attempt_at)
       IS DISTINCT FROM
       (OLD.publication_status, OLD.published_at, OLD.attempt_count,
        OLD.lease_generation, OLD.next_attempt_at) THEN
        RAISE EXCEPTION 'published matching command delivery record is terminal' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_immutable_command BEFORE UPDATE ON order_service.matching_outbox
FOR EACH ROW EXECUTE FUNCTION order_service.enforce_outbox_immutability();

COMMENT ON TABLE order_service.matching_outbox IS
    'Immutable matching-command payloads; delivery is at-least-once with downstream command-id deduplication.';

-- The database also rejects accidental ACCEPTED-without-outbox commits.
-- Deferred checks allow TX B to UPDATE order then INSERT outbox in one commit.
CREATE FUNCTION order_service.check_accepted_has_outbox()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.admission_status = 'ACCEPTED' AND NOT EXISTS (
        SELECT 1 FROM order_service.matching_outbox x
        WHERE x.order_id = NEW.order_id
          AND x.command_type = 'PLACE_LIMIT'
          AND x.market_numeric_id = NEW.market_numeric_id
    ) THEN
        RAISE EXCEPTION 'ACCEPTED order must commit with its matching outbox record'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER accepted_order_requires_outbox
    AFTER INSERT OR UPDATE ON order_service.orders
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION order_service.check_accepted_has_outbox();

-- Prevent any standalone outbox command being committed for an order that
-- never completed admission, or with the wrong market routing.
CREATE FUNCTION order_service.check_outbox_parent_accepted()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM order_service.orders o
        WHERE o.order_id = NEW.order_id
          AND o.admission_status = 'ACCEPTED'
          AND o.market_numeric_id = NEW.market_numeric_id
    ) THEN
        RAISE EXCEPTION 'matching outbox requires an ACCEPTED parent in the same market'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER outbox_requires_accepted_order
    AFTER INSERT ON order_service.matching_outbox
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION order_service.check_outbox_parent_accepted();

-- Admission identities and published obligations are durable; deletion would
-- permit idempotency key reuse or destroy an accepted publication obligation.
CREATE FUNCTION order_service.reject_admission_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'admission/outbox deletion is not supported'
        USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER orders_no_delete BEFORE DELETE ON order_service.orders
    FOR EACH ROW EXECUTE FUNCTION order_service.reject_admission_delete();
CREATE TRIGGER outbox_no_delete BEFORE DELETE ON order_service.matching_outbox
    FOR EACH ROW EXECUTE FUNCTION order_service.reject_admission_delete();

COMMIT;
