-- CEX 18.2: versioned market catalog + immutable additional TX A provenance.
-- Applies AFTER 000001. No live markets or fee policies are seeded.
BEGIN;

CREATE TABLE order_service.market_catalog (
    market_id VARCHAR(64) PRIMARY KEY,
    numeric_market_id BIGINT NOT NULL UNIQUE,
    config_version BIGINT NOT NULL,
    base_asset VARCHAR(32) NOT NULL,
    quote_asset VARCHAR(32) NOT NULL,
    base_scale SMALLINT NOT NULL,
    quote_scale SMALLINT NOT NULL,
    price_scale SMALLINT NOT NULL,
    price_tick_units NUMERIC(20,0) NOT NULL,
    quantity_step_base_atoms NUMERIC(20,0) NOT NULL,
    min_quantity_base_atoms NUMERIC(20,0) NOT NULL,
    min_notional_quote_atoms NUMERIC(20,0) NOT NULL,
    status VARCHAR(8) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT market_catalog_ids CHECK (
        market_id ~ '^[A-Z0-9-]+$' AND
        base_asset ~ '^[A-Z0-9]+$' AND quote_asset ~ '^[A-Z0-9]+$' AND
        base_asset <> quote_asset AND
        numeric_market_id BETWEEN 1 AND 4294967295 AND config_version > 0
    ),
    CONSTRAINT market_catalog_scales CHECK (
        base_scale BETWEEN 0 AND 18 AND quote_scale BETWEEN 0 AND 18 AND
        price_scale BETWEEN 0 AND 18
    ),
    CONSTRAINT market_catalog_exact_bounds CHECK (
        price_tick_units BETWEEN 1 AND 18446744073709551615 AND
        quantity_step_base_atoms BETWEEN 1 AND 18446744073709551615 AND
        min_quantity_base_atoms BETWEEN 1 AND 18446744073709551615 AND
        min_notional_quote_atoms BETWEEN 1 AND 18446744073709551615
    ),
    CONSTRAINT market_catalog_status CHECK (status IN ('ACTIVE', 'HALTED'))
);

COMMENT ON TABLE order_service.market_catalog IS
    'Order Service local catalog; SELECT FOR SHARE serializes TX A snapshots against config updates. No implicit market seed.';

-- A revision is explicit and monotonically versioned. A running engine must
-- be coordinated with changes to tick/lot configuration before activation.
CREATE FUNCTION order_service.enforce_market_catalog_revision()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.market_id, NEW.numeric_market_id, NEW.base_asset, NEW.quote_asset,
        NEW.base_scale, NEW.quote_scale, NEW.price_scale)
       IS DISTINCT FROM
       (OLD.market_id, OLD.numeric_market_id, OLD.base_asset, OLD.quote_asset,
        OLD.base_scale, OLD.quote_scale, OLD.price_scale) THEN
        RAISE EXCEPTION 'market identity and scales are immutable' USING ERRCODE = '23514';
    END IF;
    -- Tick and lot revisions require a halted market throughout the edit.
    -- The operator must still coordinate the eventual reactivation with the matcher.
    IF (NEW.price_tick_units, NEW.quantity_step_base_atoms)
       IS DISTINCT FROM (OLD.price_tick_units, OLD.quantity_step_base_atoms)
       AND (OLD.status <> 'HALTED' OR NEW.status <> 'HALTED') THEN
        RAISE EXCEPTION 'tick/lot revisions require HALTED market' USING ERRCODE = '23514';
    END IF;
    IF NEW.config_version <> OLD.config_version + 1 THEN
        RAISE EXCEPTION 'market configuration version must increase by one' USING ERRCODE = '23514';
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;
CREATE TRIGGER market_catalog_versioned_update BEFORE UPDATE ON order_service.market_catalog
    FOR EACH ROW EXECUTE FUNCTION order_service.enforce_market_catalog_revision();

CREATE TRIGGER market_catalog_no_delete BEFORE DELETE ON order_service.market_catalog
    FOR EACH ROW EXECUTE FUNCTION order_service.reject_admission_delete();

ALTER TABLE order_service.orders
    ADD COLUMN market_min_quantity_base_atoms NUMERIC(20,0),
    ADD COLUMN market_min_notional_quote_atoms NUMERIC(20,0),
    ADD COLUMN market_status VARCHAR(8),
    ADD COLUMN reservation_policy_id VARCHAR(64),
    ADD COLUMN reservation_policy_version BIGINT;

ALTER TABLE order_service.orders
    ADD CONSTRAINT orders_added_snapshot_bounds CHECK (
        (market_min_quantity_base_atoms IS NULL OR market_min_quantity_base_atoms BETWEEN 1 AND 18446744073709551615) AND
        (market_min_notional_quote_atoms IS NULL OR market_min_notional_quote_atoms BETWEEN 1 AND 18446744073709551615) AND
        (market_status IS NULL OR market_status IN ('ACTIVE', 'HALTED')) AND
        ((reservation_policy_id IS NULL AND reservation_policy_version IS NULL) OR
         (reservation_policy_id IS NOT NULL AND length(reservation_policy_id) > 0 AND
          reservation_policy_version IS NOT NULL AND reservation_policy_version > 0))
    );

-- New PENDING rows must be fully prepared. Existing 000001-era rows (if
-- any) remain untouched by this additive migration and require reconciliation.
CREATE FUNCTION order_service.require_new_pending_snapshot()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.admission_status = 'PENDING' AND (
        NEW.market_min_quantity_base_atoms IS NULL OR
        NEW.market_min_notional_quote_atoms IS NULL OR
        NEW.market_status IS DISTINCT FROM 'ACTIVE' OR
        NEW.reservation_policy_id IS NULL OR NEW.reservation_policy_version IS NULL
    ) THEN
        RAISE EXCEPTION 'new PENDING admission must contain market and reservation policy snapshots'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER orders_new_pending_snapshot BEFORE INSERT ON order_service.orders
    FOR EACH ROW EXECUTE FUNCTION order_service.require_new_pending_snapshot();

-- Additive immutable snapshot checks retain compatibility with 000001 test
-- fixtures (which predate these columns). The repository ALWAYS populates them
-- for newly created PENDING records.
CREATE FUNCTION order_service.enforce_added_order_snapshot_immutability()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.market_min_quantity_base_atoms, NEW.market_min_notional_quote_atoms,
        NEW.market_status, NEW.reservation_policy_id, NEW.reservation_policy_version)
       IS DISTINCT FROM
       (OLD.market_min_quantity_base_atoms, OLD.market_min_notional_quote_atoms,
        OLD.market_status, OLD.reservation_policy_id, OLD.reservation_policy_version) THEN
        RAISE EXCEPTION 'immutable admission market/fee snapshot changed' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER orders_added_snapshot_immutable BEFORE UPDATE ON order_service.orders
    FOR EACH ROW EXECUTE FUNCTION order_service.enforce_added_order_snapshot_immutability();

COMMIT;
