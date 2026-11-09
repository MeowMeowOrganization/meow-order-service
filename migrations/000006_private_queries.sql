-- CEX 19.5: read-only account-scoped private query indexes.
-- No lifecycle, admission, execution or cancellation data is rewritten.
BEGIN;
CREATE INDEX orders_private_history_idx
    ON order_service.orders (account_id, created_at DESC, order_id DESC);
CREATE INDEX orders_private_market_history_idx
    ON order_service.orders (account_id, market_id, created_at DESC, order_id DESC);
CREATE INDEX orders_private_open_idx
    ON order_service.orders (account_id, created_at DESC, order_id DESC)
    WHERE admission_status = 'ACCEPTED';
CREATE INDEX private_exec_maker_history_idx
    ON order_service.private_executions
    (maker_order_id, applied_at DESC, market_numeric_id DESC, market_sequence DESC, event_ordinal DESC);
CREATE INDEX private_exec_taker_history_idx
    ON order_service.private_executions
    (taker_order_id, applied_at DESC, market_numeric_id DESC, market_sequence DESC, event_ordinal DESC);
COMMIT;
