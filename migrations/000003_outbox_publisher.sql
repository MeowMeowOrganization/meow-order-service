-- CEX 18.5: speed up per-market head-of-line lookup. Additive, idempotent
-- index migration; apply exactly once after 000001 and 000002.
BEGIN;
CREATE INDEX matching_outbox_market_unsent_idx
  ON order_service.matching_outbox (market_numeric_id, outbox_id)
  WHERE publication_status <> 'SENT';
COMMIT;
