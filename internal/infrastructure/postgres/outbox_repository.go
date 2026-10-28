package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/outbox"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxOutboxBatch = 100

// OutboxRepository is stateless; multiple instances/processes can claim from
// one database without a distributed lock manager.
type OutboxRepository struct{ db *pgxpool.Pool }

var _ app.Repository = (*OutboxRepository)(nil)

func NewOutboxRepository(db *pgxpool.Pool) (*OutboxRepository, error) {
	if db == nil {
		return nil, errors.New("outbox repository requires PostgreSQL")
	}
	return &OutboxRepository{db: db}, nil
}

// Claim uses the database clock, not an application clock. NOT EXISTS prevents
// overtaking an earlier unsent command for the SAME market, including commands
// claimed by another worker or waiting for retry. SKIP LOCKED allows other
// markets to progress while another transaction holds their row lock.
func (r *OutboxRepository) Claim(ctx context.Context, limit int, leaseTTL time.Duration) ([]app.Message, error) {
	if limit <= 0 || limit > maxOutboxBatch || leaseTTL < time.Second || leaseTTL > 5*time.Minute || leaseTTL%time.Millisecond != 0 {
		return nil, errors.New("outbox claim requires batch 1..100 and whole-millisecond lease 1s..5m")
	}
	owner, err := newLeaseOwner()
	if err != nil {
		return nil, err
	}
	const query = `WITH eligible AS (
        SELECT o.outbox_id
        FROM order_service.matching_outbox AS o
        WHERE o.next_attempt_at <= clock_timestamp()
          AND (o.publication_status = 'PENDING'
             OR (o.publication_status = 'IN_FLIGHT' AND o.lease_until <= clock_timestamp()))
          AND NOT EXISTS (
              SELECT 1 FROM order_service.matching_outbox AS earlier
              WHERE earlier.market_numeric_id = o.market_numeric_id
                AND earlier.outbox_id < o.outbox_id
                AND earlier.publication_status <> 'SENT'
          )
        ORDER BY o.outbox_id
        LIMIT $1
        FOR UPDATE OF o SKIP LOCKED
    ), claimed AS (
        UPDATE order_service.matching_outbox o
        SET publication_status = 'IN_FLIGHT', lease_owner = $2::uuid,
            lease_generation = o.lease_generation + 1,
            lease_until = clock_timestamp() + ($3::bigint * interval '1 millisecond'),
            attempt_count = o.attempt_count + 1,
            updated_at = clock_timestamp()
        FROM eligible e
        WHERE o.outbox_id = e.outbox_id
        RETURNING o.outbox_id, o.order_id, o.command_id, o.market_numeric_id,
          o.kafka_topic, o.kafka_key, o.payload, o.attempt_count,
          o.lease_owner::text, o.lease_generation, o.lease_until
    )
    SELECT * FROM claimed ORDER BY outbox_id`
	rows, err := r.db.Query(ctx, query, limit, owner, leaseTTL.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("claim matching outbox: %w", err)
	}
	defer rows.Close()
	var messages []app.Message
	for rows.Next() {
		var m app.Message
		var numeric int64
		if err := rows.Scan(&m.ID, &m.OrderID, &m.CommandID, &numeric, &m.Topic,
			&m.Key, &m.Payload, &m.AttemptCount, &m.LeaseOwner, &m.LeaseGeneration, &m.LeaseUntil); err != nil {
			return nil, fmt.Errorf("decode claimed outbox row: %w", err)
		}
		if numeric <= 0 || numeric > 4294967295 {
			return nil, errors.New("outbox market ID outside uint32 range")
		}
		m.MarketNumericID = uint32(numeric)
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed outbox: %w", err)
	}
	return messages, nil
}

// MarkSent only succeeds when the producer received an ack AND the publication
// fence is still current. An unknown commit outcome is harmless: a future
// claimant will read SENT, or will retry the same immutable command.
func (r *OutboxRepository) MarkSent(ctx context.Context, m app.Message) error {
	tag, err := r.db.Exec(ctx, `UPDATE order_service.matching_outbox
      SET publication_status = 'SENT', published_at = clock_timestamp(),
          lease_owner = NULL, lease_until = NULL, updated_at = clock_timestamp()
      WHERE outbox_id = $1 AND publication_status = 'IN_FLIGHT'
        AND lease_owner = $2::uuid AND lease_generation = $3
        AND lease_until > clock_timestamp()`, m.ID, m.LeaseOwner, m.LeaseGeneration)
	if err != nil {
		return fmt.Errorf("mark outbox SENT: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrLeaseLost
	}
	return nil
}

// Retry leaves the durable message intact and schedules a bounded backoff.
// It never sends a command to a dead-letter stream or drops it automatically.
func (r *OutboxRepository) Retry(ctx context.Context, m app.Message, backoff time.Duration) error {
	if backoff < 0 || backoff > 5*time.Minute || backoff%time.Millisecond != 0 {
		return errors.New("invalid outbox retry backoff")
	}
	tag, err := r.db.Exec(ctx, `UPDATE order_service.matching_outbox
      SET publication_status = 'PENDING', lease_owner = NULL, lease_until = NULL,
          next_attempt_at = clock_timestamp() + ($4::bigint * interval '1 millisecond'),
          updated_at = clock_timestamp()
      WHERE outbox_id = $1 AND publication_status = 'IN_FLIGHT'
        AND lease_owner = $2::uuid AND lease_generation = $3
        AND lease_until > clock_timestamp()`, m.ID, m.LeaseOwner, m.LeaseGeneration, backoff.Milliseconds())
	if err != nil {
		return fmt.Errorf("schedule outbox retry: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrLeaseLost
	}
	return nil
}
