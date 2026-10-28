// Package outbox defines the application-owned durable publication contract.
// The database is the source of publication state; Kafka is at-least-once.
package outbox

import (
	"context"
	"errors"
	"time"
)

var ErrLeaseLost = errors.New("outbox publication lease lost or expired")

// Message holds an immutable matching command and a database-issued delivery
// fence. Lease metadata MUST NOT be used as a Kafka idempotency key: the stable
// command_id inside Payload already identifies the logical command.
type Message struct {
	ID              int64
	OrderID         int64
	CommandID       string
	MarketNumericID uint32
	Topic           string
	Key             string
	Payload         []byte
	AttemptCount    int32
	LeaseOwner      string
	LeaseGeneration int64
	LeaseUntil      time.Time
}

// Repository uses short PostgreSQL transactions. Claims atomically move eligible
// PENDING/expired IN_FLIGHT rows to IN_FLIGHT using FOR UPDATE SKIP LOCKED.
// Only the current (owner,generation), before lease expiration, can mark SENT
// or schedule another attempt. An expired claim is recovered on the next poll.
type Repository interface {
	Claim(ctx context.Context, limit int, leaseTTL time.Duration) ([]Message, error)
	MarkSent(ctx context.Context, msg Message) error
	Retry(ctx context.Context, msg Message, backoff time.Duration) error
}

// Publisher synchronously returns nil ONLY after a successful broker ack.
// A timeout/error has an ambiguous result and MUST lead to retry, never SENT.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}
