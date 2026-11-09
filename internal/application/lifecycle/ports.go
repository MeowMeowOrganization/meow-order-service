// Package lifecycle defines transport-independent ports for consuming committed
// matching results. A result is acknowledged only after its PostgreSQL projection.
package lifecycle

import (
	"context"
	"errors"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
)

// Stable application-level error categories used for fail-closed decisions.
// PostgreSQL owns persistence details, but the worker should not import it.
var (
	ErrResultGap           = errors.New("matching result sequence gap")
	ErrResultConflict      = errors.New("matching result contradicts committed provenance")
	ErrResultInvalid       = errors.New("invalid matching result")
	ErrResultCommitUnknown = errors.New("matching result transaction commit outcome unknown; replay same result")
)

type ApplyOutcome string

const (
	Applied   ApplyOutcome = "APPLIED"
	Duplicate ApplyOutcome = "DUPLICATE"
)

// Projector commits a complete matching result atomically, including its
// deduplication journal and per-market cursor.
type Projector interface {
	ApplyMatchingResult(context.Context, *matchingpb.MatchingResult) (ApplyOutcome, error)
}

// Record represents one Kafka message. Its position is opaque to projection:
// it MUST NEVER be substituted for the matcher's market_sequence.
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

// Consumer must commit the supplied record's Kafka offset synchronously.
// Fetch must NOT implicitly acknowledge or commit a record.
type Consumer interface {
	Fetch(context.Context) (Record, error)
	Commit(context.Context, Record) error
	Close() error
}
