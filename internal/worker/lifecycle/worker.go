// Package lifecycle connects Kafka committed MatchingResult records to the
// existing atomic PostgreSQL lifecycle projector. No partial acknowledgments.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/lifecycle"
	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/lifecycle"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"
)

// Options control bounded attempts (each transaction has its own deadline).
// Retrying never fetches a newer message, never commits, and never skips gaps.
type Options struct {
	ApplyTimeout  time.Duration
	CommitTimeout time.Duration
	RetryBase     time.Duration
	RetryMax      time.Duration
}

type Worker struct {
	consumer  app.Consumer
	projector app.Projector
	logger    *slog.Logger
	opts      Options
	topic     string
}

func New(consumer app.Consumer, projector app.Projector, logger *slog.Logger, topic string, opts Options) (*Worker, error) {
	if consumer == nil || projector == nil || logger == nil || topic == "" ||
		opts.ApplyTimeout < time.Millisecond || opts.ApplyTimeout > 5*time.Minute ||
		opts.CommitTimeout < time.Millisecond || opts.CommitTimeout > 5*time.Minute ||
		opts.RetryBase < time.Millisecond || opts.RetryMax < opts.RetryBase || opts.RetryMax > 5*time.Minute {
		return nil, errors.New("invalid lifecycle worker dependencies/options")
	}
	return &Worker{consumer: consumer, projector: projector, logger: logger, topic: topic, opts: opts}, nil
}

// Run processes exactly one result at a time. A fatal integrity violation
// stops the worker and is propagated by bootstrap to stop the whole service.
func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := w.ProcessOne(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	return nil
}

// ProcessOne fetches and projects ONE complete record before committing it.
// Tests use this entrypoint to demonstrate no commit before DB success.
func (w *Worker) ProcessOne(ctx context.Context) error {
	rec, err := w.consumer.Fetch(ctx)
	if err != nil {
		return fmt.Errorf("fetch matching result: %w", err)
	}
	if rec.Topic != w.topic || len(rec.Value) == 0 {
		return fmt.Errorf("%w: unexpected topic or empty result at partition=%d offset=%d", app.ErrResultInvalid, rec.Partition, rec.Offset)
	}
	var result matchingpb.MatchingResult
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(rec.Value, &result); err != nil {
		return fmt.Errorf("%w: invalid protobuf at partition=%d offset=%d: %v", app.ErrResultInvalid, rec.Partition, rec.Offset, err)
	}
	cmd := result.GetCommand()
	if cmd == nil || cmd.GetMarketId() == 0 || result.GetMarketSequence() == 0 ||
		string(rec.Key) != strconv.FormatUint(uint64(cmd.GetMarketId()), 10) {
		return fmt.Errorf("%w: market partition key or result identity mismatch at partition=%d offset=%d", app.ErrResultInvalid, rec.Partition, rec.Offset)
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		applyCtx, cancel := context.WithTimeout(ctx, w.opts.ApplyTimeout)
		outcome, applyErr := w.projector.ApplyMatchingResult(applyCtx, &result)
		cancel()
		if applyErr == nil {
			if outcome != app.Applied && outcome != app.Duplicate {
				return fmt.Errorf("unexpected lifecycle projection outcome %q", outcome)
			}
			// A commit failure is ambiguous. Stop, close the reader and let
			// the consumer group redeliver (the DB journal ensures no-op).
			commitCtx, stopCommit := context.WithTimeout(ctx, w.opts.CommitTimeout)
			commitErr := w.consumer.Commit(commitCtx, rec)
			stopCommit()
			if commitErr != nil {
				return fmt.Errorf("Kafka commit failed after PostgreSQL %s (restart to replay): %w", outcome, commitErr)
			}
			w.logger.Debug("matching result projected and acknowledged", "market", cmd.GetMarketId(),
				"sequence", result.GetMarketSequence(), "outcome", outcome,
				"partition", rec.Partition, "offset", rec.Offset)
			return nil
		}
		if !retryable(applyErr) {
			return fmt.Errorf("matching result projection stopped (market=%d seq=%d partition=%d offset=%d): %w",
				cmd.GetMarketId(), result.GetMarketSequence(), rec.Partition, rec.Offset, applyErr)
		}
		delay := retryDelay(attempt, w.opts.RetryBase, w.opts.RetryMax)
		w.logger.Warn("transient lifecycle projection error; retrying same Kafka record", "market", cmd.GetMarketId(),
			"sequence", result.GetMarketSequence(), "partition", rec.Partition, "offset", rec.Offset,
			"retry_delay", delay, "error", applyErr)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func retryable(err error) bool {
	if errors.Is(err, app.ErrResultCommitUnknown) {
		return true
	}
	if errors.Is(err, app.ErrResultGap) || errors.Is(err, app.ErrResultConflict) ||
		errors.Is(err, app.ErrResultInvalid) || errors.Is(err, domain.ErrInvariant) {
		return false
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "40001", "40P01", "55P03", "53300", "53400", "57P01", "57P02", "57P03":
			return true
		}
		return len(pg.Code) >= 2 && pg.Code[:2] == "08" // connection exception
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded)
}

func retryDelay(attempt int, base, max time.Duration) time.Duration {
	delay := base
	for i := 0; i < attempt && delay < max; i++ {
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}
