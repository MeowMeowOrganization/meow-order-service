package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/outbox"
)

// Options bounds each synchronous broker acknowledgment and lease lifetime.
// Claims are per-market head-of-line and different markets may publish in
// parallel. There is no automatic financial DLQ or message discard.
type Options struct {
	BatchSize      int
	PollInterval   time.Duration
	LeaseTTL       time.Duration
	PublishTimeout time.Duration
	RetryBase      time.Duration
	RetryMax       time.Duration
}

type Worker struct {
	repo      app.Repository
	publisher app.Publisher
	logger    *slog.Logger
	opts      Options
}

func New(repo app.Repository, publisher app.Publisher, logger *slog.Logger, opts Options) (*Worker, error) {
	if repo == nil || publisher == nil || logger == nil || opts.BatchSize < 1 || opts.BatchSize > 100 ||
		opts.PollInterval < 10*time.Millisecond || opts.PollInterval > time.Minute ||
		opts.LeaseTTL <= opts.PublishTimeout+time.Second || opts.LeaseTTL > 5*time.Minute ||
		opts.PublishTimeout < time.Millisecond || opts.PublishTimeout > time.Minute ||
		opts.RetryBase < time.Millisecond || opts.RetryMax < opts.RetryBase || opts.RetryMax > 5*time.Minute {
		return nil, errors.New("invalid outbox worker options or dependencies")
	}
	return &Worker{repo: repo, publisher: publisher, logger: logger, opts: opts}, nil
}

// Run polls until cancellation; transient Kafka/DB failures are logged and
// retried. The service remains alive so /readyz can reflect DB availability.
// On cancellation, any ambiguous in-flight rows are recovered after lease TTL.
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		n, err := w.ProcessOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.logger.Error("outbox processing failed; retrying", "error", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		if err == nil && n == w.opts.BatchSize {
			continue
		} // drain backlog
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// ProcessOnce is exposed for deterministic worker tests. A failed message
// cannot cause another message to be marked SENT. Distinct markets can proceed
// independently, while the repository blocks newer same-market commands.
func (w *Worker) ProcessOnce(ctx context.Context) (int, error) {
	messages, err := w.repo.Claim(ctx, w.opts.BatchSize, w.opts.LeaseTTL)
	if err != nil {
		return 0, fmt.Errorf("claim outbox batch: %w", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(messages))
	for _, msg := range messages {
		wg.Add(1)
		go func(m app.Message) {
			defer wg.Done()
			if err := w.publishOne(ctx, m); err != nil {
				errs <- err
			}
		}(msg)
	}
	wg.Wait()
	close(errs)
	var all []error
	for e := range errs {
		all = append(all, e)
	}
	return len(messages), errors.Join(all...)
}

func (w *Worker) publishOne(ctx context.Context, m app.Message) error {
	// A claimed row might have waited behind a slow broker send or GC pause.
	// Never knowingly send without enough lease time for an ACK + DB update.
	if time.Until(m.LeaseUntil) <= w.opts.PublishTimeout+time.Second {
		return fmt.Errorf("outbox %d lease too near expiry; let lease recover", m.ID)
	}
	publishCtx, cancel := context.WithTimeout(ctx, w.opts.PublishTimeout)
	err := w.publisher.Publish(publishCtx, m)
	cancel()
	if err == nil {
		// Do not mark SENT on a cancelled DB write or if the lease was stolen.
		if err := w.repo.MarkSent(ctx, m); err != nil {
			return fmt.Errorf("outbox %d acknowledged but could not mark SENT (retry may duplicate): %w", m.ID, err)
		}
		return nil
	}
	// Timeout/error is ambiguous. Kafka may have committed the command.
	// Retry always sends the SAME immutable payload and command_id.
	delay := retryDelay(m.AttemptCount, w.opts.RetryBase, w.opts.RetryMax)
	if ctx.Err() != nil { // shutdown: leave IN_FLIGHT for expired-lease recovery
		return fmt.Errorf("outbox %d interrupted after ambiguous Kafka result: %w", m.ID, err)
	}
	if retryErr := w.repo.Retry(ctx, m, delay); retryErr != nil {
		return fmt.Errorf("outbox %d Kafka error: %v; retry scheduling failed: %w", m.ID, err, retryErr)
	}
	return fmt.Errorf("outbox %d Kafka result ambiguous, retry scheduled: %w", m.ID, err)
}

// retryDelay is bounded, deterministic exponential backoff, never infinite.
func retryDelay(attempt int32, base, cap time.Duration) time.Duration {
	delay := base
	for i := int32(1); i < attempt && delay < cap; i++ {
		if delay > cap/2 {
			return cap
		}
		delay *= 2
	}
	if delay > cap {
		return cap
	}
	return delay
}
