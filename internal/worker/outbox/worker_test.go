package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/outbox"
)

type fakeRepo struct {
	mu       sync.Mutex
	queue    []app.Message
	sent     []int64
	retries  []time.Duration
	claimErr error
	markErr  error
}

func (r *fakeRepo) Claim(_ context.Context, _ int, _ time.Duration) ([]app.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claimErr != nil {
		return nil, r.claimErr
	}
	result := r.queue
	r.queue = nil
	return result, nil
}
func (r *fakeRepo) MarkSent(_ context.Context, m app.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.markErr != nil {
		return r.markErr
	}
	r.sent = append(r.sent, m.ID)
	return nil
}
func (r *fakeRepo) Retry(_ context.Context, _ app.Message, d time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retries = append(r.retries, d)
	return nil
}

type fakeProducer struct {
	mu       sync.Mutex
	messages []app.Message
	err      error
}

func (p *fakeProducer) Publish(_ context.Context, m app.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, m)
	return p.err
}
func workerForTest(t *testing.T, r *fakeRepo, p *fakeProducer) *Worker {
	t.Helper()
	w, err := New(r, p, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		BatchSize: 8, PollInterval: 10 * time.Millisecond, LeaseTTL: 20 * time.Second,
		PublishTimeout: time.Second, RetryBase: 100 * time.Millisecond, RetryMax: 800 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func sampleMessage(id int64, attempt int32) app.Message {
	return app.Message{ID: id, OrderID: id, CommandID: "place:123", MarketNumericID: 7, Topic: "matching.commands.v1", Key: "7",
		Payload: []byte{1, 2, 3}, LeaseUntil: time.Now().Add(30 * time.Second), AttemptCount: attempt}
}
func TestWorkerMarksSentOnlyAfterAcknowledgement(t *testing.T) {
	r := &fakeRepo{queue: []app.Message{sampleMessage(1, 1), sampleMessage(2, 1)}}
	p := &fakeProducer{}
	w := workerForTest(t, r, p)
	n, err := w.ProcessOnce(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("process: %d %v", n, err)
	}
	if len(r.sent) != 2 || len(r.retries) != 0 || len(p.messages) != 2 {
		t.Fatalf("wrong acknowledgements: %+v", r)
	}
}
func TestWorkerRetriesAmbiguousKafkaFailureWithoutSENT(t *testing.T) {
	r := &fakeRepo{queue: []app.Message{sampleMessage(9, 4)}}
	p := &fakeProducer{err: context.DeadlineExceeded}
	w := workerForTest(t, r, p)
	n, err := w.ProcessOnce(context.Background())
	if n != 1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected worker result: %d %v", n, err)
	}
	if len(r.sent) != 0 || len(r.retries) != 1 || r.retries[0] != 800*time.Millisecond {
		t.Fatalf("ack was incorrectly assumed: %+v", r)
	}
}
func TestWorkerAckWithLostDatabaseFenceNeverReturnsSuccess(t *testing.T) {
	r := &fakeRepo{queue: []app.Message{sampleMessage(3, 1)}, markErr: app.ErrLeaseLost}
	w := workerForTest(t, r, &fakeProducer{})
	_, err := w.ProcessOnce(context.Background())
	if !errors.Is(err, app.ErrLeaseLost) || len(r.sent) != 0 || len(r.retries) != 0 {
		t.Fatalf("unfenced SENT accepted: %v %+v", err, r)
	}
}
func TestWorkerShutdownLeavesAmbiguousMessageForLeaseRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeRepo{queue: []app.Message{sampleMessage(3, 1)}}
	p := &cancelProducer{cancel: cancel}
	w := workerForTest(t, r, &fakeProducer{})
	w.publisher = p
	_, _ = w.ProcessOnce(ctx)
	if len(r.sent) != 0 || len(r.retries) != 0 {
		t.Fatalf("cancellation changed durable state: %+v", r)
	}
}

type cancelProducer struct{ cancel context.CancelFunc }

func (p *cancelProducer) Publish(_ context.Context, _ app.Message) error {
	p.cancel()
	return context.Canceled
}

func TestBoundedBackoff(t *testing.T) {
	for attempt, want := range map[int32]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 4: 800 * time.Millisecond, 50: 800 * time.Millisecond} {
		if got := retryDelay(attempt, 100*time.Millisecond, 800*time.Millisecond); got != want {
			t.Fatalf("attempt=%d: %v != %v", attempt, got, want)
		}
	}
}

type gatedProducer struct {
	started chan struct{}
	ack     chan struct{}
}

func (p *gatedProducer) Publish(ctx context.Context, _ app.Message) error {
	close(p.started)
	select {
	case <-p.ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestWorkerNeverMarksSENTBeforeKafkaAck(t *testing.T) {
	repo := &fakeRepo{queue: []app.Message{sampleMessage(18, 1)}}
	gated := &gatedProducer{started: make(chan struct{}), ack: make(chan struct{})}
	worker := workerForTest(t, repo, &fakeProducer{})
	worker.publisher = gated
	done := make(chan error, 1)
	go func() { _, err := worker.ProcessOnce(context.Background()); done <- err }()
	select {
	case <-gated.started:
	case <-time.After(time.Second):
		t.Fatal("publisher was never called")
	}
	repo.mu.Lock()
	before := len(repo.sent)
	repo.mu.Unlock()
	if before != 0 {
		t.Fatal("SENT was written before Kafka acknowledged")
	}
	close(gated.ack)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not complete after ack")
	}
	repo.mu.Lock()
	after := len(repo.sent)
	repo.mu.Unlock()
	if after != 1 {
		t.Fatalf("expected one MarkSent after ack, got %d", after)
	}
}
