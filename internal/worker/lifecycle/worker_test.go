package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/lifecycle"
	domain "github.com/MeowMeowOrganization/meow-order-service/internal/domain/lifecycle"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"
)

type fakeConsumer struct {
	rec       app.Record
	fetched   int
	committed int
	commitErr error
}

func (c *fakeConsumer) Fetch(context.Context) (app.Record, error) { c.fetched++; return c.rec, nil }
func (c *fakeConsumer) Commit(_ context.Context, rec app.Record) error {
	if rec.Offset != c.rec.Offset {
		return errors.New("incorrect offset")
	}
	if c.commitErr != nil {
		return c.commitErr
	}
	c.committed++
	return nil
}
func (*fakeConsumer) Close() error { return nil }

type projectorResponse struct {
	outcome app.ApplyOutcome
	err     error
}
type fakeProjector struct {
	responses []projectorResponse
	attempts  int
	callback  func()
}

func (p *fakeProjector) ApplyMatchingResult(_ context.Context, _ *matchingpb.MatchingResult) (app.ApplyOutcome, error) {
	p.attempts++
	if p.callback != nil {
		p.callback()
	}
	if len(p.responses) == 0 {
		return app.Applied, nil
	}
	out := p.responses[0]
	p.responses = p.responses[1:]
	return out.outcome, out.err
}
func fakeRecord(t *testing.T) app.Record {
	t.Helper()
	result := &matchingpb.MatchingResult{Command: &matchingpb.MatchingCommand{MarketId: 7, CommandId: "place:11"}, MarketSequence: 1}
	data, err := proto.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return app.Record{Topic: "matching.results.v1", Partition: 0, Offset: 12, Key: []byte("7"), Value: data}
}
func workerFor(t *testing.T, c *fakeConsumer, p *fakeProjector) *Worker {
	t.Helper()
	w, err := New(c, p, slog.New(slog.NewTextHandler(io.Discard, nil)), "matching.results.v1", Options{
		ApplyTimeout: time.Second, CommitTimeout: time.Second, RetryBase: time.Millisecond, RetryMax: 4 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func TestAcknowledgeOnlyAfterSuccessfulProjection(t *testing.T) {
	for _, outcome := range []app.ApplyOutcome{app.Applied, app.Duplicate} {
		t.Run(string(outcome), func(t *testing.T) {
			c := &fakeConsumer{rec: fakeRecord(t)}
			p := &fakeProjector{responses: []projectorResponse{{outcome: outcome}}, callback: func() {
				if c.committed != 0 {
					t.Fatal("offset committed before PostgreSQL")
				}
			}}
			if err := workerFor(t, c, p).ProcessOne(context.Background()); err != nil {
				t.Fatal(err)
			}
			if c.committed != 1 || c.fetched != 1 || p.attempts != 1 {
				t.Fatalf("fetch=%d projection=%d commit=%d", c.fetched, p.attempts, c.committed)
			}
		})
	}
}
func TestMalformedAndWrongMarketKeyNeverCommitted(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*app.Record)
	}{
		{"invalid protobuf", func(r *app.Record) { r.Value = []byte{0xff} }},
		{"wrong market key", func(r *app.Record) { r.Key = []byte("9") }},
		{"wrong topic", func(r *app.Record) { r.Topic = "matching.commands.v1" }},
		{"noncanonical market key", func(r *app.Record) { r.Key = []byte("07") }},
		{"empty payload", func(r *app.Record) { r.Value = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeConsumer{rec: fakeRecord(t)}
			tc.mutate(&c.rec)
			p := &fakeProjector{}
			if err := workerFor(t, c, p).ProcessOne(context.Background()); !errors.Is(err, app.ErrResultInvalid) {
				t.Fatalf("expected invalid result: %v", err)
			}
			if c.committed != 0 || p.attempts != 0 {
				t.Fatalf("unsafe acknowledgment or projection: %+v %+v", c, p)
			}
		})
	}
}
func TestIntegrityErrorsHaltWithoutCommitting(t *testing.T) {
	for _, problem := range []error{app.ErrResultGap, app.ErrResultConflict, app.ErrResultInvalid, domain.ErrInvariant, errors.New("SQLSTATE 23514")} {
		c := &fakeConsumer{rec: fakeRecord(t)}
		p := &fakeProjector{responses: []projectorResponse{{err: problem}}}
		if err := workerFor(t, c, p).ProcessOne(context.Background()); !errors.Is(err, problem) {
			t.Fatalf("expected fatal integrity error %v: %v", problem, err)
		}
		if c.committed != 0 || p.attempts != 1 {
			t.Fatalf("unsafe retry/commit on integrity error: %d %d", p.attempts, c.committed)
		}
	}
}
func TestTransientFailureRetriesSameRecordThenAcknowledgesOnce(t *testing.T) {
	c := &fakeConsumer{rec: fakeRecord(t)}
	p := &fakeProjector{responses: []projectorResponse{{err: &net.DNSError{IsTimeout: true}}, {err: app.ErrResultCommitUnknown}, {outcome: app.Duplicate}}}
	if err := workerFor(t, c, p).ProcessOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.attempts != 3 || c.fetched != 1 || c.committed != 1 {
		t.Fatalf("attempts=%d fetches=%d commits=%d", p.attempts, c.fetched, c.committed)
	}
}
func TestCancellationWhileRetryingNeverCommits(t *testing.T) {
	c := &fakeConsumer{rec: fakeRecord(t)}
	ctx, cancel := context.WithCancel(context.Background())
	p := &fakeProjector{callback: cancel, responses: []projectorResponse{{err: app.ErrResultCommitUnknown}}}
	err := workerFor(t, c, p).ProcessOne(ctx)
	if !errors.Is(err, context.Canceled) || c.committed != 0 || p.attempts != 1 {
		t.Fatalf("err=%v commits=%d attempts=%d", err, c.committed, p.attempts)
	}
}
func TestUnknownOutcomeAndFailedKafkaCommitStop(t *testing.T) {
	c := &fakeConsumer{rec: fakeRecord(t)}
	p := &fakeProjector{responses: []projectorResponse{{outcome: app.ApplyOutcome("SURPRISE")}}}
	if err := workerFor(t, c, p).ProcessOne(context.Background()); err == nil || c.committed != 0 {
		t.Fatalf("unknown outcome acknowledged: %v", err)
	}
	c = &fakeConsumer{rec: fakeRecord(t), commitErr: errors.New("rebalance")}
	p = &fakeProjector{}
	if err := workerFor(t, c, p).ProcessOne(context.Background()); err == nil || c.committed != 0 || p.attempts != 1 {
		t.Fatalf("Kafka error incorrectly handled: %v", err)
	}
}
func TestRetryBackoffBounded(t *testing.T) {
	for attempt, want := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond, 4 * time.Millisecond} {
		if got := retryDelay(attempt, time.Millisecond, 4*time.Millisecond); got != want {
			t.Fatalf("attempt %d got %v want %v", attempt, got, want)
		}
	}
}

func TestPostgreSQLTransientErrorClassification(t *testing.T) {
	cases := []struct {
		err   error
		retry bool
	}{
		{&pgconn.PgError{Code: "40001"}, true},
		{&pgconn.PgError{Code: "40P01"}, true},
		{&pgconn.PgError{Code: "08006"}, true},
		{&pgconn.PgError{Code: "23514"}, false},
		{&pgconn.PgError{Code: "42P08"}, false},
		{app.ErrResultGap, false},
		{app.ErrResultCommitUnknown, true},
	}
	for _, tc := range cases {
		if got := retryable(tc.err); got != tc.retry {
			t.Fatalf("%v retryable=%v want=%v", tc.err, got, tc.retry)
		}
	}
}
