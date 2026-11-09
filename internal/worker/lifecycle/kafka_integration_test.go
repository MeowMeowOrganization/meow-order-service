//go:build integration

package lifecycle

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	matchingpb "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/matching/v1alpha1"
	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/lifecycle"
	adapter "github.com/MeowMeowOrganization/meow-order-service/internal/infrastructure/kafka"
	kafkago "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// TestKafkaLifecycleGroupOffsetsAndReplay uses a real Kafka broker. It proves
// that the worker commits only after projection, and that restarting with the
// same group resumes from the broker-committed offset. PostgreSQL's atomic
// journal is covered by lifecycle_repository_integration_test.go.
//
// Set ORDER_SERVICE_KAFKA_INTEGRATION_BROKERS=localhost:9092 to opt in.
func TestKafkaLifecycleGroupOffsetsAndReplay(t *testing.T) {
	raw := os.Getenv("ORDER_SERVICE_KAFKA_INTEGRATION_BROKERS")
	if raw == "" {
		t.Skip("set ORDER_SERVICE_KAFKA_INTEGRATION_BROKERS for real Kafka test")
	}
	brokers := strings.Split(raw, ",")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	topic := fmt.Sprintf("cex19.lifecycle.test.%d", time.Now().UnixNano())
	group := fmt.Sprintf("cex19.lifecycle.group.%d", time.Now().UnixNano())
	// This test targets a single-node development broker (KRaft combined mode).
	// Controller() can report an empty endpoint (:0) in this configuration;
	// the already-connected bootstrap broker can handle topic admin requests.
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker %q: %v", brokers[0], err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka test topic %q: %v", topic, err)
	}
	// Clean up the topic before the connection is closed (Cleanup is LIFO).
	t.Cleanup(func() {
		if err := conn.DeleteTopics(topic); err != nil {
			t.Logf("remove temporary Kafka topic %q: %v", topic, err)
		}
	})
	// CreateTopics acknowledges topic creation before the partition is guaranteed
	// to have an elected, reachable leader. Wait before the first produce.
	if err := waitForKafkaTestLeader(ctx, brokers[0], topic); err != nil {
		t.Fatalf("Kafka test topic not ready: %v", err)
	}
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, RequiredAcks: kafkago.RequireAll,
		Balancer: &kafkago.Hash{}, BatchSize: 1, AllowAutoTopicCreation: false}
	defer writer.Close()
	publish := func(seq uint64) {
		t.Helper()
		msg := &matchingpb.MatchingResult{Command: &matchingpb.MatchingCommand{CommandId: fmt.Sprintf("place:%d", seq), MarketId: 7}, MarketSequence: seq}
		data, e := proto.Marshal(msg)
		if e != nil {
			t.Fatal(e)
		}
		if e := writer.WriteMessages(ctx, kafkago.Message{Key: []byte("7"), Value: data}); e != nil {
			t.Fatal(e)
		}
	}
	publish(1)
	publish(1) // simulate transport-level duplicate with a separate Kafka offset
	c, err := adapter.NewLifecycleConsumer(brokers, topic, group)
	if err != nil {
		t.Fatal(err)
	}
	projector := &sequenceProjector{seen: map[uint64]bool{}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := New(c, projector, logger, topic, Options{ApplyTimeout: 5 * time.Second, CommitTimeout: 5 * time.Second, RetryBase: time.Millisecond, RetryMax: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if projector.applied != 1 || projector.duplicates != 1 {
		t.Fatalf("applied=%d duplicates=%d", projector.applied, projector.duplicates)
	}
	publish(2)
	c2, err := adapter.NewLifecycleConsumer(brokers, topic, group)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	w2, err := New(c2, projector, logger, topic, Options{ApplyTimeout: 5 * time.Second, CommitTimeout: 5 * time.Second, RetryBase: time.Millisecond, RetryMax: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	if projector.applied != 2 || projector.duplicates != 1 {
		t.Fatalf("consumer group restart lost offsets: applied=%d duplicate=%d", projector.applied, projector.duplicates)
	}
}

// waitForKafkaTestLeader waits for the actual partition leader before the
// first produce. Each attempt has its own timeout: a single stalled metadata
// request must not consume the entire readiness budget. Kafka's advertised
// listener must also be reachable from the host running this test.
func waitForKafkaTestLeader(ctx context.Context, broker, topic string) error {
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastDial error
	for {
		attemptCtx, finish := context.WithTimeout(readyCtx, 2*time.Second)
		leaderConn, err := kafkago.DialLeader(attemptCtx, "tcp", broker, topic, 0)
		finish()
		if err == nil {
			_ = leaderConn.Close()
			return nil
		}
		lastDial = err
		select {
		case <-readyCtx.Done():
			return fmt.Errorf("Kafka topic %q partition 0 unavailable (bootstrap %s): %w; last leader lookup: %v; check Docker broker logs, listener metadata and connectivity", topic, broker, readyCtx.Err(), lastDial)
		case <-ticker.C:
		}
	}
}

type sequenceProjector struct {
	seen                map[uint64]bool
	applied, duplicates int
}

func (s *sequenceProjector) ApplyMatchingResult(_ context.Context, r *matchingpb.MatchingResult) (app.ApplyOutcome, error) {
	if s.seen[r.MarketSequence] {
		s.duplicates++
		return app.Duplicate, nil
	}
	s.seen[r.MarketSequence] = true
	s.applied++
	return app.Applied, nil
}
