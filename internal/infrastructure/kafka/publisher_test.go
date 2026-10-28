package kafka

import (
	"context"
	"errors"
	"testing"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/outbox"
	kafkago "github.com/segmentio/kafka-go"
)

type recordingWriter struct {
	messages []kafkago.Message
	err      error
	closed   bool
}

func (w *recordingWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	w.messages = append(w.messages, msgs...)
	return w.err
}
func (w *recordingWriter) Close() error { w.closed = true; return nil }
func TestPublisherSendsImmutableBytesWithMarketPartitionKey(t *testing.T) {
	writer := &recordingWriter{}
	p := &Publisher{writer: writer, topic: "matching.commands.v1"}
	m := app.Message{MarketNumericID: 42, Topic: "matching.commands.v1", Key: "42", CommandID: "place:123", Payload: []byte{0, 1, 2, 3, 4}}
	if err := p.Publish(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("writes=%d", len(writer.messages))
	}
	got := writer.messages[0]
	if string(got.Key) != "42" || len(got.Value) != 5 || got.Value[2] != 2 ||
		len(got.Headers) != 1 || got.Headers[0].Key != "command_id" || string(got.Headers[0].Value) != "place:123" {
		t.Fatalf("mismatched Kafka message: %+v", got)
	}
	if err := p.Close(); err != nil || !writer.closed {
		t.Fatalf("close: %v", err)
	}
}
func TestPublisherTimeoutIsNotSuccess(t *testing.T) {
	writer := &recordingWriter{err: context.DeadlineExceeded}
	p := &Publisher{writer: writer, topic: "matching.commands.v1"}
	err := p.Publish(context.Background(), app.Message{MarketNumericID: 7, Topic: "matching.commands.v1", Key: "7", CommandID: "place:1", Payload: []byte{42}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected ambiguous failure: %v", err)
	}
}
func TestPublisherRejectsBadEnvelope(t *testing.T) {
	p := &Publisher{writer: &recordingWriter{}, topic: "matching.commands.v1"}
	if err := p.Publish(context.Background(), app.Message{MarketNumericID: 7, Key: "8", Topic: "matching.commands.v1", Payload: []byte{1}, CommandID: "place:1"}); err == nil {
		t.Fatal("bad partition key accepted")
	}
}
