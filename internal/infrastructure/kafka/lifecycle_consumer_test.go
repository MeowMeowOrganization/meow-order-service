package kafka

import (
	"context"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

type readerStub struct {
	record    kafkago.Message
	fetches   int
	commits   int
	committed []kafkago.Message
	commitErr error
	closed    bool
}

func (r *readerStub) FetchMessage(context.Context) (kafkago.Message, error) {
	r.fetches++
	return r.record, nil
}
func (r *readerStub) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.commits++
	r.committed = append(r.committed, msgs...)
	return r.commitErr
}
func (r *readerStub) Close() error { r.closed = true; return nil }
func TestConsumerFetchDoesNotAutoCommit(t *testing.T) {
	reader := &readerStub{record: kafkago.Message{Topic: "matching.results.v1", Partition: 2, Offset: 42, Key: []byte("7"), Value: []byte{1, 2}}}
	c := &LifecycleConsumer{reader: reader, topic: "matching.results.v1"}
	rec, err := c.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if reader.commits != 0 || rec.Offset != 42 || rec.Partition != 2 {
		t.Fatalf("unexpected fetch: %+v", rec)
	}
	if _, err := c.Fetch(context.Background()); err == nil || reader.fetches != 1 {
		t.Fatalf("fetched ahead of pending record: %v", err)
	}
	if err := c.Commit(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if reader.commits != 1 || len(reader.committed) != 1 || reader.committed[0].Offset != 42 {
		t.Fatal("wrong offset commit")
	}
	if err := c.Close(); err != nil || !reader.closed {
		t.Fatal("reader not closed")
	}
}
func TestCommitRejectsWrongRecordAndNeverFetchesAhead(t *testing.T) {
	reader := &readerStub{record: kafkago.Message{Topic: "matching.results.v1", Partition: 0, Offset: 5}}
	c := &LifecycleConsumer{reader: reader, topic: "matching.results.v1"}
	rec, _ := c.Fetch(context.Background())
	altered := rec
	altered.Offset++
	if err := c.Commit(context.Background(), altered); err == nil {
		t.Fatal("accepted wrong offset")
	}
	if reader.commits != 0 {
		t.Fatal("committed wrong record")
	}
	altered = rec
	altered.Value = []byte{123}
	if err := c.Commit(context.Background(), altered); err == nil {
		t.Fatal("accepted wrong payload")
	}
	reader.commitErr = errors.New("rebalance")
	if err := c.Commit(context.Background(), rec); err == nil {
		t.Fatal("ignored commit failure")
	}
	if _, err := c.Fetch(context.Background()); err == nil {
		t.Fatal("advanced after ambiguous commit")
	}
}
func TestLifecycleConsumerConfigValidation(t *testing.T) {
	tests := []struct {
		brokers      []string
		topic, group string
	}{
		{nil, "matching.results.v1", "meow-order-service-lifecycle-v1"},
		{[]string{"bad"}, "matching.results.v1", "meow-order-service-lifecycle-v1"},
		{[]string{"localhost:9092"}, "bad topic!", "meow-order-service-lifecycle-v1"},
		{[]string{"localhost:9092"}, "matching.results.v1", ""},
	}
	for _, tc := range tests {
		if c, err := NewLifecycleConsumer(tc.brokers, tc.topic, tc.group); err == nil {
			_ = c.Close()
			t.Fatalf("accepted invalid config: %+v", tc)
		}
	}
}
