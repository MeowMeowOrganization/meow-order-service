package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/lifecycle"
	kafkago "github.com/segmentio/kafka-go"
)

// reader is satisfied by *kafka.Reader; injected for adapter tests.
// kafka-go's FetchMessage + CommitMessages is the non-auto-committing API.
type reader interface {
	FetchMessage(context.Context) (kafkago.Message, error)
	CommitMessages(context.Context, ...kafkago.Message) error
	Close() error
}

type LifecycleConsumer struct {
	reader  reader
	topic   string
	pending *kafkago.Message
}

var _ app.Consumer = (*LifecycleConsumer)(nil)

// NewLifecycleConsumer participates in a stable Kafka consumer group. The
// group ID must remain unchanged across restarts. CommitInterval=0 ensures
// broker commits are synchronous; ReadMessage is deliberately NOT used.
func NewLifecycleConsumer(brokers []string, topic, groupID string) (*LifecycleConsumer, error) {
	if len(brokers) == 0 || !validTopic(topic) || !validTopic(groupID) {
		return nil, errors.New("valid Kafka brokers, results topic and stable group ID required")
	}
	for _, broker := range brokers {
		if !validateBrokerAddress(broker) {
			return nil, errors.New("invalid Kafka broker host:port")
		}
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafkago.FirstOffset, // new groups start at sequence 1
		CommitInterval: 0,                   // synchronous, explicit commits only
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
	})
	return &LifecycleConsumer{reader: r, topic: topic}, nil
}

func (c *LifecycleConsumer) Fetch(ctx context.Context) (app.Record, error) {
	if c == nil || c.reader == nil {
		return app.Record{}, errors.New("Kafka lifecycle consumer not initialized")
	}
	if c.pending != nil {
		return app.Record{}, errors.New("previous matching result has not been acknowledged")
	}
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return app.Record{}, fmt.Errorf("fetch Kafka matching result: %w", err)
	}
	// Retain the exact message for synchronous commit and prevent fetching
	// ahead of an unacknowledged result.
	c.pending = &msg
	return app.Record{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
		Key: append([]byte(nil), msg.Key...), Value: append([]byte(nil), msg.Value...)}, nil
}

func (c *LifecycleConsumer) Commit(ctx context.Context, rec app.Record) error {
	if c == nil || c.reader == nil || c.pending == nil {
		return errors.New("no pending Kafka matching result to commit")
	}
	m := c.pending
	if rec.Topic != m.Topic || rec.Topic != c.topic || rec.Partition != m.Partition || rec.Offset != m.Offset ||
		!bytes.Equal(rec.Key, m.Key) || !bytes.Equal(rec.Value, m.Value) {
		return errors.New("Kafka matching result commit position mismatch")
	}
	if err := c.reader.CommitMessages(ctx, *m); err != nil {
		// Leave the record pending. The caller must stop and reopen the reader
		// rather than fetch/commit a newer offset over an uncertain one.
		return fmt.Errorf("commit Kafka matching result offset: %w", err)
	}
	c.pending = nil
	return nil
}

func (c *LifecycleConsumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	return c.reader.Close()
}
