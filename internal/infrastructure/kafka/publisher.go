package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	app "github.com/MeowMeowOrganization/meow-order-service/internal/application/outbox"
	kafkago "github.com/segmentio/kafka-go"
)

// writer is intentionally small so the synchronous ACK contract can be tested
// independently of a broker. kafka-go Writer.WriteMessages returns after the
// configured required broker acknowledgment when Async=false.
type writer interface {
	WriteMessages(context.Context, ...kafkago.Message) error
	Close() error
}

type Publisher struct {
	writer writer
	topic  string
}

var _ app.Publisher = (*Publisher)(nil)

// NewPublisher uses a key-hash partitioner: every message for one numeric
// market ID is routed to the same partition. Keep partition count stable once
// publishing starts; adding partitions changes key-to-partition mapping.
// RequiredAcks=ALL and Async=false are essential to the SENT invariant.
func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	if len(brokers) == 0 || !validTopic(topic) {
		return nil, errors.New("Kafka brokers and matching topic are required")
	}
	for _, b := range brokers {
		host, port, splitErr := net.SplitHostPort(b)
		n, portErr := strconv.Atoi(port)
		if splitErr != nil || strings.TrimSpace(b) != b || host == "" || portErr != nil || n < 1 || n > 65535 {
			return nil, errors.New("Kafka broker must be host:port with a valid port")
		}
	}
	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
		BatchSize:              1,
		BatchTimeout:           10 * time.Millisecond,
		MaxAttempts:            1, // DB is authoritative for retries and audit metadata.
	}
	return &Publisher{writer: w, topic: topic}, nil
}

// Publish sends exactly the committed payload bytes, with decimal market ID as
// partition key. The command's stable command_id lives inside those bytes.
// A timeout MAY mean Kafka accepted the message; the DB must retry it, and
// the matching adapter MUST persistently deduplicate command_id.
func (p *Publisher) Publish(ctx context.Context, m app.Message) error {
	if p == nil || p.writer == nil {
		return errors.New("Kafka publisher not initialized")
	}
	if m.MarketNumericID == 0 || m.Topic != p.topic ||
		m.Key != strconv.FormatUint(uint64(m.MarketNumericID), 10) || len(m.Payload) == 0 || m.CommandID == "" {
		return errors.New("invalid immutable matching outbox envelope")
	}
	msg := kafkago.Message{
		Key:     []byte(m.Key),
		Value:   m.Payload,
		Headers: []kafkago.Header{{Key: "command_id", Value: []byte(m.CommandID)}},
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("Kafka matching command acknowledgment uncertain: %w", err)
	}
	return nil
}

func (p *Publisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func validTopic(s string) bool {
	if len(s) == 0 || len(s) > 128 || s == "." || s == ".." {
		return false
	}
	for _, ch := range []byte(s) {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '.' || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}
