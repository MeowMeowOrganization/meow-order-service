package config

import (
	"testing"
	"time"
)

func TestOutboxDisabledByDefault(t *testing.T) {
	cfg, err := fromMap(map[string]string{"ORDER_SERVICE_DATABASE_URL": "postgres://meow:secret@localhost:5432/orders"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OutboxEnabled || cfg.OutboxBatchSize != 8 || cfg.OutboxLeaseTTL != 45*time.Second || cfg.MatchingTopic != "matching.commands.v1" {
		t.Fatalf("unexpected outbox defaults: %+v", cfg)
	}
}

func TestOutboxEnabledRequiresBrokerAndValidTiming(t *testing.T) {
	base := map[string]string{"ORDER_SERVICE_DATABASE_URL": "postgres://meow:secret@localhost/orders", "ORDER_SERVICE_OUTBOX_ENABLED": "true"}
	if _, err := fromMap(base); err == nil {
		t.Fatal("enabled without brokers")
	}
	base["ORDER_SERVICE_KAFKA_BROKERS"] = "127.0.0.1:9092"
	if _, err := fromMap(base); err != nil {
		t.Fatal(err)
	}
	base["ORDER_SERVICE_OUTBOX_LEASE_TTL"] = "5s"
	if _, err := fromMap(base); err == nil {
		t.Fatal("lease shorter than producer timeout")
	}
	base["ORDER_SERVICE_OUTBOX_LEASE_TTL"] = "20s"
	base["ORDER_SERVICE_KAFKA_BROKERS"] = "127.0.0.1:bad"
	if _, err := fromMap(base); err == nil {
		t.Fatal("invalid Kafka port accepted")
	}
}
