package config

import (
	"testing"
	"time"
)

func TestLifecycleDisabledByDefault(t *testing.T) {
	cfg, err := fromMap(map[string]string{"ORDER_SERVICE_DATABASE_URL": "postgres://meow:secret@localhost/orders"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LifecycleEnabled || cfg.ResultsTopic != "matching.results.v1" || cfg.LifecycleGroupID != "meow-order-service-lifecycle-v1" || cfg.LifecycleApplyTimeout != 30*time.Second || cfg.LifecycleCommitTimeout != 10*time.Second {
		t.Fatalf("bad defaults: %+v", cfg)
	}
}
func TestLifecycleConfigValidations(t *testing.T) {
	base := map[string]string{"ORDER_SERVICE_DATABASE_URL": "postgres://meow:secret@localhost/orders", "ORDER_SERVICE_LIFECYCLE_ENABLED": "true"}
	if _, err := fromMap(base); err == nil {
		t.Fatal("enabled without Kafka brokers")
	}
	base["ORDER_SERVICE_KAFKA_BROKERS"] = "localhost:9092"
	if cfg, err := fromMap(base); err != nil || len(cfg.KafkaBrokers) != 1 || !cfg.LifecycleEnabled {
		t.Fatalf("consumer-only config: %+v %v", cfg, err)
	}
	for key, invalid := range map[string]string{
		"ORDER_SERVICE_LIFECYCLE_ENABLED":        "unexpected",
		"ORDER_SERVICE_LIFECYCLE_GROUP_ID":       "no group!",
		"ORDER_SERVICE_LIFECYCLE_APPLY_TIMEOUT":  "0s",
		"ORDER_SERVICE_LIFECYCLE_COMMIT_TIMEOUT": "0s",
		"ORDER_SERVICE_LIFECYCLE_RETRY_BASE":     "-1s",
		"ORDER_SERVICE_LIFECYCLE_RETRY_MAX":      "10ms",
		"ORDER_SERVICE_MATCHING_RESULTS_TOPIC":   "bad topic!",
	} {
		orig, ok := base[key]
		base[key] = invalid
		if _, err := fromMap(base); err == nil {
			t.Errorf("accepted %s=%s", key, invalid)
		}
		if ok {
			base[key] = orig
		} else {
			delete(base, key)
		}
	}
	base["ORDER_SERVICE_MATCHING_RESULTS_TOPIC"] = "matching.commands.v1"
	if _, err := fromMap(base); err == nil {
		t.Fatal("same input/output topic accepted")
	}
}
