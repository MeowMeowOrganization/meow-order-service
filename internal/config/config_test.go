package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func fromMap(values map[string]string) (Config, error) {
	return FromLookup(func(key string) (string, bool) { v, ok := values[key]; return v, ok })
}

func TestDefaults(t *testing.T) {
	cfg, err := fromMap(map[string]string{
		"ORDER_SERVICE_DATABASE_URL": "postgres://user:secret@localhost:5432/orders?sslmode=disable",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseMaxConns != 10 || cfg.LogLevel != slog.LevelInfo || cfg.ReadinessTimeout != 2*time.Second || cfg.ShutdownTimeout != 20*time.Second {
		t.Fatalf("unexpected defaults: max_conns=%d level=%s readiness=%s shutdown=%s", cfg.DatabaseMaxConns, cfg.LogLevel, cfg.ReadinessTimeout, cfg.ShutdownTimeout)
	}
}

func TestInvalidConfiguration(t *testing.T) {
	cases := []struct{ key, value string }{
		{"ORDER_SERVICE_DATABASE_URL", ""},
		{"ORDER_SERVICE_DATABASE_URL", "postgres://admin:hunter2@"},
		{"ORDER_SERVICE_DATABASE_MAX_CONNS", "0"},
		{"ORDER_SERVICE_DATABASE_MAX_CONNS", "1001"},
		{"ORDER_SERVICE_LOG_LEVEL", "VERBOSE"},
		{"ORDER_SERVICE_HTTP_ADDR", "127.0.0.1"},
		{"ORDER_SERVICE_HTTP_ADDR", ":0"},
		{"ORDER_SERVICE_STARTUP_TIMEOUT", "not-a-duration"},
		{"ORDER_SERVICE_READINESS_TIMEOUT", "0s"},
		{"ORDER_SERVICE_SHUTDOWN_TIMEOUT", "6m"},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			values := map[string]string{"ORDER_SERVICE_DATABASE_URL": "postgres://user:secret@localhost/db"}
			values[tc.key] = tc.value
			_, err := fromMap(values)
			if err == nil {
				t.Fatal("expected validation failure")
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "hunter2") {
				t.Fatalf("database secret leaked in configuration error: %v", err)
			}
		})
	}
}
