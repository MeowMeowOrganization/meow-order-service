package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contains validated process configuration. DatabaseURL must never be logged.
type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	DatabaseMaxConns int32
	LogLevel         slog.Level
	StartupTimeout   time.Duration
	ReadinessTimeout time.Duration
	ShutdownTimeout  time.Duration
}

// Load obtains configuration exclusively from the environment.
func Load() (Config, error) { return FromLookup(os.LookupEnv) }

// FromLookup makes configuration deterministic and testable without changing process environment.
func FromLookup(lookup func(string) (string, bool)) (Config, error) {
	get := func(key, fallback string) string {
		if value, ok := lookup(key); ok {
			return value
		}
		return fallback
	}
	cfg := Config{
		HTTPAddr:    get("ORDER_SERVICE_HTTP_ADDR", "127.0.0.1:8081"),
		DatabaseURL: get("ORDER_SERVICE_DATABASE_URL", ""),
	}
	n, err := strconv.ParseInt(get("ORDER_SERVICE_DATABASE_MAX_CONNS", "10"), 10, 32)
	if err != nil || n < 1 || n > 1000 {
		return Config{}, errors.New("ORDER_SERVICE_DATABASE_MAX_CONNS must be between 1 and 1000")
	}
	cfg.DatabaseMaxConns = int32(n)

	switch strings.ToUpper(get("ORDER_SERVICE_LOG_LEVEL", "INFO")) {
	case "DEBUG":
		cfg.LogLevel = slog.LevelDebug
	case "INFO":
		cfg.LogLevel = slog.LevelInfo
	case "WARN":
		cfg.LogLevel = slog.LevelWarn
	case "ERROR":
		cfg.LogLevel = slog.LevelError
	default:
		return Config{}, errors.New("ORDER_SERVICE_LOG_LEVEL must be DEBUG, INFO, WARN, or ERROR")
	}

	durations := []struct {
		name     string
		output   *time.Duration
		fallback string
	}{
		{"ORDER_SERVICE_STARTUP_TIMEOUT", &cfg.StartupTimeout, "10s"},
		{"ORDER_SERVICE_READINESS_TIMEOUT", &cfg.ReadinessTimeout, "2s"},
		{"ORDER_SERVICE_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout, "20s"},
	}
	for _, field := range durations {
		parsed, err := time.ParseDuration(get(field.name, field.fallback))
		if err != nil || parsed <= 0 || parsed > 5*time.Minute {
			return Config{}, fmt.Errorf("%s must be a positive duration no longer than 5m", field.name)
		}
		*field.output = parsed
	}
	if err := validateListenAddr(cfg.HTTPAddr); err != nil {
		return Config{}, err
	}
	if err := validateDatabaseURL(cfg.DatabaseURL); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateListenAddr(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.New("ORDER_SERVICE_HTTP_ADDR must be host:port (IPv6 in brackets)")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return errors.New("ORDER_SERVICE_HTTP_ADDR port must be between 1 and 65535")
	}
	return nil
}

func validateDatabaseURL(dsn string) error {
	// Do not interpolate the URL into errors: it can contain credentials.
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Hostname() == "" {
		return errors.New("ORDER_SERVICE_DATABASE_URL must be a postgres:// or postgresql:// URL with a host")
	}
	if parsed.Fragment != "" {
		return errors.New("ORDER_SERVICE_DATABASE_URL cannot contain a fragment")
	}
	return nil
}
