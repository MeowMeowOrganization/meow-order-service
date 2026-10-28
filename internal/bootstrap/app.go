package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/MeowMeowOrganization/meow-order-service/internal/config"
	"github.com/MeowMeowOrganization/meow-order-service/internal/infrastructure/httpserver"
	"github.com/MeowMeowOrganization/meow-order-service/internal/infrastructure/kafka"
	"github.com/MeowMeowOrganization/meow-order-service/internal/infrastructure/postgres"
	rpc "github.com/MeowMeowOrganization/meow-order-service/internal/transport/connectrpc"
	"github.com/MeowMeowOrganization/meow-order-service/internal/transport/health"
	"github.com/MeowMeowOrganization/meow-order-service/internal/worker/outbox"
)

// Run is the composition root: no package besides bootstrap constructs all dependencies.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	startupCtx, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	pool, err := postgres.Open(startupCtx, cfg.DatabaseURL, cfg.DatabaseMaxConns)
	cancel()
	if err != nil {
		return fmt.Errorf("startup: %w", err)
	}
	defer pool.Close()

	healthHandler := health.New(pool, cfg.ReadinessTimeout)
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", healthHandler.Live)
	mux.HandleFunc("/readyz", healthHandler.Ready)
	rpc.Register(mux, rpc.OrderHandler{})
	srv := httpserver.New(cfg.HTTPAddr, mux, logger)
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("bind HTTP listener: %w", err)
	}

	// Start the publication loop only when explicitly enabled. The default
	// Docker dev environment contains PostgreSQL but no Kafka broker.
	if !cfg.OutboxEnabled {
		return serve(ctx, listener, srv, healthHandler, cfg.ShutdownTimeout, logger)
	}
	repository, err := postgres.NewOutboxRepository(pool)
	if err != nil {
		_ = listener.Close()
		return err
	}
	producer, err := kafka.NewPublisher(cfg.KafkaBrokers, cfg.MatchingTopic)
	if err != nil {
		_ = listener.Close()
		return err
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Warn("Kafka producer close failed", "error", err)
		}
	}()
	worker, err := outbox.New(repository, producer, logger, outbox.Options{
		BatchSize: cfg.OutboxBatchSize, PollInterval: cfg.OutboxPollInterval,
		LeaseTTL: cfg.OutboxLeaseTTL, PublishTimeout: cfg.OutboxPublishTimeout,
		RetryBase: cfg.OutboxRetryBase, RetryMax: cfg.OutboxRetryMax,
	})
	if err != nil {
		_ = listener.Close()
		return err
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(workerCtx) }()
	logger.Info("matching outbox worker started", "topic", cfg.MatchingTopic)
	serveErr := serve(ctx, listener, srv, healthHandler, cfg.ShutdownTimeout, logger)
	stopWorker()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer drainCancel()
	select {
	case workerErr := <-workerDone:
		if workerErr != nil {
			return errors.Join(serveErr, workerErr)
		}
	case <-drainCtx.Done():
		return errors.Join(serveErr, errors.New("outbox worker did not stop within shutdown timeout"))
	}
	logger.Info("matching outbox worker stopped")
	return serveErr
}

type readinessState interface {
	MarkServing()
	MarkStopping()
}

func serve(ctx context.Context, ln net.Listener, srv *http.Server, readiness readinessState, timeout time.Duration, logger *slog.Logger) error {
	// The listener already exists, but readiness remains false until Serve begins.
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	readiness.MarkServing()
	logger.Info("order service listening", "addr", ln.Addr().String())

	select {
	case err := <-errCh:
		readiness.MarkStopping()
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("HTTP serve: %w", err)
	case <-ctx.Done():
		readiness.MarkStopping()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		shutdownErr := srv.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			logger.Warn("graceful HTTP drain failed", "error", shutdownErr)
			// Force active connections closed after the grace period.
			_ = srv.Close()
		}
		serveErr := <-errCh
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return fmt.Errorf("HTTP serve: %w", serveErr)
		}
		if shutdownErr != nil {
			return fmt.Errorf("HTTP shutdown: %w", shutdownErr)
		}
		logger.Info("order service stopped")
		return nil
	}
}
