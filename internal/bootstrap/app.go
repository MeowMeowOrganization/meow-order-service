package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	cancelapp "github.com/MeowMeowOrganization/meow-order-service/internal/application/cancel"
	queryapp "github.com/MeowMeowOrganization/meow-order-service/internal/application/privatequery"
	"github.com/MeowMeowOrganization/meow-order-service/internal/config"
	"github.com/MeowMeowOrganization/meow-order-service/internal/infrastructure/httpserver"
	"github.com/MeowMeowOrganization/meow-order-service/internal/infrastructure/kafka"
	"github.com/MeowMeowOrganization/meow-order-service/internal/infrastructure/postgres"
	rpc "github.com/MeowMeowOrganization/meow-order-service/internal/transport/connectrpc"
	"github.com/MeowMeowOrganization/meow-order-service/internal/transport/health"
	"github.com/MeowMeowOrganization/meow-order-service/internal/worker/lifecycle"
	"github.com/MeowMeowOrganization/meow-order-service/internal/worker/outbox"
)

// Run is the composition root: no other package constructs all dependencies.
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
	// The CancelOrder RPC is opt-in only when BOTH publication and
	// result-consumption workers are enabled. Never accept new commands when
	// this process cannot reconcile their committed matcher outcomes.
	var orderHandler rpc.OrderHandler
	if cfg.OutboxEnabled && cfg.LifecycleEnabled {
		cancelRepo, err := postgres.NewCancelRepository(pool)
		if err != nil {
			return err
		}
		cancelService, err := cancelapp.New(cancelRepo)
		if err != nil {
			return err
		}
		orderHandler = rpc.NewOrderHandler(cancelService)
	}
	queryRepo, err := postgres.NewPrivateQueryRepository(pool)
	if err != nil {
		return err
	}
	queryService, err := queryapp.New(queryRepo)
	if err != nil {
		return err
	}
	orderHandler = orderHandler.WithPrivateQueries(queryService)
	rpc.Register(mux, orderHandler)
	srv := httpserver.New(cfg.HTTPAddr, mux, logger)
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("bind HTTP listener: %w", err)
	}

	var tasks []backgroundTask
	if cfg.OutboxEnabled {
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
		tasks = append(tasks, backgroundTask{name: "matching outbox", run: worker.Run})
		logger.Info("matching outbox configured", "topic", cfg.MatchingTopic)
	}
	if cfg.LifecycleEnabled {
		repository, err := postgres.NewLifecycleRepository(pool)
		if err != nil {
			_ = listener.Close()
			return err
		}
		consumer, err := kafka.NewLifecycleConsumer(cfg.KafkaBrokers, cfg.ResultsTopic, cfg.LifecycleGroupID)
		if err != nil {
			_ = listener.Close()
			return err
		}
		defer func() {
			if err := consumer.Close(); err != nil {
				logger.Warn("Kafka lifecycle consumer close failed", "error", err)
			}
		}()
		worker, err := lifecycle.New(consumer, repository, logger, cfg.ResultsTopic, lifecycle.Options{
			ApplyTimeout: cfg.LifecycleApplyTimeout, CommitTimeout: cfg.LifecycleCommitTimeout,
			RetryBase: cfg.LifecycleRetryBase, RetryMax: cfg.LifecycleRetryMax,
		})
		if err != nil {
			_ = listener.Close()
			return err
		}
		tasks = append(tasks, backgroundTask{name: "Kafka matching-result consumer", run: worker.Run})
		logger.Info("Kafka lifecycle consumer configured", "topic", cfg.ResultsTopic, "group", cfg.LifecycleGroupID)
	}
	return runManaged(ctx, listener, srv, healthHandler, cfg.ShutdownTimeout, logger, tasks)
}

type backgroundTask struct {
	name string
	run  func(context.Context) error
}
type processExit struct {
	name string
	err  error
}

// runManaged supervises the HTTP server and both optional workers. A worker
// ending unexpectedly (even with a nil error) is a service failure: cancel
// the shared context, remove readiness and drain HTTP and remaining workers.
func runManaged(ctx context.Context, ln net.Listener, srv *http.Server, readiness readinessState,
	timeout time.Duration, logger *slog.Logger, tasks []backgroundTask) error {
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	exits := make(chan processExit, len(tasks)+1)
	go func() { exits <- processExit{name: "HTTP", err: serve(runCtx, ln, srv, readiness, timeout, logger)} }()
	for _, task := range tasks {
		task := task
		go func() { exits <- processExit{name: task.name, err: task.run(runCtx)} }()
	}
	first := <-exits
	var problems []error
	if ctx.Err() == nil {
		if first.err == nil {
			problems = append(problems, fmt.Errorf("%s stopped unexpectedly", first.name))
		} else {
			problems = append(problems, fmt.Errorf("%s failed: %w", first.name, first.err))
		}
	} else if first.err != nil {
		problems = append(problems, fmt.Errorf("%s shutdown: %w", first.name, first.err))
	}
	readiness.MarkStopping()
	stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for i := 0; i < len(tasks); i++ {
		select {
		case other := <-exits:
			if other.err != nil && !errors.Is(other.err, context.Canceled) {
				problems = append(problems, fmt.Errorf("%s: %w", other.name, other.err))
			}
		case <-deadline.C:
			return errors.Join(append(problems, errors.New("background worker or HTTP shutdown exceeded timeout"))...)
		}
	}
	return errors.Join(problems...)
}

type readinessState interface {
	MarkServing()
	MarkStopping()
}

func serve(ctx context.Context, ln net.Listener, srv *http.Server, readiness readinessState, timeout time.Duration, logger *slog.Logger) error {
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
