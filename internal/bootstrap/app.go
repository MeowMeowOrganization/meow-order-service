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
	"github.com/MeowMeowOrganization/meow-order-service/internal/infrastructure/postgres"
	rpc "github.com/MeowMeowOrganization/meow-order-service/internal/transport/connectrpc"
	"github.com/MeowMeowOrganization/meow-order-service/internal/transport/health"
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

	return serve(ctx, listener, srv, healthHandler, cfg.ShutdownTimeout, logger)
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
