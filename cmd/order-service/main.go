package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/MeowMeowOrganization/meow-order-service/internal/bootstrap"
	"github.com/MeowMeowOrganization/meow-order-service/internal/config"
	"github.com/MeowMeowOrganization/meow-order-service/internal/logging"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		// Configuration errors are designed to exclude secret values.
		logging.New(os.Stdout, slog.LevelInfo).Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	logger := logging.New(os.Stdout, cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := bootstrap.Run(ctx, cfg, logger); err != nil {
		logger.Error("order service stopped with error", "error", err)
		os.Exit(1)
	}
}
