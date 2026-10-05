package main

import (
	"context"
	"errors"
	"kafka-golang-analytics/cmd/api/routers"
	"kafka-golang-analytics/internal/config"
	"kafka-golang-analytics/internal/logging"
	"kafka-golang-analytics/internal/storage"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.LoadAPIConfig()

	logger := logging.New(cfg.Logging)

	storageCtx, cancelStorage := context.WithTimeout(context.Background(), time.Duration(cfg.DatabaseConfig.ConnectionTimeout)*time.Second)
	statsStore, err := storage.NewStatsStore(storageCtx, cfg.DatabaseConfig)

	cancelStorage()

	if err != nil {
		logger.Error("failed to init postgres database connection", "error", err)
		os.Exit(1)
	}

	defer statsStore.Close()

	server := &http.Server{
		Addr:        cfg.HttpAddr,
		Handler:     routers.Router(statsStore, logger, time.Duration(cfg.QueryTimeout)*time.Second),
		ReadTimeout: time.Duration(cfg.HttpReadTimeout) * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go runServer(server, logger, cfg, stop)

	<-ctx.Done()

	logger.Info("shutting down api")

	httpShutdownCtx, httpServerCancel := context.WithTimeout(context.Background(), time.Duration(cfg.HttpShutdownTimeout)*time.Second)

	defer httpServerCancel()

	err = server.Shutdown(httpShutdownCtx)

	if err != nil {
		logger.Error("failed to gracefully shutdown httpserver", "error", err)
	}
}

func runServer(server *http.Server, logger *slog.Logger, cfg config.APIConfig, stop context.CancelFunc) {
	logger.Info("starting http server at", "addr", cfg.HttpAddr)
	err := server.ListenAndServe()

	// Shutdown makes ListenAndServe return ErrServerClosed, which is not a failure.
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("failed to start http server", "error", err)
		stop()
	}
}
