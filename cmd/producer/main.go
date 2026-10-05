package main

import (
	"context"
	"log/slog"
	"moveoai-backend-chall/cmd/producer/routers"
	"moveoai-backend-chall/internal/config"
	"moveoai-backend-chall/internal/logging"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {

	cfg := config.LoadProducerConfig()

	logger := logging.New(cfg.Logging)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.ProduceTopic),
	)

	if err != nil {
		logger.Error("failed to init producer client", "error", err)
		os.Exit(1)
	}

	defer client.Close()

	produceCtx, cancelProduce := context.WithCancel(context.Background())
	defer cancelProduce()

	server := &http.Server{
		Addr:        cfg.HttpAddr,
		Handler:     routers.Producer(produceCtx, client, logger),
		ReadTimeout: time.Duration(cfg.ReadTimeout),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go runServer(server, logger, cfg, stop)

	<-ctx.Done()

	logger.Info("shutting down producer")

	httpShutdownCtx, httpServerCancel := context.WithTimeout(context.Background(), time.Duration(cfg.HttpShutdownTimeout))

	defer httpServerCancel()

	err = server.Shutdown(httpShutdownCtx)

	if err != nil {
		logger.Error("failed to gracefully shutdown httpserver", "error", err)
	}

	cancelProduce()

	drainingCtx, cancelDraining := context.WithTimeout(context.Background(), time.Duration(cfg.BufferDrainingTimeout))
	defer cancelDraining()

	err = client.Flush(drainingCtx)

	if err != nil {
		logger.Error("Draining buffer failed", "error", err)
	} else {
		logger.Info("Buffered drained sucessfully")
	}
}

func runServer(server *http.Server, logger *slog.Logger, cfg config.ProducerConfig, stop context.CancelFunc) {
	logger.Info("starting http server at", "addr", cfg.HttpAddr, "brokers", cfg.Brokers, "topic", cfg.ProduceTopic)
	err := server.ListenAndServe()

	if err != nil {
		logger.Error("failed to start http server", "error", err)
		stop()
	}
}
