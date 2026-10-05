package main

import (
	"context"
	"kafka-golang-analytics/internal/config"
	consumerorchestrator "kafka-golang-analytics/internal/consumer_orchestrator"
	"kafka-golang-analytics/internal/logging"
	"kafka-golang-analytics/internal/storage"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := config.LoadConsumerConfig()

	logger := logging.New(cfg.Logging)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.ConsumerTopic),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		logger.Error("kafka client init failed", "error", err)
		os.Exit(1)
	}

	defer client.Close()

	storageCtx, cancelStorage := context.WithTimeout(context.Background(), time.Duration(cfg.DatabaseConfig.ConnectionTimeout))
	userActiviryStorage, err := storage.NewStatsStore(storageCtx, cfg.DatabaseConfig)

	cancelStorage()

	if err != nil {
		logger.Error("failed to init postgress database connection", "error", err)
		os.Exit(1)
	}

	orchestrator := consumerorchestrator.New(client, cfg.ConsumerOrchestrator, logger, userActiviryStorage)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("Starting consumer", "brokers", cfg.Brokers, "topic", cfg.ConsumerTopic, "group_id", cfg.GroupID)

	orchestrator.Run(ctx)

	logger.Info("Shutting down consumer")

	drainingCtx, drainingCancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConsumerDrainingTimeout))

	defer drainingCancel()

	orchestrator.Shutdown(drainingCtx)
}
