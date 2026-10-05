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

	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	storageCtx, cancelStorage := context.WithTimeout(context.Background(), time.Duration(cfg.DatabaseConfig.ConnectionTimeout)*time.Second)
	userActiviryStorage, err := storage.NewStatsStore(storageCtx, cfg.DatabaseConfig)

	cancelStorage()

	if err != nil {
		logger.Error("failed to init postgress database connection", "error", err)
		os.Exit(1)
	}

	defer userActiviryStorage.Close()

	orchestrator := consumerorchestrator.New(cfg.ConsumerOrchestrator, logger, userActiviryStorage)

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.ConsumerTopic),
		kgo.DisableAutoCommit(),
	}
	opts = append(opts, orchestrator.RebalanceOpts()...)

	client, err := kgo.NewClient(opts...)

	if err != nil {
		logger.Error("kafka client init failed", "error", err)
		os.Exit(1)
	}

	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("Starting consumer", "brokers", cfg.Brokers, "topic", cfg.ConsumerTopic, "group_id", cfg.GroupID)

	orchestrator.Run(ctx, client)

	logger.Info("Shutting down consumer")

	drainingCtx, drainingCancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConsumerDrainingTimeout)*time.Second)

	defer drainingCancel()

	orchestrator.Shutdown(drainingCtx, client)
}
