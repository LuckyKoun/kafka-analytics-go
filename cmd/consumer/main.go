package main

import (
	"context"
	"moveoai-backend-chall/internal/config"
	consumerorchestrator "moveoai-backend-chall/internal/consumer_orchestrator"
	"moveoai-backend-chall/internal/logging"
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

	orchestrator := consumerorchestrator.New(client, cfg.ConsumerOrchestrator, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("Starting consumer", "brokers", cfg.Brokers, "topic", cfg.ConsumerTopic, "group_id", cfg.GroupID)

	orchestrator.Run(ctx, logger)

	logger.Info("Shutting down consumer")

	drainingCtx, drainingCancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConsumerDrainingTimeout))

	defer drainingCancel()

	orchestrator.Shutdown(drainingCtx)

}
