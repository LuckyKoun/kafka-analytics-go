package consumerorchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"kafka-golang-analytics/internal/types"
	useractivity "kafka-golang-analytics/internal/user_activity"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type UserActivityStorage interface {
	IncrementCounts(ctx context.Context, deltas map[types.Key]int) error
}

type UserActivityAnalysisService interface {
	Add(activity types.UserActivity)
	Drain() map[types.Key]int
	Restore(map[types.Key]int)
}

type ConsumerOrchestratorConfig struct {
	ConsumerFlushTimeout int
}

type ConsumerOrchestrator struct {
	cfg                 ConsumerOrchestratorConfig
	client              *kgo.Client
	logger              *slog.Logger
	userActivitySvc     UserActivityAnalysisService
	userActivityStorage UserActivityStorage

	failedToCommit []*kgo.Record
}

func New(client *kgo.Client, cfg ConsumerOrchestratorConfig, logger *slog.Logger, storage UserActivityStorage) *ConsumerOrchestrator {
	userActivitySvc := useractivity.NewAnalysisService()
	return &ConsumerOrchestrator{
		cfg:                 cfg,
		client:              client,
		logger:              logger,
		userActivitySvc:     userActivitySvc,
		userActivityStorage: storage,
	}
}

func (c *ConsumerOrchestrator) poll(ctx context.Context) []*kgo.Record {
	fetches := c.client.PollFetches(ctx)
	if fetches.IsClientClosed() || ctx.Err() != nil {
		return nil
	}
	fetches.EachError(func(topic string, partition int32, err error) {
		if !errors.Is(err, context.Canceled) {
			c.logger.Error("fetch error", "topic", topic, "partition", partition, "error", err)
		}
	})
	recs := fetches.Records()
	if recs == nil {
		recs = []*kgo.Record{}
	}
	return recs
}

func (c *ConsumerOrchestrator) flush(ctx context.Context) error {
	deltas := c.userActivitySvc.Drain()
	err := c.userActivityStorage.IncrementCounts(ctx, deltas)
	if err != nil {
		c.userActivitySvc.Restore(deltas)
		return fmt.Errorf("persist user activity deltas: %w", err)
	}
	return nil
}

func (c *ConsumerOrchestrator) flushAndCommit(recs []*kgo.Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.cfg.ConsumerFlushTimeout))
	defer cancel()

	if err := c.flush(ctx); err != nil {
		c.failedToCommit = append(c.failedToCommit, recs...)
		return err
	}

	toCommit := recs
	if len(c.failedToCommit) > 0 {
		toCommit = append(c.failedToCommit, recs...)
	}
	err := c.client.CommitRecords(ctx, toCommit...)
	if err != nil {
		c.failedToCommit = toCommit
		return err
	}
	c.failedToCommit = nil
	return nil
}

func (c *ConsumerOrchestrator) process(rec *kgo.Record) {
	var activity types.UserActivity
	err := json.Unmarshal(rec.Value, &activity)
	if err != nil {
		c.logger.Warn("skipping malformed message",
			"error", err, "partition", rec.Partition, "offset", rec.Offset)
		return
	}
	c.userActivitySvc.Add(activity)
	c.logger.Info("user activity recorded",
		"user_id", activity.UserID,
		"activity_type", activity.ActivityType,
		"partition", rec.Partition,
		"offset", rec.Offset,
	)
}

func (c *ConsumerOrchestrator) Shutdown(ctx context.Context) {
	err := c.client.LeaveGroupContext(ctx)
	if err != nil {
		c.logger.Warn("group leave failed; client will close via timeout", "error", err)
	}

	closed := make(chan struct{})
	go func() {
		c.client.Close()
		close(closed)
	}()
	select {
	case <-closed:
		c.logger.Info("Consumer client closed sucessfully.")
	case <-ctx.Done():
		c.logger.Warn("kafka client close timed out; exiting without clean close")
	}
}

func (c *ConsumerOrchestrator) Run(ctx context.Context) {
	batches := make(chan []*kgo.Record, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(batches)
		for {
			recs := c.poll(ctx)
			if recs == nil {
				return
			}
			if len(recs) == 0 {
				continue
			}
			select {
			case batches <- recs:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for recs := range batches {
			for _, rec := range recs {
				c.process(rec)
			}

			err := c.flushAndCommit(recs)

			if err != nil {
				c.logger.Error("failed to commit user activity results", "error", err)
			}
		}
	}()

	wg.Wait()
}
