package consumerorchestrator

import (
	"context"
	"errors"
	"kafka-golang-analytics/internal/types"
	useractivity "kafka-golang-analytics/internal/user_activity"
	"log/slog"
	"sync"

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

// ConsumerOrchestratorConfig holds timeouts in seconds.
type ConsumerOrchestratorConfig struct {
	ConsumerFlushTimeout int
	// DrainingTimeout bounds how long a stopping partition worker may spend on
	// its final flush + commit. Keep it well below the group rebalance timeout
	// (60s by default in franz-go), since revokes wait for it.
	DrainingTimeout int
	// PartitionBuffer is how many polled batches may queue per partition worker.
	PartitionBuffer int
	// MaxPollRecords bounds how many records one poll returns across partitions.
	MaxPollRecords int
}

type topicPartition struct {
	topic     string
	partition int32
}

// ConsumerOrchestrator runs one partitionWorker goroutine per partition
// currently assigned to this group member. The set of workers follows group
// rebalances through the callbacks from RebalanceOpts.
type ConsumerOrchestrator struct {
	cfg                 ConsumerOrchestratorConfig
	logger              *slog.Logger
	userActivityStorage UserActivityStorage
	newUserActivitySvc  func() UserActivityAnalysisService

	mu      sync.Mutex
	workers map[topicPartition]*partitionWorker
}

func New(cfg ConsumerOrchestratorConfig, logger *slog.Logger, storage UserActivityStorage) *ConsumerOrchestrator {
	return &ConsumerOrchestrator{
		cfg:                 cfg,
		logger:              logger,
		userActivityStorage: storage,
		newUserActivitySvc: func() UserActivityAnalysisService {
			return useractivity.NewAnalysisService()
		},
		workers: make(map[topicPartition]*partitionWorker),
	}
}

// RebalanceOpts returns the client options that tie partition workers to group
// membership. They must be passed to kgo.NewClient. BlockRebalanceOnPoll makes
// the callbacks run serially with polling, so a worker is never stopped (or
// started) between a poll and the dispatch of its records, and offsets are
// never committed for partitions this member no longer owns.
func (c *ConsumerOrchestrator) RebalanceOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
	}
}

// The default balancer is cooperative-sticky, so assigned and revoked only
// carry the partitions that changed. Workers for the rest keep running.
func (c *ConsumerOrchestrator) onAssigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for topic, partitions := range assigned {
		for _, partition := range partitions {
			c.startWorkerLocked(topic, partition, cl)
		}
	}
}

// onRevoked is called at the end of every group session, possibly with no
// partitions. Workers get to flush and commit before the partitions move.
func (c *ConsumerOrchestrator) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	c.stopWorkers(c.takeWorkers(revoked), true)
}

// onLost means the session is gone and commits would fail or be stale, so the
// workers drop their uncommitted state without flushing.
func (c *ConsumerOrchestrator) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.stopWorkers(c.takeWorkers(lost), false)
}

func (c *ConsumerOrchestrator) startWorkerLocked(topic string, partition int32, committer recordCommitter) {
	key := topicPartition{topic, partition}
	if _, ok := c.workers[key]; ok {
		c.logger.Warn("partition already has a worker", "topic", topic, "partition", partition)
		return
	}

	w := newPartitionWorker(topic, partition, c.cfg, c.logger, c.newUserActivitySvc(), c.userActivityStorage, committer)
	c.workers[key] = w
	go w.run()
	c.logger.Info("partition worker started", "topic", topic, "partition", partition)
}

// takeWorkers removes the given partitions from the registry and returns their
// workers, so each worker is stopped by exactly one caller.
func (c *ConsumerOrchestrator) takeWorkers(partitions map[string][]int32) []*partitionWorker {
	c.mu.Lock()
	defer c.mu.Unlock()

	var taken []*partitionWorker
	for topic, ps := range partitions {
		for _, partition := range ps {
			key := topicPartition{topic, partition}
			if w, ok := c.workers[key]; ok {
				delete(c.workers, key)
				taken = append(taken, w)
			}
		}
	}
	return taken
}

func (c *ConsumerOrchestrator) takeAllWorkers() []*partitionWorker {
	c.mu.Lock()
	defer c.mu.Unlock()

	taken := make([]*partitionWorker, 0, len(c.workers))
	for key, w := range c.workers {
		delete(c.workers, key)
		taken = append(taken, w)
	}
	return taken
}

// stopWorkers signals every worker first so they flush concurrently, then
// waits for all of them.
func (c *ConsumerOrchestrator) stopWorkers(workers []*partitionWorker, graceful bool) {
	for _, w := range workers {
		w.stop(graceful)
	}
	for _, w := range workers {
		w.wait()
	}
}

func (c *ConsumerOrchestrator) dispatch(ctx context.Context, p kgo.FetchTopicPartition) {
	if len(p.Records) == 0 {
		return
	}

	c.mu.Lock()
	w := c.workers[topicPartition{p.Topic, p.Partition}]
	c.mu.Unlock()

	if w == nil {
		// BlockRebalanceOnPoll should make this impossible.
		c.logger.Error("no worker for partition; records left uncommitted",
			"topic", p.Topic, "partition", p.Partition, "records", len(p.Records))
		return
	}
	if !w.enqueue(ctx, p.Records) {
		c.logger.Warn("partition worker not accepting records; they will be redelivered",
			"topic", p.Topic, "partition", p.Partition, "records", len(p.Records))
	}
}

// Run polls and dispatches each partition's records to that partition's
// worker. It returns after ctx is cancelled (or the client is closed) and all
// workers have flushed and committed what they had.
//
// The send into a worker's buffer blocks while it is full, and rebalances stay
// blocked until the poll loop calls AllowRebalance. A slow partition therefore
// slows polling for all partitions, so keep PartitionBuffer and MaxPollRecords
// small relative to the flush timeout.
func (c *ConsumerOrchestrator) Run(ctx context.Context, client *kgo.Client) {
	for {
		fetches := client.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			client.AllowRebalance()
			break
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				c.logger.Error("fetch error", "topic", topic, "partition", partition, "error", err)
			}
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			c.dispatch(ctx, p)
		})
		client.AllowRebalance()
	}

	c.stopWorkers(c.takeAllWorkers(), true)
}

func (c *ConsumerOrchestrator) Shutdown(ctx context.Context, client *kgo.Client) {
	err := client.LeaveGroupContext(ctx)
	if err != nil {
		c.logger.Warn("group leave failed; client will close via timeout", "error", err)
	}

	closed := make(chan struct{})
	go func() {
		client.Close()
		close(closed)
	}()
	select {
	case <-closed:
		c.logger.Info("Consumer client closed sucessfully.")
	case <-ctx.Done():
		c.logger.Warn("kafka client close timed out; exiting without clean close")
	}
}
