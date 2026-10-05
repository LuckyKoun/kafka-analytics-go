package consumerorchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-golang-analytics/internal/types"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const defaultRetryInterval = 2 * time.Second

// recordCommitter is the slice of *kgo.Client a worker needs.
type recordCommitter interface {
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
}

// partitionWorker consumes exactly one topic-partition on its own goroutine.
//
// It owns its UserActivityAnalysisService instance. That is what keeps the
// invariant "a partition's offset is committed only after that partition's
// counts are durable": Drain takes everything in the service, so sharing one
// across partitions would let one worker persist (or commit past) another
// worker's counts.
type partitionWorker struct {
	topic     string
	partition int32
	logger    *slog.Logger

	svc       UserActivityAnalysisService
	storage   UserActivityStorage
	committer recordCommitter

	flushTimeout  time.Duration
	stopTimeout   time.Duration
	retryInterval time.Duration

	recs chan []*kgo.Record
	quit chan struct{}
	done chan struct{}

	stopOnce sync.Once
	// flushOnStop is written before quit is closed and read after quit is
	// received, so the channel close orders the access.
	flushOnStop bool

	// pending is the highest record handled but not yet committed. It is only
	// touched by run().
	pending *kgo.Record
}

func newPartitionWorker(
	topic string,
	partition int32,
	cfg ConsumerOrchestratorConfig,
	logger *slog.Logger,
	svc UserActivityAnalysisService,
	storage UserActivityStorage,
	committer recordCommitter,
) *partitionWorker {
	return &partitionWorker{
		topic:         topic,
		partition:     partition,
		logger:        logger.With("topic", topic, "partition", partition),
		svc:           svc,
		storage:       storage,
		committer:     committer,
		flushTimeout:  time.Duration(cfg.ConsumerFlushTimeout) * time.Second,
		stopTimeout:   time.Duration(cfg.DrainingTimeout) * time.Second,
		retryInterval: defaultRetryInterval,
		recs:          make(chan []*kgo.Record, max(cfg.PartitionBuffer, 0)),
		quit:          make(chan struct{}),
		done:          make(chan struct{}),
	}
}

// enqueue hands a batch to the worker. It blocks while the worker's buffer is
// full, which is the backpressure point of the whole consumer.
func (w *partitionWorker) enqueue(ctx context.Context, recs []*kgo.Record) bool {
	select {
	case w.recs <- recs:
		return true
	case <-w.quit:
		return false
	case <-ctx.Done():
		return false
	}
}

// stop asks the worker to exit and is safe to call more than once. A graceful
// stop drains buffered batches and does a final flush + commit. A non-graceful
// stop (partition lost) skips them: the new owner reprocesses everything that
// was not committed, so flushing here would double count.
func (w *partitionWorker) stop(graceful bool) {
	w.stopOnce.Do(func() {
		w.flushOnStop = graceful
		close(w.quit)
	})
}

func (w *partitionWorker) wait() {
	<-w.done
}

func (w *partitionWorker) run() {
	defer close(w.done)

	var retry <-chan time.Time
	for {
		select {
		case recs := <-w.recs:
			w.handle(context.Background(), recs)
		case <-retry:
			w.flushAndCommit(context.Background())
		case <-w.quit:
			w.finish()
			return
		}

		// Without this an idle partition would never retry a failed flush.
		retry = nil
		if w.pending != nil {
			retry = time.After(w.retryInterval)
		}
	}
}

func (w *partitionWorker) finish() {
	if !w.flushOnStop {
		w.logger.Info("partition worker stopped without flushing")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.stopTimeout)
	defer cancel()

	for {
		if ctx.Err() != nil {
			w.logger.Warn("partition worker stop timed out; uncommitted records will be redelivered")
			return
		}
		select {
		case recs := <-w.recs:
			w.handle(ctx, recs)
		default:
			w.flushAndCommit(ctx)
			w.logger.Info("partition worker stopped")
			return
		}
	}
}

func (w *partitionWorker) handle(ctx context.Context, recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	for _, rec := range recs {
		w.process(rec)
	}
	// Records of one partition arrive in offset order.
	w.pending = recs[len(recs)-1]
	w.flushAndCommit(ctx)
}

func (w *partitionWorker) process(rec *kgo.Record) {
	var activity types.UserActivity
	err := json.Unmarshal(rec.Value, &activity)
	if err != nil {
		w.logger.Warn("skipping malformed message", "error", err, "offset", rec.Offset)
		return
	}
	w.svc.Add(activity)
	w.logger.Info("user activity recorded",
		"user_id", activity.UserID,
		"activity_type", activity.ActivityType,
		"offset", rec.Offset,
	)
}

func (w *partitionWorker) flush(ctx context.Context) error {
	deltas := w.svc.Drain()
	err := w.storage.IncrementCounts(ctx, deltas)
	if err != nil {
		w.svc.Restore(deltas)
		return fmt.Errorf("persist user activity deltas: %w", err)
	}
	return nil
}

// flushAndCommit persists the counts and then commits the pending offset. On
// any failure pending is kept and the next attempt (next batch, retry timer or
// final stop) covers it. A commit failure after a successful flush does not
// persist anything twice: the next flush drains only new counts.
func (w *partitionWorker) flushAndCommit(parent context.Context) {
	if w.pending == nil {
		return
	}

	ctx, cancel := context.WithTimeout(parent, w.flushTimeout)
	defer cancel()

	if err := w.flush(ctx); err != nil {
		w.logger.Error("failed to persist user activity", "error", err, "offset", w.pending.Offset)
		return
	}
	if err := w.committer.CommitRecords(ctx, w.pending); err != nil {
		w.logger.Error("failed to commit offset", "error", err, "offset", w.pending.Offset)
		return
	}
	w.pending = nil
}
