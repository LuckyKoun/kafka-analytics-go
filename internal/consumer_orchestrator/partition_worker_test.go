package consumerorchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"kafka-golang-analytics/internal/types"
	useractivity "kafka-golang-analytics/internal/user_activity"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type StorageMock struct {
	mu        sync.Mutex
	calls     int
	failures  int
	gate      chan struct{}
	persisted map[types.Key]int
}

func (s *StorageMock) IncrementCounts(_ context.Context, deltas map[types.Key]int) error {
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failures {
		return errors.New("db down")
	}
	if s.persisted == nil {
		s.persisted = make(map[types.Key]int)
	}
	for k, n := range deltas {
		s.persisted[k] += n
	}
	return nil
}

func (s *StorageMock) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *StorageMock) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	sum := 0
	for _, n := range s.persisted {
		sum += n
	}
	return sum
}

type CommiterMock struct {
	mu       sync.Mutex
	failures int
	attempts int
	commits  []*kgo.Record
}

func (c *CommiterMock) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts++
	if c.attempts <= c.failures {
		return errors.New("commit failed")
	}
	c.commits = append(c.commits, rs...)
	return nil
}

func (c *CommiterMock) committed() []*kgo.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*kgo.Record(nil), c.commits...)
}

func (c *CommiterMock) attemptCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestWorker(storage UserActivityStorage, committer recordCommitter) *partitionWorker {
	w := newPartitionWorker("topic", 3, ConsumerOrchestratorConfig{
		ConsumerFlushTimeout: 2,
		DrainingTimeout:      2,
		PartitionBuffer:      4,
	}, testLogger(), useractivity.NewAnalysisService(), storage, committer)
	w.retryInterval = 10 * time.Millisecond
	return w
}

func activityRecord(offset int64, userID string) *kgo.Record {
	return &kgo.Record{
		Topic:     "topic",
		Partition: 3,
		Offset:    offset,
		Value:     []byte(fmt.Sprintf(`{"user_id":%q,"activity_type":"page_view"}`, userID)),
	}
}

func rawRecord(offset int64, value string) *kgo.Record {
	return &kgo.Record{Topic: "topic", Partition: 3, Offset: offset, Value: []byte(value)}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func disableTimerRetries(w *partitionWorker) {
	w.retryInterval = time.Hour
}

func stopAndWait(w *partitionWorker, graceful bool) {
	w.stop(graceful)
	w.wait()
}

func TestWorkerCommitsOnlyLastRecordAfterFlush(t *testing.T) {
	storage := &StorageMock{}
	committer := &CommiterMock{}
	w := newTestWorker(storage, committer)
	go w.run()

	w.enqueue(context.Background(), []*kgo.Record{
		activityRecord(10, "u1"), activityRecord(11, "u1"), activityRecord(12, "u2"),
	})

	eventually(t, "offset commit", func() bool { return len(committer.committed()) == 1 })
	stopAndWait(w, true)

	if got := committer.committed()[0].Offset; got != 12 {
		t.Fatalf("committed offset %d, want 12", got)
	}
	if got := storage.total(); got != 3 {
		t.Fatalf("persisted %d activities, want 3", got)
	}
	if got := storage.persisted[types.Key{UserID: "u1", ActivityType: "page_view"}]; got != 2 {
		t.Fatalf("u1 persisted %d, want 2", got)
	}
}

func TestWorkerSkipsMalformedButStillCommits(t *testing.T) {
	storage := &StorageMock{}
	committer := &CommiterMock{}
	w := newTestWorker(storage, committer)
	go w.run()

	w.enqueue(context.Background(), []*kgo.Record{activityRecord(1, "u1"), rawRecord(2, "{not json")})

	eventually(t, "offset commit", func() bool { return len(committer.committed()) == 1 })
	stopAndWait(w, true)

	if got := committer.committed()[0].Offset; got != 2 {
		t.Fatalf("committed offset %d, want 2 (malformed record is skipped but committed)", got)
	}
	if got := storage.total(); got != 1 {
		t.Fatalf("persisted %d activities, want 1", got)
	}
}

func TestWorkerFlushFailureIsRetriedWithoutCommitting(t *testing.T) {
	storage := &StorageMock{failures: 2}
	committer := &CommiterMock{}
	w := newTestWorker(storage, committer)
	go w.run()

	w.enqueue(context.Background(), []*kgo.Record{activityRecord(5, "u1"), activityRecord(6, "u1")})

	eventually(t, "offset commit after retries", func() bool { return len(committer.committed()) == 1 })
	stopAndWait(w, true)

	if got := committer.committed()[0].Offset; got != 6 {
		t.Fatalf("committed offset %d, want 6", got)
	}
	if got := storage.callCount(); got < 3 {
		t.Fatalf("storage called %d times, want at least 3 (2 failures then success)", got)
	}
	if got := storage.total(); got != 2 {
		t.Fatalf("persisted %d activities, want exactly 2 (restored counts must not be lost or doubled)", got)
	}
}

func TestWorkerCommitFailureDoesNotPersistTwice(t *testing.T) {
	storage := &StorageMock{}
	committer := &CommiterMock{failures: 2}
	w := newTestWorker(storage, committer)
	go w.run()

	w.enqueue(context.Background(), []*kgo.Record{activityRecord(7, "u1"), activityRecord(8, "u2")})

	eventually(t, "offset commit after retries", func() bool { return len(committer.committed()) == 1 })
	stopAndWait(w, true)

	if got := committer.committed()[0].Offset; got != 8 {
		t.Fatalf("committed offset %d, want 8", got)
	}
	if got := storage.total(); got != 2 {
		t.Fatalf("persisted %d activities, want exactly 2 (commit retries must not re-persist)", got)
	}
}

func TestWorkerGracefulStopDrainsBufferedBatches(t *testing.T) {
	storage := &StorageMock{gate: make(chan struct{})}
	committer := &CommiterMock{}
	w := newTestWorker(storage, committer)
	go w.run()

	batchThatParksTheWorkerInsideStorage := []*kgo.Record{activityRecord(1, "u1")}
	batchWaitingInTheBuffer := []*kgo.Record{activityRecord(2, "u2")}
	w.enqueue(context.Background(), batchThatParksTheWorkerInsideStorage)
	w.enqueue(context.Background(), batchWaitingInTheBuffer)
	w.stop(true)
	close(storage.gate)
	w.wait()

	if got := storage.total(); got != 2 {
		t.Fatalf("persisted %d activities, want 2", got)
	}
	commits := committer.committed()
	if len(commits) == 0 || commits[len(commits)-1].Offset != 2 {
		t.Fatalf("last committed record = %v, want offset 2", commits)
	}
}

func TestWorkerGracefulStopFlushesLeftoverFailedWork(t *testing.T) {
	storage := &StorageMock{failures: 1}
	committer := &CommiterMock{}
	w := newTestWorker(storage, committer)
	disableTimerRetries(w)
	go w.run()

	w.enqueue(context.Background(), []*kgo.Record{activityRecord(4, "u1")})
	eventually(t, "first (failing) flush", func() bool { return storage.callCount() == 1 })
	stopAndWait(w, true)

	if got := storage.total(); got != 1 {
		t.Fatalf("persisted %d activities, want 1", got)
	}
	if c := committer.committed(); len(c) != 1 || c[0].Offset != 4 {
		t.Fatalf("committed %v, want offset 4", c)
	}
}

func TestWorkerLostStopDoesNotFlushOrCommit(t *testing.T) {
	storage := &StorageMock{failures: 1000}
	committer := &CommiterMock{}
	w := newTestWorker(storage, committer)
	disableTimerRetries(w)
	go w.run()

	w.enqueue(context.Background(), []*kgo.Record{activityRecord(4, "u1")})
	eventually(t, "first (failing) flush", func() bool { return storage.callCount() == 1 })
	stopAndWait(w, false)

	if got := storage.callCount(); got != 1 {
		t.Fatalf("storage called %d times, want 1 (a lost partition must not flush)", got)
	}
	if got := committer.attemptCount(); got != 0 {
		t.Fatalf("commit attempted %d times, want 0", got)
	}
}

func TestWorkerStopIsIdempotentAndRejectsLateBatches(t *testing.T) {
	w := newTestWorker(&StorageMock{}, &CommiterMock{})
	go w.run()

	w.stop(true)
	w.stop(false)
	w.wait()

	if !w.flushOnStop {
		t.Fatal("second stop overrode the first")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	for i := 0; i < cap(w.recs); i++ {
		w.recs <- nil
	}
	if w.enqueue(ctx, []*kgo.Record{activityRecord(1, "u1")}) {
		t.Fatal("enqueue succeeded on a stopped worker with a full buffer")
	}
}

func TestNewPartitionWorkerReadsConfiguredTimeoutsAsWholeSeconds(t *testing.T) {
	w := newPartitionWorker("topic", 0, ConsumerOrchestratorConfig{ConsumerFlushTimeout: 10, DrainingTimeout: 7},
		testLogger(), useractivity.NewAnalysisService(), &StorageMock{}, &CommiterMock{})

	if w.flushTimeout != 10*time.Second || w.stopTimeout != 7*time.Second {
		t.Fatalf("flush timeout %v and stop timeout %v, want 10s and 7s", w.flushTimeout, w.stopTimeout)
	}
}

func TestNewPartitionWorkerTreatsANegativeBufferSizeAsNoBuffer(t *testing.T) {
	w := newPartitionWorker("topic", 0, ConsumerOrchestratorConfig{PartitionBuffer: -3},
		testLogger(), useractivity.NewAnalysisService(), &StorageMock{}, &CommiterMock{})

	if got := cap(w.recs); got != 0 {
		t.Fatalf("buffer capacity = %d, want 0", got)
	}
}

func TestEnqueueGivesUpWhenTheContextIsCancelledWhileTheBufferIsFull(t *testing.T) {
	w := newTestWorker(&StorageMock{}, &CommiterMock{})
	for i := 0; i < cap(w.recs); i++ {
		w.recs <- nil
	}
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()

	accepted := w.enqueue(cancelledContext, []*kgo.Record{activityRecord(1, "u1")})

	if accepted {
		t.Fatal("enqueue reported success although the buffer was full and the context cancelled")
	}
}

func TestEnqueueAcceptsABatchWhileTheBufferHasRoom(t *testing.T) {
	w := newTestWorker(&StorageMock{}, &CommiterMock{})

	accepted := w.enqueue(context.Background(), []*kgo.Record{activityRecord(1, "u1")})

	if !accepted || len(w.recs) != 1 {
		t.Fatalf("accepted=%v with %d queued batches, want accepted with 1 queued", accepted, len(w.recs))
	}
}
