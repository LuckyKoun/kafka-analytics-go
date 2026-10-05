package consumerorchestrator

import (
	"context"
	"kafka-golang-analytics/internal/types"
	useractivity "kafka-golang-analytics/internal/user_activity"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func newTestOrchestrator(storage UserActivityStorage) *ConsumerOrchestrator {
	c := New(ConsumerOrchestratorConfig{ConsumerFlushTimeout: 2, DrainingTimeout: 2, PartitionBuffer: 4}, testLogger(), storage)
	return c
}

func (c *ConsumerOrchestrator) startForTest(topic string, partition int32, committer recordCommitter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startWorkerLocked(topic, partition, committer)
}

func (c *ConsumerOrchestrator) workerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.workers)
}

func TestEachPartitionGetsItsOwnAnalyticsInstance(t *testing.T) {
	c := newTestOrchestrator(&fakeStorage{})
	c.startForTest("topic", 0, &fakeCommitter{})
	c.startForTest("topic", 1, &fakeCommitter{})
	defer func() { c.stopWorkers(c.takeAllWorkers(), false) }()

	w0 := c.workers[topicPartition{"topic", 0}]
	w1 := c.workers[topicPartition{"topic", 1}]
	if w0.svc == w1.svc {
		t.Fatal("partition workers share one analytics service; Drain would mix partitions")
	}
	if _, ok := w0.svc.(*useractivity.UserActivityAnalysisService); !ok {
		t.Fatalf("unexpected analytics service type %T", w0.svc)
	}
}

func TestStartWorkerTwiceKeepsTheFirst(t *testing.T) {
	c := newTestOrchestrator(&fakeStorage{})
	c.startForTest("topic", 0, &fakeCommitter{})
	first := c.workers[topicPartition{"topic", 0}]
	c.startForTest("topic", 0, &fakeCommitter{})
	defer func() { c.stopWorkers(c.takeAllWorkers(), false) }()

	if c.workerCount() != 1 || c.workers[topicPartition{"topic", 0}] != first {
		t.Fatal("duplicate assignment replaced or duplicated the worker")
	}
}

func TestRevokeFlushesAndCommitsOnlyRevokedPartitions(t *testing.T) {
	storage := &fakeStorage{failures: 1}
	committers := map[int32]*fakeCommitter{0: {}, 1: {}}
	c := newTestOrchestrator(storage)
	for p, cm := range committers {
		c.startForTest("topic", p, cm)
	}
	defer func() { c.stopWorkers(c.takeAllWorkers(), false) }()

	// Partition 0's first flush fails, so it holds uncommitted work when revoked.
	c.dispatch(context.Background(), kgo.FetchTopicPartition{
		Topic: "topic",
		FetchPartition: kgo.FetchPartition{
			Partition: 0,
			Records:   []*kgo.Record{activityRecordFor("topic", 0, 9, "u1")},
		},
	})
	eventually(t, "first (failing) flush", func() bool { return storage.callCount() == 1 })

	c.onRevoked(context.Background(), nil, map[string][]int32{"topic": {0}})

	if c.workerCount() != 1 {
		t.Fatalf("workers after revoke = %d, want 1", c.workerCount())
	}
	if _, ok := c.workers[topicPartition{"topic", 1}]; !ok {
		t.Fatal("revoking partition 0 removed partition 1's worker")
	}
	if got := committers[0].committed(); len(got) != 1 || got[0].Offset != 9 {
		t.Fatalf("revoked partition committed %v, want offset 9", got)
	}
	if got := committers[1].attemptCount(); got != 0 {
		t.Fatalf("surviving partition committed %d times, want 0", got)
	}
}

func TestLostDropsWorkersWithoutCommitting(t *testing.T) {
	storage := &fakeStorage{failures: 1000}
	cm := &fakeCommitter{}
	c := newTestOrchestrator(storage)
	c.startForTest("topic", 0, cm)

	c.dispatch(context.Background(), kgo.FetchTopicPartition{
		Topic: "topic",
		FetchPartition: kgo.FetchPartition{
			Partition: 0,
			Records:   []*kgo.Record{activityRecordFor("topic", 0, 3, "u1")},
		},
	})
	eventually(t, "first (failing) flush", func() bool { return storage.callCount() == 1 })

	c.onLost(context.Background(), nil, map[string][]int32{"topic": {0}})

	if c.workerCount() != 0 {
		t.Fatalf("workers after lost = %d, want 0", c.workerCount())
	}
	if storage.callCount() != 1 || cm.attemptCount() != 0 {
		t.Fatalf("lost partition flushed (%d calls) or committed (%d)", storage.callCount(), cm.attemptCount())
	}
}

func TestRevokeWithNothingAssignedIsANoop(t *testing.T) {
	c := newTestOrchestrator(&fakeStorage{})
	done := make(chan struct{})
	go func() {
		c.onRevoked(context.Background(), nil, map[string][]int32{})
		c.onRevoked(context.Background(), nil, map[string][]int32{"topic": {5}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("onRevoked blocked with nothing to revoke")
	}
}

func TestDispatchWithoutWorkerDoesNotPanic(t *testing.T) {
	c := newTestOrchestrator(&fakeStorage{})
	c.dispatch(context.Background(), kgo.FetchTopicPartition{
		Topic: "topic",
		FetchPartition: kgo.FetchPartition{
			Partition: 7,
			Records:   []*kgo.Record{activityRecordFor("topic", 7, 1, "u1")},
		},
	})
}

func TestStopAllDrainsEveryWorker(t *testing.T) {
	storage := &fakeStorage{}
	committers := map[int32]*fakeCommitter{0: {}, 1: {}, 2: {}}
	c := newTestOrchestrator(storage)
	for p, cm := range committers {
		c.startForTest("topic", p, cm)
		c.dispatch(context.Background(), kgo.FetchTopicPartition{
			Topic: "topic",
			FetchPartition: kgo.FetchPartition{
				Partition: p,
				Records:   []*kgo.Record{activityRecordFor("topic", p, int64(p)+100, "u1")},
			},
		})
	}

	c.stopWorkers(c.takeAllWorkers(), true)

	for p, cm := range committers {
		got := cm.committed()
		if len(got) != 1 || got[0].Offset != int64(p)+100 {
			t.Fatalf("partition %d committed %v, want offset %d", p, got, int64(p)+100)
		}
	}
	if got := storage.total(); got != 3 {
		t.Fatalf("persisted %d activities, want 3", got)
	}
	if storage.persisted[types.Key{UserID: "u1", ActivityType: "page_view"}] != 3 {
		t.Fatalf("unexpected persisted counts %v", storage.persisted)
	}
}

func activityRecordFor(topic string, partition int32, offset int64, userID string) *kgo.Record {
	r := activityRecord(offset, userID)
	r.Topic = topic
	r.Partition = partition
	return r
}
