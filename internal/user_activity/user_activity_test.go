package useractivity

import (
	"kafka-golang-analytics/internal/types"
	"sync"
	"testing"
)

func activity(userID string) types.UserActivity {
	return types.UserActivity{UserID: userID, ActivityType: types.ActivityPageView}
}

func sum(m map[types.Key]int) int {
	total := 0
	for _, n := range m {
		total += n
	}
	return total
}

// The service is memory-safe under concurrent use (run with -race), and no
// count is lost or duplicated across Add / Drain / Restore.
func TestConcurrentAddDrainRestoreConservesCounts(t *testing.T) {
	const writers, perWriter = 8, 2000
	svc := NewAnalysisService()

	var writersWG, drainerWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				svc.Add(activity("user"))
			}
		}()
	}

	stop := make(chan struct{})
	var drained int
	drainerWG.Add(1)
	go func() {
		defer drainerWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			deltas := svc.Drain()
			if i%2 == 0 {
				// Simulates a failed flush: counts must come back.
				svc.Restore(deltas)
			} else {
				drained += sum(deltas)
			}
		}
	}()

	writersWG.Wait()
	close(stop)
	drainerWG.Wait()

	drained += sum(svc.Drain())
	if want := writers * perWriter; drained != want {
		t.Fatalf("accounted for %d activities, want %d", drained, want)
	}
}

// Documents why each partition worker needs its own instance: Drain returns
// everything in the service, regardless of which partition it came from.
func TestDrainTakesEverythingSoInstancesMustNotBeShared(t *testing.T) {
	shared := NewAnalysisService()
	shared.Add(activity("from-partition-0"))
	shared.Add(activity("from-partition-1"))
	if got := sum(shared.Drain()); got != 2 {
		t.Fatalf("shared Drain returned %d, want both partitions' counts (2)", got)
	}

	p0, p1 := NewAnalysisService(), NewAnalysisService()
	p0.Add(activity("from-partition-0"))
	p1.Add(activity("from-partition-1"))
	if got := sum(p0.Drain()); got != 1 {
		t.Fatalf("partition 0 instance drained %d, want 1", got)
	}
	if got := sum(p1.Drain()); got != 1 {
		t.Fatalf("partition 1 instance drained %d, want 1", got)
	}
}
