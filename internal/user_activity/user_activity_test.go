package useractivity

import (
	"kafka-golang-analytics/internal/types"
	"sync"
	"testing"
)

func activityOf(userID string, activityType types.ActivityType) types.UserActivity {
	return types.UserActivity{UserID: userID, ActivityType: activityType}
}

func pageViewBy(userID string) types.UserActivity {
	return activityOf(userID, types.ActivityPageView)
}

func keyOf(userID, activityType string) types.Key {
	return types.Key{UserID: userID, ActivityType: activityType}
}

func sum(counts map[types.Key]int) int {
	total := 0
	for _, n := range counts {
		total += n
	}
	return total
}

func TestAddCountsEveryOccurrenceOfTheSameUserAndActivity(t *testing.T) {
	service := NewAnalysisService()

	service.Add(pageViewBy("u1"))
	service.Add(pageViewBy("u1"))
	service.Add(pageViewBy("u1"))

	if got := service.Drain()[keyOf("u1", "page_view")]; got != 3 {
		t.Fatalf("count for u1/page_view = %d, want 3", got)
	}
}

func TestAddKeepsDifferentActivityTypesOfTheSameUserSeparate(t *testing.T) {
	service := NewAnalysisService()

	service.Add(activityOf("u1", "page_view"))
	service.Add(activityOf("u1", "click"))
	service.Add(activityOf("u1", "click"))

	counts := service.Drain()
	if counts[keyOf("u1", "page_view")] != 1 || counts[keyOf("u1", "click")] != 2 {
		t.Fatalf("counts = %v, want page_view=1 and click=2", counts)
	}
}

func TestAddKeepsDifferentUsersSeparate(t *testing.T) {
	service := NewAnalysisService()

	service.Add(pageViewBy("u1"))
	service.Add(pageViewBy("u2"))
	service.Add(pageViewBy("u2"))

	counts := service.Drain()
	if counts[keyOf("u1", "page_view")] != 1 || counts[keyOf("u2", "page_view")] != 2 {
		t.Fatalf("counts = %v, want u1=1 and u2=2", counts)
	}
}

func TestAddIgnoresTimestampAndMetadataWhenCounting(t *testing.T) {
	service := NewAnalysisService()
	first := types.UserActivity{UserID: "u1", ActivityType: "page_view", Metadata: types.Metadata{PageUrl: "/a"}}
	second := types.UserActivity{UserID: "u1", ActivityType: "page_view", Metadata: types.Metadata{PageUrl: "/b"}}

	service.Add(first)
	service.Add(second)

	counts := service.Drain()
	if len(counts) != 1 || counts[keyOf("u1", "page_view")] != 2 {
		t.Fatalf("counts = %v, want a single u1/page_view entry with 2", counts)
	}
}

func TestDrainReturnsEverythingCountedSoFar(t *testing.T) {
	service := NewAnalysisService()
	service.Add(pageViewBy("u1"))
	service.Add(pageViewBy("u2"))

	if got := sum(service.Drain()); got != 2 {
		t.Fatalf("drained %d activities, want 2", got)
	}
}

func TestDrainLeavesTheServiceEmptyForTheNextRound(t *testing.T) {
	service := NewAnalysisService()
	service.Add(pageViewBy("u1"))
	service.Drain()

	if got := service.Drain(); len(got) != 0 {
		t.Fatalf("second drain returned %v, want nothing", got)
	}
}

func TestDrainOfAFreshServiceReturnsAnEmptyMapThatCallersMayRead(t *testing.T) {
	counts := NewAnalysisService().Drain()

	if counts == nil || len(counts) != 0 {
		t.Fatalf("Drain on a fresh service = %v, want an empty non-nil map", counts)
	}
}

func TestCountsAddedAfterADrainStartFromZero(t *testing.T) {
	service := NewAnalysisService()
	service.Add(pageViewBy("u1"))
	service.Add(pageViewBy("u1"))
	service.Drain()

	service.Add(pageViewBy("u1"))

	if got := service.Drain()[keyOf("u1", "page_view")]; got != 1 {
		t.Fatalf("count after the drain = %d, want 1", got)
	}
}

func TestDrainedCountsAreNotAffectedByLaterAdds(t *testing.T) {
	service := NewAnalysisService()
	service.Add(pageViewBy("u1"))
	drained := service.Drain()

	service.Add(pageViewBy("u1"))

	if got := drained[keyOf("u1", "page_view")]; got != 1 {
		t.Fatalf("previously drained count changed to %d, want 1", got)
	}
}

func TestRestorePutsDrainedCountsBackSoTheyCanBeDrainedAgain(t *testing.T) {
	service := NewAnalysisService()
	service.Add(pageViewBy("u1"))
	service.Add(pageViewBy("u2"))
	drained := service.Drain()

	service.Restore(drained)

	counts := service.Drain()
	if counts[keyOf("u1", "page_view")] != 1 || counts[keyOf("u2", "page_view")] != 1 {
		t.Fatalf("counts after restore = %v, want u1=1 and u2=1", counts)
	}
}

func TestRestoreAddsOntoCountsThatArrivedAfterTheDrain(t *testing.T) {
	service := NewAnalysisService()
	service.Add(pageViewBy("u1"))
	service.Add(pageViewBy("u1"))
	drained := service.Drain()
	service.Add(pageViewBy("u1"))

	service.Restore(drained)

	if got := service.Drain()[keyOf("u1", "page_view")]; got != 3 {
		t.Fatalf("count after restore = %d, want 3 (2 restored + 1 new)", got)
	}
}

func TestRestoreCreatesEntriesForKeysTheServiceHasNotSeenSinceTheDrain(t *testing.T) {
	service := NewAnalysisService()

	service.Restore(map[types.Key]int{keyOf("ghost", "click"): 4})

	if got := service.Drain()[keyOf("ghost", "click")]; got != 4 {
		t.Fatalf("restored count = %d, want 4", got)
	}
}

func TestRestoringNothingChangesNothing(t *testing.T) {
	service := NewAnalysisService()
	service.Add(pageViewBy("u1"))

	service.Restore(nil)
	service.Restore(map[types.Key]int{})

	counts := service.Drain()
	if len(counts) != 1 || counts[keyOf("u1", "page_view")] != 1 {
		t.Fatalf("counts = %v, want the single original entry", counts)
	}
}

func TestInstancesDoNotShareCounts(t *testing.T) {
	first, second := NewAnalysisService(), NewAnalysisService()

	first.Add(pageViewBy("from-first"))
	second.Add(pageViewBy("from-second"))

	if got := sum(first.Drain()); got != 1 {
		t.Fatalf("first instance drained %d, want only its own 1", got)
	}
	if got := sum(second.Drain()); got != 1 {
		t.Fatalf("second instance drained %d, want only its own 1", got)
	}
}

func TestOneInstanceDrainsTheCountsOfEveryPartitionFedIntoIt(t *testing.T) {
	shared := NewAnalysisService()
	shared.Add(pageViewBy("from-partition-0"))
	shared.Add(pageViewBy("from-partition-1"))

	if got := sum(shared.Drain()); got != 2 {
		t.Fatalf("shared instance drained %d, want both partitions' counts (2)", got)
	}
}

func TestConcurrentAddDrainAndRestoreNeverLoseOrDuplicateACount(t *testing.T) {
	const writers, activitiesPerWriter = 8, 2000
	service := NewAnalysisService()

	var writersDone, drainerDone sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersDone.Add(1)
		go func() {
			defer writersDone.Done()
			for i := 0; i < activitiesPerWriter; i++ {
				service.Add(pageViewBy("user"))
			}
		}()
	}

	stopDraining := make(chan struct{})
	var persisted int
	drainerDone.Add(1)
	go func() {
		defer drainerDone.Done()
		for round := 0; ; round++ {
			select {
			case <-stopDraining:
				return
			default:
			}
			deltas := service.Drain()
			flushFails := round%2 == 0
			if flushFails {
				service.Restore(deltas)
			} else {
				persisted += sum(deltas)
			}
		}
	}()

	writersDone.Wait()
	close(stopDraining)
	drainerDone.Wait()

	persisted += sum(service.Drain())
	if want := writers * activitiesPerWriter; persisted != want {
		t.Fatalf("accounted for %d activities, want %d", persisted, want)
	}
}
