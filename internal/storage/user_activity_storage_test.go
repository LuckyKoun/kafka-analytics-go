package storage

import (
	"context"
	"encoding/json"
	"kafka-golang-analytics/internal/types"
	"strings"
	"testing"
	"time"
)

func unreachableDatabase() DatabaseConfig {
	return DatabaseConfig{Dsn: "postgres://nobody@127.0.0.1:1/none", ConnectionTimeout: 1}
}

func TestBuildStatsEmptyHasNonNilMaps(t *testing.T) {
	stats := buildStats(nil)

	if stats.TotalUsers != 0 {
		t.Fatalf("TotalUsers = %d, want 0", stats.TotalUsers)
	}
	if stats.ActivityTotals == nil || stats.UserActivityCounts == nil {
		t.Fatal("maps must be non-nil so they encode as {} rather than null")
	}
	out, _ := json.Marshal(stats)
	if strings.Contains(string(out), "null") {
		t.Fatalf("empty stats encoded with null: %s", out)
	}
}

func TestBuildStatsAggregates(t *testing.T) {
	stats := buildStats([]userActivityStat{
		{UserID: "u1", ActivityType: "page_view", TotalActivityCount: 3},
		{UserID: "u1", ActivityType: "click", TotalActivityCount: 4},
		{UserID: "u2", ActivityType: "page_view", TotalActivityCount: 2},
		{UserID: "u3", ActivityType: "click", TotalActivityCount: 1},
	})

	if stats.TotalUsers != 3 {
		t.Errorf("TotalUsers = %d, want 3", stats.TotalUsers)
	}
	if got := stats.ActivityTotals["page_view"]; got != 5 {
		t.Errorf("page_view total = %d, want 5", got)
	}
	if got := stats.ActivityTotals["click"]; got != 5 {
		t.Errorf("click total = %d, want 5", got)
	}
	if got := stats.UserActivityCounts["u1"]["click"]; got != 4 {
		t.Errorf("u1 click = %d, want 4", got)
	}
	if got := len(stats.UserActivityCounts["u2"]); got != 1 {
		t.Errorf("u2 has %d activity types, want 1", got)
	}
}

func TestBuildStatsViewsAreConsistent(t *testing.T) {
	rows := []userActivityStat{
		{UserID: "a", ActivityType: "x", TotalActivityCount: 7},
		{UserID: "a", ActivityType: "y", TotalActivityCount: 1},
		{UserID: "b", ActivityType: "x", TotalActivityCount: 9},
	}

	stats := buildStats(rows)

	perUserSum, perActivitySum := 0, 0
	for _, byType := range stats.UserActivityCounts {
		for _, n := range byType {
			perUserSum += n
		}
	}
	for _, n := range stats.ActivityTotals {
		perActivitySum += n
	}
	if perUserSum != perActivitySum || perUserSum != 17 {
		t.Fatalf("per-user sum %d, per-activity sum %d, want both 17", perUserSum, perActivitySum)
	}
}

func TestBuildStatsCountsAUserOnceNoMatterHowManyActivityTypesTheyHave(t *testing.T) {
	stats := buildStats([]userActivityStat{
		{UserID: "busy", ActivityType: "page_view", TotalActivityCount: 1},
		{UserID: "busy", ActivityType: "click", TotalActivityCount: 1},
		{UserID: "busy", ActivityType: "scroll", TotalActivityCount: 1},
	})

	if stats.TotalUsers != 1 {
		t.Fatalf("TotalUsers = %d, want 1", stats.TotalUsers)
	}
}

func TestBuildStatsListsEveryActivityTypeOfAUserUnderThatUser(t *testing.T) {
	stats := buildStats([]userActivityStat{
		{UserID: "u1", ActivityType: "page_view", TotalActivityCount: 3},
		{UserID: "u1", ActivityType: "click", TotalActivityCount: 4},
	})

	want := map[string]int{"page_view": 3, "click": 4}
	got := stats.UserActivityCounts["u1"]
	if len(got) != len(want) || got["page_view"] != 3 || got["click"] != 4 {
		t.Fatalf("u1 counts = %v, want %v", got, want)
	}
}

func TestBuildStatsTotalsAnActivityAcrossAllUsers(t *testing.T) {
	stats := buildStats([]userActivityStat{
		{UserID: "u1", ActivityType: "click", TotalActivityCount: 10},
		{UserID: "u2", ActivityType: "click", TotalActivityCount: 20},
		{UserID: "u3", ActivityType: "click", TotalActivityCount: 30},
	})

	if got := stats.ActivityTotals["click"]; got != 60 {
		t.Fatalf("click total = %d, want 60", got)
	}
}

func TestBuildStatsDoesNotInventActivityTypesThatWereNeverStored(t *testing.T) {
	stats := buildStats([]userActivityStat{{UserID: "u1", ActivityType: "click", TotalActivityCount: 1}})

	if _, present := stats.ActivityTotals["page_view"]; present {
		t.Fatalf("ActivityTotals = %v, should only hold click", stats.ActivityTotals)
	}
}

func TestBuildStatsKeepsUsersWhoseCountIsZero(t *testing.T) {
	stats := buildStats([]userActivityStat{{UserID: "quiet", ActivityType: "click", TotalActivityCount: 0}})

	if stats.TotalUsers != 1 || stats.UserActivityCounts["quiet"]["click"] != 0 {
		t.Fatalf("stats = %+v, want the quiet user to be listed with a zero count", stats)
	}
}

func TestIncrementCountsWithoutDeltasSucceedsWithoutTouchingTheDatabase(t *testing.T) {
	storeWithoutConnection := &StatsStore{}

	for name, deltas := range map[string]map[types.Key]int{"nil deltas": nil, "empty deltas": {}} {
		t.Run(name, func(t *testing.T) {
			if err := storeWithoutConnection.IncrementCounts(context.Background(), deltas); err != nil {
				t.Fatalf("IncrementCounts returned %v, want nil", err)
			}
		})
	}
}

func TestUserActivityStatsAreStoredInTheTableTheConsumerAndTheAPIShare(t *testing.T) {
	if got := (userActivityStat{}).TableName(); got != "user_activity_stats" {
		t.Fatalf("TableName = %q, want user_activity_stats", got)
	}
}

func TestNewStatsStoreReturnsNoStoreWhenTheDatabaseIsUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := NewStatsStore(ctx, unreachableDatabase())

	if err == nil || store != nil {
		t.Fatalf("NewStatsStore returned store=%v err=%v, want no store and an error", store, err)
	}
}

func TestNewStatsStoreSaysItWasTheConnectionThatFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := NewStatsStore(ctx, unreachableDatabase())

	if err == nil || !strings.HasPrefix(err.Error(), "postgres open:") {
		t.Fatalf("error = %v, want it to start with %q", err, "postgres open:")
	}
}

func TestNewStatsStoreRejectsAMalformedConnectionString(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := NewStatsStore(ctx, DatabaseConfig{Dsn: "://not a dsn"})

	if err == nil || store != nil {
		t.Fatalf("NewStatsStore returned store=%v err=%v, want no store and an error", store, err)
	}
}
