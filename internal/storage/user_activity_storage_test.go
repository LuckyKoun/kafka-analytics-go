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
	stats := buildStats(0, nil, nil, 1, 50)

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

func TestBuildStatsReportsTheGlobalUserCountEvenWhenOnlyAPageOfUsersIsListed(t *testing.T) {
	pageRows := []userActivityStat{
		{UserID: "u1", ActivityType: "page_view", TotalActivityCount: 1},
		{UserID: "u2", ActivityType: "page_view", TotalActivityCount: 1},
	}

	stats := buildStats(120, nil, pageRows, 1, 2)

	if stats.TotalUsers != 120 || len(stats.UserActivityCounts) != 2 {
		t.Fatalf("TotalUsers=%d with %d users listed, want 120 with 2", stats.TotalUsers, len(stats.UserActivityCounts))
	}
}

func TestBuildStatsListsOnlyTheUsersItWasGiven(t *testing.T) {
	pageRows := []userActivityStat{{UserID: "u7", ActivityType: "page_view", TotalActivityCount: 4}}

	stats := buildStats(10, nil, pageRows, 2, 1)

	if _, listed := stats.UserActivityCounts["u7"]; !listed || len(stats.UserActivityCounts) != 1 {
		t.Fatalf("UserActivityCounts = %v, want only u7", stats.UserActivityCounts)
	}
}

func TestBuildStatsListsEveryActivityTypeOfAUserUnderThatUser(t *testing.T) {
	pageRows := []userActivityStat{
		{UserID: "u1", ActivityType: "page_view", TotalActivityCount: 3},
		{UserID: "u1", ActivityType: "click", TotalActivityCount: 4},
	}

	stats := buildStats(1, nil, pageRows, 1, 50)

	got := stats.UserActivityCounts["u1"]
	if len(got) != 2 || got["page_view"] != 3 || got["click"] != 4 {
		t.Fatalf("u1 counts = %v, want page_view=3 and click=4", got)
	}
}

func TestBuildStatsTakesActivityTotalsFromTheGlobalTotalsNotFromThePageRows(t *testing.T) {
	totals := []activityTotal{{ActivityType: "click", Total: 1000}, {ActivityType: "page_view", Total: 5000}}
	pageRows := []userActivityStat{{UserID: "u1", ActivityType: "click", TotalActivityCount: 3}}

	stats := buildStats(40, totals, pageRows, 1, 1)

	if stats.ActivityTotals["click"] != 1000 || stats.ActivityTotals["page_view"] != 5000 {
		t.Fatalf("ActivityTotals = %v, want the global click=1000 and page_view=5000", stats.ActivityTotals)
	}
}

func TestBuildStatsDoesNotInventActivityTypesThatWereNeverStored(t *testing.T) {
	stats := buildStats(1, []activityTotal{{ActivityType: "click", Total: 1}}, nil, 1, 50)

	if _, present := stats.ActivityTotals["page_view"]; present || len(stats.ActivityTotals) != 1 {
		t.Fatalf("ActivityTotals = %v, should only hold click", stats.ActivityTotals)
	}
}

func TestBuildStatsKeepsUsersWhoseCountIsZero(t *testing.T) {
	pageRows := []userActivityStat{{UserID: "quiet", ActivityType: "click", TotalActivityCount: 0}}

	stats := buildStats(1, nil, pageRows, 1, 50)

	if stats.UserActivityCounts["quiet"]["click"] != 0 || len(stats.UserActivityCounts) != 1 {
		t.Fatalf("stats = %+v, want the quiet user to be listed with a zero count", stats)
	}
}

func TestBuildStatsEchoesThePageAndPageSizeItWasAskedFor(t *testing.T) {
	stats := buildStats(120, nil, nil, 3, 20)

	if stats.Pagination.Page != 3 || stats.Pagination.PageSize != 20 {
		t.Fatalf("Pagination = %+v, want page 3 with page size 20", stats.Pagination)
	}
}

func TestBuildStatsComputesTheTotalPagesFromAllUsersNotJustTheListedOnes(t *testing.T) {
	pageRows := []userActivityStat{{UserID: "u1", ActivityType: "click", TotalActivityCount: 1}}

	stats := buildStats(120, nil, pageRows, 1, 50)

	if stats.Pagination.TotalPages != 3 {
		t.Fatalf("TotalPages = %d, want 3 for 120 users at 50 per page", stats.Pagination.TotalPages)
	}
}

func TestTotalPagesRoundsUpToCoverEveryUser(t *testing.T) {
	cases := []struct {
		name       string
		totalUsers int
		pageSize   int
		want       int
	}{
		{"no users need no pages", 0, 50, 0},
		{"one user needs one page", 1, 50, 1},
		{"exactly one full page", 50, 50, 1},
		{"one user more than a full page", 51, 50, 2},
		{"exactly two full pages", 100, 50, 2},
		{"one user more than two full pages", 101, 50, 3},
		{"one user per page", 7, 1, 7},
		{"a page size larger than the user count", 3, 100, 1},
		{"a zero page size has no pages", 10, 0, 0},
		{"a negative page size has no pages", 10, -5, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := totalPages(tc.totalUsers, tc.pageSize); got != tc.want {
				t.Fatalf("totalPages(%d, %d) = %d, want %d", tc.totalUsers, tc.pageSize, got, tc.want)
			}
		})
	}
}

func TestGetStatsRejectsANonPositivePageOrPageSizeWithoutQueryingTheDatabase(t *testing.T) {
	storeWithoutConnection := &StatsStore{}
	cases := map[string][2]int{
		"page zero":           {0, 50},
		"a negative page":     {-1, 50},
		"page size zero":      {1, 0},
		"a negative pagesize": {1, -10},
	}

	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := storeWithoutConnection.GetStats(context.Background(), args[0], args[1])

			if err == nil {
				t.Fatalf("GetStats(page=%d, page_size=%d) succeeded, want an error", args[0], args[1])
			}
		})
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
