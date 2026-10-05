package storage

import (
	"encoding/json"
	"strings"
	"testing"
)

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
