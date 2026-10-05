package types

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"
)

func jsonKeysOf(t *testing.T, value any) []string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatalf("not a json object: %s", encoded)
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestPageViewActivityTypeUsesTheValueProducersSend(t *testing.T) {
	if ActivityPageView != "page_view" {
		t.Fatalf("ActivityPageView = %q, want page_view", ActivityPageView)
	}
}

func TestUserActivityDecodesEveryFieldOfTheProducerPayload(t *testing.T) {
	payload := `{
		"user_id": "user-7",
		"activity_type": "page_view",
		"timestamp": "2026-03-01T10:00:00Z",
		"metadata": {"page_url": "/pricing", "referrer": "newsletter"}
	}`

	var activity UserActivity
	if err := json.Unmarshal([]byte(payload), &activity); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	want := UserActivity{
		UserID:       "user-7",
		ActivityType: ActivityPageView,
		Timestamp:    time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC),
		Metadata:     Metadata{PageUrl: "/pricing", Referrer: "newsletter"},
	}
	if !reflect.DeepEqual(activity, want) {
		t.Fatalf("decoded %+v, want %+v", activity, want)
	}
}

func TestUserActivityDecodesWithoutMetadataWhenTheProducerOmitsIt(t *testing.T) {
	var activity UserActivity
	err := json.Unmarshal([]byte(`{"user_id":"user-7","activity_type":"page_view"}`), &activity)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if activity.Metadata != (Metadata{}) {
		t.Fatalf("Metadata = %+v, want the zero value", activity.Metadata)
	}
}

func TestUserActivityDecodesWithoutTimestampWhenTheProducerOmitsIt(t *testing.T) {
	var activity UserActivity
	err := json.Unmarshal([]byte(`{"user_id":"user-7","activity_type":"page_view"}`), &activity)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !activity.Timestamp.IsZero() {
		t.Fatalf("Timestamp = %v, want the zero time", activity.Timestamp)
	}
}

func TestUserActivityRejectsAUserIDThatIsNotAString(t *testing.T) {
	var activity UserActivity

	err := json.Unmarshal([]byte(`{"user_id": 42, "activity_type": "page_view"}`), &activity)

	if err == nil {
		t.Fatal("expected an error for a numeric user_id")
	}
}

func TestUserActivityRejectsATimestampThatIsNotRFC3339(t *testing.T) {
	var activity UserActivity

	err := json.Unmarshal([]byte(`{"user_id":"u1","activity_type":"page_view","timestamp":"yesterday"}`), &activity)

	if err == nil {
		t.Fatal("expected an error for a malformed timestamp")
	}
}

func TestUserActivitySurvivesAJSONRoundTripWithoutChanges(t *testing.T) {
	original := UserActivity{
		UserID:       "user-9",
		ActivityType: ActivityPageView,
		Timestamp:    time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC),
		Metadata:     Metadata{PageUrl: "/docs", Referrer: "search"},
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var decoded UserActivity
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf("round trip produced %+v, want %+v", decoded, original)
	}
}

func TestMetadataOmitsFieldsThatAreEmpty(t *testing.T) {
	keys := jsonKeysOf(t, Metadata{PageUrl: "/docs"})

	if !reflect.DeepEqual(keys, []string{"page_url"}) {
		t.Fatalf("encoded keys = %v, want only page_url", keys)
	}
}

func TestUserActivityEncodesUsingSnakeCaseKeys(t *testing.T) {
	keys := jsonKeysOf(t, UserActivity{UserID: "u1", ActivityType: ActivityPageView, Metadata: Metadata{PageUrl: "/"}})

	for _, want := range []string{"user_id", "activity_type", "metadata"} {
		found := false
		for _, key := range keys {
			found = found || key == want
		}
		if !found {
			t.Errorf("encoded keys %v are missing %q", keys, want)
		}
	}
}

func TestKeysWithTheSameUserButDifferentActivitiesAreDistinctMapEntries(t *testing.T) {
	counts := map[Key]int{}

	counts[Key{UserID: "u1", ActivityType: "page_view"}]++
	counts[Key{UserID: "u1", ActivityType: "click"}]++

	if len(counts) != 2 {
		t.Fatalf("expected 2 entries, got %d: %v", len(counts), counts)
	}
}

func TestKeysWithTheSameUserAndActivityShareOneMapEntry(t *testing.T) {
	counts := map[Key]int{}

	counts[Key{UserID: "u1", ActivityType: "page_view"}]++
	counts[Key{UserID: "u1", ActivityType: "page_view"}]++

	if got := counts[Key{UserID: "u1", ActivityType: "page_view"}]; got != 2 || len(counts) != 1 {
		t.Fatalf("expected a single entry with count 2, got %v", counts)
	}
}

func TestStatsEncodesExactlyTheThreeDocumentedSections(t *testing.T) {
	keys := jsonKeysOf(t, Stats{})

	want := []string{"activity_totals", "total_users", "user_activity_counts"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("encoded keys = %v, want %v", keys, want)
	}
}

func TestStatsReportsZeroUsersExplicitlyInsteadOfOmittingTheField(t *testing.T) {
	encoded, err := json.Marshal(Stats{TotalUsers: 0})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var object map[string]any
	_ = json.Unmarshal(encoded, &object)
	if got, present := object["total_users"]; !present || got != float64(0) {
		t.Fatalf("total_users = %v (present=%v), want an explicit 0", got, present)
	}
}

func TestStatsKeepsNestedUserCountsUnderEachUserID(t *testing.T) {
	stats := Stats{
		TotalUsers:         1,
		ActivityTotals:     map[string]int{"page_view": 3},
		UserActivityCounts: map[string]map[string]int{"u1": {"page_view": 3}},
	}

	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	want := `{"total_users":1,"activity_totals":{"page_view":3},"user_activity_counts":{"u1":{"page_view":3}}}`
	if string(encoded) != want {
		t.Fatalf("encoded %s, want %s", encoded, want)
	}
}
