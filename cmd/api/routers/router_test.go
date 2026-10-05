package routers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"kafka-golang-analytics/internal/types"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeStatsReader struct {
	stats types.Stats
	err   error
	block bool
}

func (f fakeStatsReader) GetStats(ctx context.Context) (types.Stats, error) {
	if f.block {
		<-ctx.Done()
		return types.Stats{}, ctx.Err()
	}
	return f.stats, f.err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func do(t *testing.T, reader StatsReader, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	Router(reader, testLogger(), time.Second).ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestStatsReturnsAggregatedResults(t *testing.T) {
	reader := fakeStatsReader{stats: types.Stats{
		TotalUsers:     2,
		ActivityTotals: map[string]int{"page_view": 5},
		UserActivityCounts: map[string]map[string]int{
			"u1": {"page_view": 3},
			"u2": {"page_view": 2},
		},
	}}

	rec := do(t, reader, http.MethodGet, "/stats")

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200; body=%s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type %q, want application/json", ct)
	}

	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("response is not json: %v", err)
	}
	want := `{"total_users":2,"activity_totals":{"page_view":5},"user_activity_counts":{"u1":{"page_view":3},"u2":{"page_view":2}}}`
	var wantMap map[string]any
	_ = json.Unmarshal([]byte(want), &wantMap)
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(wantMap)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("body %s, want %s", gotJSON, wantJSON)
	}
}

func TestStatsEmptyIsObjectsNotNull(t *testing.T) {
	reader := fakeStatsReader{stats: types.Stats{
		ActivityTotals:     map[string]int{},
		UserActivityCounts: map[string]map[string]int{},
	}}

	body := do(t, reader, http.MethodGet, "/stats").Body.String()

	if strings.Contains(body, "null") {
		t.Fatalf("empty stats must serialize as empty objects, got %s", body)
	}
	if !strings.Contains(body, `"total_users":0`) {
		t.Fatalf("missing total_users in %s", body)
	}
}

func TestStatsStorageErrorReturns500WithoutLeakingDetails(t *testing.T) {
	reader := fakeStatsReader{err: errors.New(`dial tcp 10.0.0.5:5432: password authentication failed for "analytics_chall"`)}

	rec := do(t, reader, http.MethodGet, "/stats")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "10.0.0.5") || strings.Contains(rec.Body.String(), "password") {
		t.Fatalf("response leaks the database error: %s", rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"error"`) {
		t.Fatalf("response has no error field: %s", rec.Body)
	}
}

func TestStatsQueryTimeoutReturns500(t *testing.T) {
	rec := httptest.NewRecorder()
	Router(fakeStatsReader{block: true}, testLogger(), 20*time.Millisecond).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 once the query times out", rec.Code)
	}
}

func TestStatsOnlyAllowsGet(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		if rec := do(t, fakeStatsReader{}, method, "/stats"); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /stats = %d, want 405", method, rec.Code)
		}
	}
}

func TestUnknownPathIs404(t *testing.T) {
	if rec := do(t, fakeStatsReader{}, http.MethodGet, "/nope"); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /nope = %d, want 404", rec.Code)
	}
}
