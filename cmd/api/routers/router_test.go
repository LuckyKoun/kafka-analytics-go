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

type pageRequest struct {
	page     int
	pageSize int
}

type StatsReaderMock struct {
	stats    types.Stats
	err      error
	block    bool
	requests []pageRequest
}

func (m *StatsReaderMock) GetStats(ctx context.Context, page, pageSize int) (types.Stats, error) {
	m.requests = append(m.requests, pageRequest{page: page, pageSize: pageSize})
	if m.block {
		<-ctx.Done()
		return types.Stats{}, ctx.Err()
	}
	return m.stats, m.err
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

func getStats(t *testing.T, reader StatsReader, query string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, reader, http.MethodGet, "/stats"+query)
}

func decodedObject(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not a json object: %q (%v)", rec.Body.String(), err)
	}
	return body
}

func TestStatsReturnsAggregatedResults(t *testing.T) {
	reader := &StatsReaderMock{stats: types.Stats{
		TotalUsers:     2,
		ActivityTotals: map[string]int{"page_view": 5},
		UserActivityCounts: map[string]map[string]int{
			"u1": {"page_view": 3},
			"u2": {"page_view": 2},
		},
		Pagination: types.Pagination{Page: 1, PageSize: 50, TotalPages: 1},
	}}

	rec := getStats(t, reader, "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200; body=%s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type %q, want application/json", ct)
	}

	want := `{"total_users":2,"activity_totals":{"page_view":5},"user_activity_counts":{"u1":{"page_view":3},"u2":{"page_view":2}},"pagination":{"page":1,"page_size":50,"total_pages":1}}`
	var wantObject map[string]any
	_ = json.Unmarshal([]byte(want), &wantObject)
	gotJSON, _ := json.Marshal(decodedObject(t, rec))
	wantJSON, _ := json.Marshal(wantObject)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("body %s, want %s", gotJSON, wantJSON)
	}
}

func TestStatsEmptyIsObjectsNotNull(t *testing.T) {
	reader := &StatsReaderMock{stats: types.Stats{
		ActivityTotals:     map[string]int{},
		UserActivityCounts: map[string]map[string]int{},
	}}

	body := getStats(t, reader, "").Body.String()

	if strings.Contains(body, "null") {
		t.Fatalf("empty stats must serialize as empty objects, got %s", body)
	}
	if !strings.Contains(body, `"total_users":0`) {
		t.Fatalf("missing total_users in %s", body)
	}
}

func TestStatsStorageErrorReturns500WithoutLeakingDetails(t *testing.T) {
	reader := &StatsReaderMock{err: errors.New(`dial tcp 10.0.0.5:5432: password authentication failed for "analytics_chall"`)}

	rec := getStats(t, reader, "")

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
	Router(&StatsReaderMock{block: true}, testLogger(), 20*time.Millisecond).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 once the query times out", rec.Code)
	}
}

func TestStatsOnlyAllowsGet(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		if rec := do(t, &StatsReaderMock{}, method, "/stats"); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /stats = %d, want 405", method, rec.Code)
		}
	}
}

func TestUnknownPathIs404(t *testing.T) {
	if rec := do(t, &StatsReaderMock{}, http.MethodGet, "/nope"); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /nope = %d, want 404", rec.Code)
	}
}

func TestStatsAsksForTheFirstPageOfFiftyUsersWhenNoPaginationIsRequested(t *testing.T) {
	reader := &StatsReaderMock{}

	getStats(t, reader, "")

	if len(reader.requests) != 1 || reader.requests[0] != (pageRequest{page: 1, pageSize: 50}) {
		t.Fatalf("store asked for %+v, want page 1 with 50 users", reader.requests)
	}
}

func TestStatsPassesTheRequestedPageAndPageSizeToTheStore(t *testing.T) {
	reader := &StatsReaderMock{}

	getStats(t, reader, "?page=3&page_size=20")

	if len(reader.requests) != 1 || reader.requests[0] != (pageRequest{page: 3, pageSize: 20}) {
		t.Fatalf("store asked for %+v, want page 3 with 20 users", reader.requests)
	}
}

func TestStatsKeepsTheDefaultPageSizeWhenOnlyThePageIsRequested(t *testing.T) {
	reader := &StatsReaderMock{}

	getStats(t, reader, "?page=4")

	if reader.requests[0] != (pageRequest{page: 4, pageSize: 50}) {
		t.Fatalf("store asked for %+v, want page 4 with the default 50 users", reader.requests[0])
	}
}

func TestStatsStartsAtTheFirstPageWhenOnlyThePageSizeIsRequested(t *testing.T) {
	reader := &StatsReaderMock{}

	getStats(t, reader, "?page_size=10")

	if reader.requests[0] != (pageRequest{page: 1, pageSize: 10}) {
		t.Fatalf("store asked for %+v, want page 1 with 10 users", reader.requests[0])
	}
}

func TestStatsAcceptsThePageSizeAtItsUpperLimit(t *testing.T) {
	reader := &StatsReaderMock{}

	rec := getStats(t, reader, "?page_size=100")

	if rec.Code != http.StatusOK || reader.requests[0].pageSize != 100 {
		t.Fatalf("status %d, asked for %+v; want 200 with 100 users", rec.Code, reader.requests)
	}
}

func TestStatsAcceptsASinglePageSize(t *testing.T) {
	reader := &StatsReaderMock{}

	rec := getStats(t, reader, "?page_size=1")

	if rec.Code != http.StatusOK || reader.requests[0].pageSize != 1 {
		t.Fatalf("status %d, asked for %+v; want 200 with 1 user", rec.Code, reader.requests)
	}
}

func TestStatsIgnoresQueryParametersItDoesNotKnow(t *testing.T) {
	reader := &StatsReaderMock{}

	rec := getStats(t, reader, "?sort=desc&page=2")

	if rec.Code != http.StatusOK || reader.requests[0] != (pageRequest{page: 2, pageSize: 50}) {
		t.Fatalf("status %d, asked for %+v; want 200 for page 2", rec.Code, reader.requests)
	}
}

func TestStatsDescribesThePaginationReportedByTheStore(t *testing.T) {
	reader := &StatsReaderMock{stats: types.Stats{Pagination: types.Pagination{Page: 2, PageSize: 20, TotalPages: 5}}}

	rec := getStats(t, reader, "?page=2&page_size=20")

	pagination, ok := decodedObject(t, rec)["pagination"].(map[string]any)
	if !ok {
		t.Fatalf("response has no pagination object: %s", rec.Body)
	}
	if pagination["page"] != float64(2) || pagination["page_size"] != float64(20) || pagination["total_pages"] != float64(5) {
		t.Fatalf("pagination = %v, want page 2, page_size 20, total_pages 5", pagination)
	}
}

func TestStatsRejectsInvalidPaginationParametersWithBadRequest(t *testing.T) {
	queries := map[string]string{
		"a zero page":                   "?page=0",
		"a negative page":               "?page=-1",
		"a page that is not a number":   "?page=abc",
		"a fractional page":             "?page=1.5",
		"an empty page":                 "?page=",
		"a page beyond the supported":   "?page=1000001",
		"a zero page size":              "?page_size=0",
		"a negative page size":          "?page_size=-5",
		"a page size that is no number": "?page_size=many",
		"a fractional page size":        "?page_size=2.5",
		"an empty page size":            "?page_size=",
		"a page size above the limit":   "?page_size=101",
		"a valid page with a bad size":  "?page=2&page_size=0",
		"a bad page with a valid size":  "?page=0&page_size=10",
	}

	for name, query := range queries {
		t.Run(name, func(t *testing.T) {
			reader := &StatsReaderMock{}

			rec := getStats(t, reader, query)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("GET /stats%s = %d, want 400", query, rec.Code)
			}
			if len(reader.requests) != 0 {
				t.Fatalf("the database was queried %d times for an invalid request", len(reader.requests))
			}
		})
	}
}

func TestStatsSaysWhichParameterIsInvalid(t *testing.T) {
	cases := map[string]string{
		"?page=0":       "page must be",
		"?page_size=0":  "page_size must be",
		"?page_size=-1": "page_size must be",
	}

	for query, expected := range cases {
		t.Run(query, func(t *testing.T) {
			rec := getStats(t, &StatsReaderMock{}, query)

			message, _ := decodedObject(t, rec)["error"].(string)
			if !strings.HasPrefix(message, "invalid pagination parameters") || !strings.Contains(message, expected) {
				t.Fatalf("error = %q, want it to explain that %q", message, expected)
			}
		})
	}
}

func TestStatsStatesTheAllowedRangeWhenPageSizeIsTooLarge(t *testing.T) {
	rec := getStats(t, &StatsReaderMock{}, "?page_size=101")

	message, _ := decodedObject(t, rec)["error"].(string)
	if !strings.Contains(message, "between 1 and 100") {
		t.Fatalf("error = %q, want it to state the 1 to 100 range", message)
	}
}

func TestParsePaginationDefaultsToTheFirstPageOfFifty(t *testing.T) {
	page, pageSize, err := parsePagination(map[string][]string{})

	if err != nil || page != 1 || pageSize != 50 {
		t.Fatalf("parsePagination = (%d, %d, %v), want (1, 50, nil)", page, pageSize, err)
	}
}

func TestParsePaginationAcceptsTheBoundariesOfBothParameters(t *testing.T) {
	cases := []struct {
		name         string
		query        map[string][]string
		wantedPage   int
		wantedSize   int
		wantedFailed bool
	}{
		{name: "smallest values", query: map[string][]string{"page": {"1"}, "page_size": {"1"}}, wantedPage: 1, wantedSize: 1},
		{name: "largest values", query: map[string][]string{"page": {"1000000"}, "page_size": {"100"}}, wantedPage: 1000000, wantedSize: 100},
		{name: "one past the largest page", query: map[string][]string{"page": {"1000001"}}, wantedFailed: true},
		{name: "one past the largest page size", query: map[string][]string{"page_size": {"101"}}, wantedFailed: true},
		{name: "one below the smallest page", query: map[string][]string{"page": {"0"}}, wantedFailed: true},
		{name: "one below the smallest page size", query: map[string][]string{"page_size": {"0"}}, wantedFailed: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			page, pageSize, err := parsePagination(tc.query)

			if tc.wantedFailed {
				if err == nil {
					t.Fatalf("expected an error, got page=%d page_size=%d", page, pageSize)
				}
				return
			}
			if err != nil || page != tc.wantedPage || pageSize != tc.wantedSize {
				t.Fatalf("parsePagination = (%d, %d, %v), want (%d, %d, nil)", page, pageSize, err, tc.wantedPage, tc.wantedSize)
			}
		})
	}
}
