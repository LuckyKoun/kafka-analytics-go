package routers

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func requestTo(method, path string) (int, *RecordProducerMock) {
	producer := &RecordProducerMock{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	response := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(validActivityBody))

	Producer(context.Background(), producer, logger).ServeHTTP(response, request)

	return response.Code, producer
}

func TestProducerAcceptsPostRequestsOnUserActivities(t *testing.T) {
	status, producer := requestTo(http.MethodPost, "/user_activities")

	if status != http.StatusAccepted || len(producer.records) != 1 {
		t.Fatalf("status %d with %d records, want %d with 1", status, len(producer.records), http.StatusAccepted)
	}
}

func TestProducerRejectsEveryOtherMethodOnUserActivitiesWithMethodNotAllowed(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			status, producer := requestTo(method, "/user_activities")

			if status != http.StatusMethodNotAllowed {
				t.Fatalf("%s /user_activities = %d, want %d", method, status, http.StatusMethodNotAllowed)
			}
			if len(producer.records) != 0 {
				t.Fatalf("%s produced %d records", method, len(producer.records))
			}
		})
	}
}

func TestProducerAnswersNotFoundForPathsItDoesNotServe(t *testing.T) {
	for _, path := range []string{"/", "/user_activities/", "/user_activity", "/stats"} {
		t.Run(path, func(t *testing.T) {
			status, producer := requestTo(http.MethodPost, path)

			if status != http.StatusNotFound {
				t.Fatalf("POST %s = %d, want %d", path, status, http.StatusNotFound)
			}
			if len(producer.records) != 0 {
				t.Fatalf("POST %s produced %d records", path, len(producer.records))
			}
		})
	}
}
