package routers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"kafka-golang-analytics/internal/types"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const validActivityBody = `{
	"user_id": "user-7",
	"activity_type": "page_view",
	"timestamp": "2026-03-01T10:00:00Z",
	"metadata": {"page_url": "/pricing", "referrer": "newsletter"}
}`

type contextKey string

type RecordProducerMock struct {
	records  []*kgo.Record
	contexts []context.Context

	completesDelivery bool
	assignedPartition int32
	assignedOffset    int64
	deliveryError     error
}

func (f *RecordProducerMock) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	f.contexts = append(f.contexts, ctx)
	f.records = append(f.records, record)
	if f.completesDelivery {
		record.Partition = f.assignedPartition
		record.Offset = f.assignedOffset
		promise(record, f.deliveryError)
	}
}

type publishOutcome struct {
	response *httptest.ResponseRecorder
	logs     string
}

func publishWith(producer RecordProducer, body string) publishOutcome {
	return publishWithContexts(context.Background(), context.Background(), producer, body)
}

func publishWithContexts(producerContext, requestContext context.Context, producer RecordProducer, body string) publishOutcome {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/user_activities", strings.NewReader(body)).WithContext(requestContext)

	Producer(producerContext, producer, logger).ServeHTTP(response, request)

	return publishOutcome{response: response, logs: logs.String()}
}

func errorMessageOf(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not json: %q", response.Body.String())
	}
	return body["error"]
}

func TestPublishAnswersAcceptedForAValidActivity(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, validActivityBody)

	if outcome.response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusAccepted)
	}
}

func TestPublishConfirmsAcceptanceWithAnOkStatusBody(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, validActivityBody)

	var body map[string]string
	_ = json.Unmarshal(outcome.response.Body.Bytes(), &body)
	if body["status"] != "ok" {
		t.Fatalf("body = %q, want status ok", outcome.response.Body.String())
	}
}

func TestPublishSendsExactlyOneRecordPerRequest(t *testing.T) {
	producer := &RecordProducerMock{}

	publishWith(producer, validActivityBody)

	if len(producer.records) != 1 {
		t.Fatalf("produced %d records, want 1", len(producer.records))
	}
}

func TestPublishUsesTheUserIDAsTheRecordKeySoAUsersActivitiesShareAPartition(t *testing.T) {
	producer := &RecordProducerMock{}

	publishWith(producer, validActivityBody)

	if got := string(producer.records[0].Key); got != "user-7" {
		t.Fatalf("record key = %q, want user-7", got)
	}
}

func TestPublishSendsTheActivityAsTheJSONRecordValue(t *testing.T) {
	producer := &RecordProducerMock{}

	publishWith(producer, validActivityBody)

	var sent types.UserActivity
	if err := json.Unmarshal(producer.records[0].Value, &sent); err != nil {
		t.Fatalf("record value is not a user activity: %q", producer.records[0].Value)
	}
	if sent.UserID != "user-7" || sent.ActivityType != types.ActivityPageView {
		t.Fatalf("sent %+v, want user-7 / page_view", sent)
	}
	if sent.Metadata.PageUrl != "/pricing" || sent.Metadata.Referrer != "newsletter" {
		t.Fatalf("metadata lost on the way: %+v", sent.Metadata)
	}
	if !sent.Timestamp.Equal(time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("timestamp lost on the way: %v", sent.Timestamp)
	}
}

func TestPublishAcceptsPayloadsThatCarryFieldsOutsideTheActivitySchema(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, `{"user_id":"user-7","activity_type":"page_view","password":"hunter2","extra":{"a":1}}`)

	if outcome.response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusAccepted)
	}
}

func TestPublishIgnoresFieldsOutsideTheActivitySchema(t *testing.T) {
	producer := &RecordProducerMock{}

	publishWith(producer, `{"user_id":"user-7","activity_type":"page_view","password":"hunter2","extra":{"a":1}}`)

	value := string(producer.records[0].Value)
	if strings.Contains(value, "hunter2") || strings.Contains(value, "extra") {
		t.Fatalf("a field outside the schema reached kafka: %s", value)
	}
}

func TestPublishStillSendsTheSchemaFieldsWhenExtraFieldsArePresent(t *testing.T) {
	producer := &RecordProducerMock{}

	publishWith(producer, `{"user_id":"user-7","activity_type":"page_view","extra":true}`)

	var sent types.UserActivity
	_ = json.Unmarshal(producer.records[0].Value, &sent)
	if sent.UserID != "user-7" || sent.ActivityType != types.ActivityPageView {
		t.Fatalf("sent %+v, want user-7 / page_view", sent)
	}
}

func TestPublishRejectsAPayloadWithoutAUserWithBadRequest(t *testing.T) {
	bodies := map[string]string{
		"an empty object":                  `{}`,
		"other fields but no user_id":      `{"activity_type":"page_view"}`,
		"an empty user_id":                 `{"user_id":"","activity_type":"page_view"}`,
		"a whitespace only user_id":        `{"user_id":"   ","activity_type":"page_view"}`,
		"a null user_id":                   `{"user_id":null,"activity_type":"page_view"}`,
		"a user_id made of tabs and lines": "{\"user_id\":\"\\t\\n\",\"activity_type\":\"page_view\"}",
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			producer := &RecordProducerMock{}

			outcome := publishWith(producer, body)

			if outcome.response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusBadRequest)
			}
			if len(producer.records) != 0 {
				t.Fatalf("produced %d records for a payload without a user", len(producer.records))
			}
		})
	}
}

func TestPublishSaysTheUserIDIsRequiredWhenItIsMissing(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, `{"activity_type":"page_view"}`)

	got := errorMessageOf(t, outcome.response)
	if !strings.HasPrefix(got, "invalid user activity body payload") || !strings.Contains(got, "user_id is required") {
		t.Fatalf("error = %q, want the invalid payload message explaining that user_id is required", got)
	}
}

func TestPublishLogsWhyAPayloadWithoutAUserWasRejected(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, `{"activity_type":"page_view"}`)

	if !strings.Contains(outcome.logs, "user_id is required") {
		t.Fatalf("expected the rejection reason in the logs, got %q", outcome.logs)
	}
}

func TestPublishAcceptsAPayloadWithAUserEvenWhenTheActivityTypeIsMissing(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, `{"user_id":"user-7"}`)

	if outcome.response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusAccepted)
	}
}

func TestPublishRejectsAMalformedJSONBodyWithBadRequest(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, `{"user_id": "user-7",`)

	if outcome.response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusBadRequest)
	}
}

func TestPublishRejectsAnEmptyBodyWithBadRequest(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, "")

	if outcome.response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusBadRequest)
	}
}

func TestPublishRejectsFieldsOfTheWrongTypeWithBadRequest(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, `{"user_id": 42, "activity_type": "page_view"}`)

	if outcome.response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusBadRequest)
	}
}

func TestPublishRejectsAMalformedTimestampWithBadRequest(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, `{"user_id":"u1","activity_type":"page_view","timestamp":"yesterday"}`)

	if outcome.response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusBadRequest)
	}
}

func TestPublishExplainsWhyABodyWasRejected(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, "not json")

	if got := errorMessageOf(t, outcome.response); !strings.HasPrefix(got, "invalid user activity body payload") {
		t.Fatalf("error = %q, want it to start with the invalid payload message", got)
	}
}

func TestPublishLogsWhyABodyWasRejected(t *testing.T) {
	outcome := publishWith(&RecordProducerMock{}, "not json")

	if !strings.Contains(outcome.logs, "invalid user activity body payload") {
		t.Fatalf("expected the rejection in the logs, got %q", outcome.logs)
	}
}

func TestPublishSendsNothingToKafkaWhenTheBodyIsRejected(t *testing.T) {
	bodies := map[string]string{
		"malformed json":    `{"user_id":`,
		"empty body":        "",
		"wrong field types": `{"user_id": 42}`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			producer := &RecordProducerMock{}

			publishWith(producer, body)

			if len(producer.records) != 0 {
				t.Fatalf("produced %d records for a rejected body", len(producer.records))
			}
		})
	}
}

func TestPublishAnswersAcceptedEvenWhenKafkaDeliveryLaterFails(t *testing.T) {
	producer := &RecordProducerMock{completesDelivery: true, deliveryError: errors.New("broker unavailable")}

	outcome := publishWith(producer, validActivityBody)

	if outcome.response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", outcome.response.Code, http.StatusAccepted)
	}
}

func TestPublishLogsTheUserAndTheCauseWhenKafkaDeliveryFails(t *testing.T) {
	producer := &RecordProducerMock{completesDelivery: true, deliveryError: errors.New("broker unavailable")}

	outcome := publishWith(producer, validActivityBody)

	for _, expected := range []string{"failed to publish into kafka", "broker unavailable", "user-7"} {
		if !strings.Contains(outcome.logs, expected) {
			t.Errorf("logs %q do not mention %q", outcome.logs, expected)
		}
	}
}

func TestPublishDoesNotReportSuccessWhenKafkaDeliveryFails(t *testing.T) {
	producer := &RecordProducerMock{completesDelivery: true, deliveryError: errors.New("broker unavailable")}

	outcome := publishWith(producer, validActivityBody)

	if strings.Contains(outcome.logs, "published activity into kafka") {
		t.Fatalf("a failed delivery was logged as published: %q", outcome.logs)
	}
}

func TestPublishLogsThePartitionAndOffsetAssignedToADeliveredRecord(t *testing.T) {
	producer := &RecordProducerMock{completesDelivery: true, assignedPartition: 3, assignedOffset: 42}

	outcome := publishWith(producer, validActivityBody)

	for _, expected := range []string{"published activity into kafka", `"partition":3`, `"offset":42`, `"activity_type":"page_view"`} {
		if !strings.Contains(outcome.logs, expected) {
			t.Errorf("logs %q do not mention %q", outcome.logs, expected)
		}
	}
}

func TestPublishProducesWithTheServerContextRatherThanTheRequestContext(t *testing.T) {
	producer := &RecordProducerMock{}
	serverContext := context.WithValue(context.Background(), contextKey("owner"), "server")

	publishWithContexts(serverContext, context.Background(), producer, validActivityBody)

	if got := producer.contexts[0].Value(contextKey("owner")); got != "server" {
		t.Fatalf("producer received a context without the server value: %v", got)
	}
}

func TestPublishKeepsProducingWhenTheClientHasAlreadyDisconnected(t *testing.T) {
	producer := &RecordProducerMock{}
	disconnectedClient, disconnect := context.WithCancel(context.Background())
	disconnect()

	publishWithContexts(context.Background(), disconnectedClient, producer, validActivityBody)

	if err := producer.contexts[0].Err(); err != nil {
		t.Fatalf("a client disconnect cancelled the produce context: %v", err)
	}
}
