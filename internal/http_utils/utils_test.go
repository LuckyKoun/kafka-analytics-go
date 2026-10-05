package httputils

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func loggerWithCapturedOutput() (*slog.Logger, *bytes.Buffer) {
	var output bytes.Buffer
	return slog.New(slog.NewJSONHandler(&output, nil)), &output
}

type FailingResponseWriterMock struct {
	header http.Header
	status int
}

func newFailingResponseWriterMock() *FailingResponseWriterMock {
	return &FailingResponseWriterMock{header: http.Header{}}
}

func (w *FailingResponseWriterMock) Header() http.Header { return w.header }

func (w *FailingResponseWriterMock) WriteHeader(status int) { w.status = status }

func (w *FailingResponseWriterMock) Write([]byte) (int, error) {
	return 0, errors.New("connection reset by peer")
}

func decodedBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not a json object: %q (%v)", recorder.Body.String(), err)
	}
	return body
}

func TestFormatErrorJoinsTheMessageAndTheCauseWhenThereIsAnError(t *testing.T) {
	got := FormatError("could not save activity", errors.New("disk is full"))

	want := map[string]string{"error": "could not save activity: disk is full"}
	if len(got) != 1 || got["error"] != want["error"] {
		t.Fatalf("FormatError = %v, want %v", got, want)
	}
}

func TestFormatErrorKeepsOnlyTheMessageWhenTheErrorIsNil(t *testing.T) {
	got := FormatError("could not save activity", nil)

	want := map[string]string{"error": "could not save activity"}
	if len(got) != 1 || got["error"] != want["error"] {
		t.Fatalf("FormatError = %v, want %v", got, want)
	}
}

func TestFormatErrorNeverLeaksTheCauseWhenTheErrorIsNil(t *testing.T) {
	got := FormatError("failed to read stats", nil)

	if strings.Contains(got["error"], ":") {
		t.Fatalf("message %q should not carry a cause", got["error"])
	}
}

func TestWriteResponseUsesTheGivenStatusCode(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()

	WriteResponse(recorder, http.StatusAccepted, map[string]string{"status": "ok"}, logger)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
}

func TestWriteResponseDeclaresAJSONContentType(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()

	WriteResponse(recorder, http.StatusOK, map[string]string{"status": "ok"}, logger)

	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestWriteResponseEncodesTheBodyAsJSON(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()

	WriteResponse(recorder, http.StatusOK, map[string]string{"status": "ok"}, logger)

	if got := decodedBody(t, recorder)["status"]; got != "ok" {
		t.Fatalf("body status = %v, want ok", got)
	}
}

func TestWriteResponseHonoursTheJSONTagsOfStructBodies(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()
	body := struct {
		TotalUsers int `json:"total_users"`
	}{TotalUsers: 12}

	WriteResponse(recorder, http.StatusOK, body, logger)

	if got := decodedBody(t, recorder)["total_users"]; got != float64(12) {
		t.Fatalf("total_users = %v, want 12", got)
	}
}

func TestWriteResponseLogsAnErrorWhenTheBodyCannotBeEncoded(t *testing.T) {
	logger, logs := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()

	WriteResponse(recorder, http.StatusOK, make(chan int), logger)

	if !strings.Contains(logs.String(), "error encoding body to json") {
		t.Fatalf("expected an encoding error in the logs, got %q", logs.String())
	}
}

func TestWriteResponseKeepsTheGivenStatusWhenTheBodyCannotBeEncoded(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()

	WriteResponse(recorder, http.StatusCreated, make(chan int), logger)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
}

func TestWriteResponseLogsAnErrorWhenTheClientConnectionBreaks(t *testing.T) {
	logger, logs := loggerWithCapturedOutput()

	WriteResponse(newFailingResponseWriterMock(), http.StatusOK, map[string]string{"status": "ok"}, logger)

	if !strings.Contains(logs.String(), "connection reset by peer") {
		t.Fatalf("expected the write failure in the logs, got %q", logs.String())
	}
}

func TestWriteErrorResponseUsesTheGivenStatusCode(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()

	WriteErrorResponse(recorder, http.StatusBadRequest, map[string]string{"error": "bad payload"}, logger)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestWriteErrorResponseDeclaresAJSONContentType(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()

	WriteErrorResponse(recorder, http.StatusBadRequest, map[string]string{"error": "bad payload"}, logger)

	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestWriteErrorResponseWritesTheErrorFieldAsJSON(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	recorder := httptest.NewRecorder()

	WriteErrorResponse(recorder, http.StatusInternalServerError, FormatError("boom", nil), logger)

	if got := decodedBody(t, recorder)["error"]; got != "boom" {
		t.Fatalf("error field = %v, want boom", got)
	}
}

func TestWriteErrorResponseLogsAnErrorWhenTheClientConnectionBreaks(t *testing.T) {
	logger, logs := loggerWithCapturedOutput()

	WriteErrorResponse(newFailingResponseWriterMock(), http.StatusBadRequest, map[string]string{"error": "bad payload"}, logger)

	if !strings.Contains(logs.String(), "connection reset by peer") {
		t.Fatalf("expected the write failure in the logs, got %q", logs.String())
	}
}

func TestWriteErrorResponseStillSendsTheStatusWhenTheClientConnectionBreaks(t *testing.T) {
	logger, _ := loggerWithCapturedOutput()
	writer := newFailingResponseWriterMock()

	WriteErrorResponse(writer, http.StatusBadGateway, map[string]string{"error": "x"}, logger)

	if writer.status != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", writer.status, http.StatusBadGateway)
	}
}
