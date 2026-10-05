package main

import (
	"bytes"
	"context"
	"kafka-golang-analytics/internal/config"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type serverRun struct {
	stopCalls int
	logs      string
}

func producerConfigServingAt(addr string) config.ProducerConfig {
	return config.ProducerConfig{
		BaseConfig:   config.BaseConfig{Brokers: []string{"broker-1:19092"}},
		ProduceTopic: "incoming.user_activity",
		HttpAddr:     addr,
	}
}

func runServerAndWaitForItToReturn(server *http.Server) serverRun {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	run := serverRun{}
	stop := func() { run.stopCalls++ }

	runServer(server, logger, producerConfigServingAt(server.Addr), stop)

	run.logs = logs.String()
	return run
}

func serverThatWasAlreadyShutDown(t *testing.T) *http.Server {
	t.Helper()
	server := &http.Server{Addr: "127.0.0.1:0"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown of an idle server failed: %v", err)
	}
	return server
}

func serverWhosePortIsTakenByAnotherListener(t *testing.T) *http.Server {
	t.Helper()
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not occupy a port: %v", err)
	}
	t.Cleanup(func() { occupied.Close() })
	return &http.Server{Addr: occupied.Addr().String()}
}

func TestRunServerDoesNotStopTheProcessAfterAGracefulShutdown(t *testing.T) {
	run := runServerAndWaitForItToReturn(serverThatWasAlreadyShutDown(t))

	if run.stopCalls != 0 {
		t.Fatalf("stop was called %d times after a graceful shutdown, want 0", run.stopCalls)
	}
}

func TestRunServerDoesNotReportAFailureAfterAGracefulShutdown(t *testing.T) {
	run := runServerAndWaitForItToReturn(serverThatWasAlreadyShutDown(t))

	if strings.Contains(run.logs, "failed to start http server") {
		t.Fatalf("a graceful shutdown was logged as a failure: %s", run.logs)
	}
}

func TestRunServerAnnouncesTheAddressBrokersAndTopic(t *testing.T) {
	run := runServerAndWaitForItToReturn(serverThatWasAlreadyShutDown(t))

	for _, expected := range []string{"starting http server at", "127.0.0.1:0", "broker-1:19092", "incoming.user_activity"} {
		if !strings.Contains(run.logs, expected) {
			t.Errorf("start announcement %s does not mention %q", run.logs, expected)
		}
	}
}

func TestRunServerStopsTheProcessWhenItCannotListenOnItsAddress(t *testing.T) {
	run := runServerAndWaitForItToReturn(serverWhosePortIsTakenByAnotherListener(t))

	if run.stopCalls != 1 {
		t.Fatalf("stop was called %d times, want exactly 1", run.stopCalls)
	}
}

func TestRunServerLogsWhyItCouldNotStart(t *testing.T) {
	run := runServerAndWaitForItToReturn(serverWhosePortIsTakenByAnotherListener(t))

	if !strings.Contains(run.logs, "failed to start http server") || !strings.Contains(run.logs, "address already in use") {
		t.Fatalf("expected the startup failure and its cause in the logs, got %s", run.logs)
	}
}
