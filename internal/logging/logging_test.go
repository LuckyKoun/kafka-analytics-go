package logging

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

var allLevels = []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}

func enabledLevels(logger *slog.Logger) []slog.Level {
	var enabled []slog.Level
	for _, level := range allLevels {
		if logger.Enabled(context.Background(), level) {
			enabled = append(enabled, level)
		}
	}
	return enabled
}

func sameLevels(a, b []slog.Level) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func standardOutputProducedBy(t *testing.T, produce func()) string {
	t.Helper()
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("could not create pipe: %v", err)
	}
	os.Stdout = writer
	defer func() { os.Stdout = original }()

	produce()

	writer.Close()
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("could not read captured output: %v", err)
	}
	return string(output)
}

func TestParseLevelMapsEachSupportedNameToItsSlogLevel(t *testing.T) {
	cases := []struct {
		name       string
		configured string
		want       slog.Level
	}{
		{"debug selects the debug level", "debug", slog.LevelDebug},
		{"info selects the info level", "info", slog.LevelInfo},
		{"warn selects the warn level", "warn", slog.LevelWarn},
		{"error selects the error level", "error", slog.LevelError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseLevel(tc.configured); got != tc.want {
				t.Fatalf("ParseLevel(%q) = %v, want %v", tc.configured, got, tc.want)
			}
		})
	}
}

func TestParseLevelIgnoresLetterCase(t *testing.T) {
	cases := map[string]slog.Level{
		"DEBUG": slog.LevelDebug,
		"Warn":  slog.LevelWarn,
		"eRRoR": slog.LevelError,
		"INFO":  slog.LevelInfo,
	}

	for configured, want := range cases {
		t.Run(configured, func(t *testing.T) {
			if got := ParseLevel(configured); got != want {
				t.Fatalf("ParseLevel(%q) = %v, want %v", configured, got, want)
			}
		})
	}
}

func TestParseLevelFallsBackToInfoForNamesItDoesNotKnow(t *testing.T) {
	cases := map[string]string{
		"an empty value":         "",
		"a made up level":        "verbose",
		"a numeric looking name": "10",
	}

	for name, configured := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ParseLevel(configured); got != slog.LevelInfo {
				t.Fatalf("ParseLevel(%q) = %v, want %v", configured, got, slog.LevelInfo)
			}
		})
	}
}

func TestParseLevelIgnoresSurroundingWhitespace(t *testing.T) {
	cases := map[string]slog.Level{
		" debug ":  slog.LevelDebug,
		"\twarn\n": slog.LevelWarn,
		"  error":  slog.LevelError,
		"info   ":  slog.LevelInfo,
	}

	for configured, want := range cases {
		t.Run(strings.TrimSpace(configured), func(t *testing.T) {
			if got := ParseLevel(configured); got != want {
				t.Fatalf("ParseLevel(%q) = %v, want %v", configured, got, want)
			}
		})
	}
}

func TestNewLoggerEnablesOnlyTheConfiguredLevelAndAbove(t *testing.T) {
	cases := []struct {
		configured string
		want       []slog.Level
	}{
		{"debug", []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}},
		{"info", []slog.Level{slog.LevelInfo, slog.LevelWarn, slog.LevelError}},
		{"warn", []slog.Level{slog.LevelWarn, slog.LevelError}},
		{"error", []slog.Level{slog.LevelError}},
	}

	for _, tc := range cases {
		t.Run("configured as "+tc.configured, func(t *testing.T) {
			logger := New(LoggingConfig{Level: tc.configured})

			if got := enabledLevels(logger); !sameLevels(got, tc.want) {
				t.Fatalf("enabled levels = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewLoggerDefaultsToInfoWhenNoLevelIsConfigured(t *testing.T) {
	logger := New(LoggingConfig{})

	want := []slog.Level{slog.LevelInfo, slog.LevelWarn, slog.LevelError}
	if got := enabledLevels(logger); !sameLevels(got, want) {
		t.Fatalf("enabled levels = %v, want %v", got, want)
	}
}

func TestNewLoggerWritesOneJSONObjectPerLineToStandardOutput(t *testing.T) {
	output := standardOutputProducedBy(t, func() {
		logger := New(LoggingConfig{Level: "info"})
		logger.Info("user activity recorded", "user_id", "u1", "partition", 3)
	})

	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one log line, got %d: %q", len(lines), output)
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("log line is not json: %q (%v)", lines[0], err)
	}
	if entry["msg"] != "user activity recorded" || entry["level"] != "INFO" {
		t.Fatalf("unexpected msg/level in %v", entry)
	}
	if entry["user_id"] != "u1" || entry["partition"] != float64(3) {
		t.Fatalf("structured attributes missing from %v", entry)
	}
}

func TestNewLoggerSuppressesRecordsBelowTheConfiguredLevel(t *testing.T) {
	output := standardOutputProducedBy(t, func() {
		logger := New(LoggingConfig{Level: "warn"})
		logger.Info("too quiet to appear")
		logger.Debug("also too quiet")
	})

	if output != "" {
		t.Fatalf("expected no output below the configured level, got %q", output)
	}
}
