package logging

import (
	"log/slog"
	"os"
	"strings"
)

type LoggingConfig struct {
	Level string `json:"level,omitempty"`
}

func New(cfg LoggingConfig) *slog.Logger {

	opts := &slog.HandlerOptions{
		Level: ParseLevel(cfg.Level),
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)

	return slog.New(handler)
}

func ParseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
