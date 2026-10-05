package routers

import (
	"context"
	"kafka-golang-analytics/internal/types"
	"log/slog"
	"net/http"
	"time"
)

type StatsReader interface {
	GetStats(ctx context.Context) (types.Stats, error)
}

func Router(store StatsReader, logger *slog.Logger, queryTimeout time.Duration) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /stats", handleStats(store, logger, queryTimeout))

	return mux
}
