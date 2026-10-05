package routers

import (
	"context"
	"log/slog"
	"net/http"
)

func Router(ctx context.Context, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /stats", handleStats(ctx, logger))

	return mux
}

func handleStats(ctx context.Context, logger *slog.Logger) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

	}
}
