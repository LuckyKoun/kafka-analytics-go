package routers

import (
	"context"
	httputils "kafka-golang-analytics/internal/http_utils"
	"log/slog"
	"net/http"
	"time"
)

func handleStats(store StatsReader, logger *slog.Logger, queryTimeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
		defer cancel()

		stats, err := store.GetStats(ctx)

		if err != nil {
			logger.Error("failed to read user activity stats", "error", err)
			httputils.WriteErrorResponse(w, http.StatusInternalServerError, httputils.FormatError("failed to read user activity stats", nil), logger)
			return
		}

		httputils.WriteResponse(w, http.StatusOK, stats, logger)
	}
}
