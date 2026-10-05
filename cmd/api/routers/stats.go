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
		page, pageSize, err := parsePagination(r.URL.Query())

		if err != nil {
			logger.Warn("invalid pagination parameters", "error", err)
			httputils.WriteErrorResponse(w, http.StatusBadRequest, httputils.FormatError("invalid pagination parameters", err), logger)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
		defer cancel()

		stats, err := store.GetStats(ctx, page, pageSize)

		if err != nil {
			logger.Error("failed to read user activity stats", "error", err)
			httputils.WriteErrorResponse(w, http.StatusInternalServerError, httputils.FormatError("failed to read user activity stats", nil), logger)
			return
		}

		httputils.WriteResponse(w, http.StatusOK, stats, logger)
	}
}
