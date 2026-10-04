package routers

import (
	"context"
	"encoding/json"
	"kafka-golang-analytics/internal/types"
	"log/slog"
	"net/http"

	"github.com/twmb/franz-go/pkg/kgo"
)

func handlePublish(ctx context.Context, client *kgo.Client, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var activity types.UserActivity

		err := json.NewDecoder(r.Body).Decode(&activity)

		if err != nil {
			logger.Error("invalid user activity body payload", "error", err)
			writeErrorResponse(w, http.StatusBadRequest, formatError("invalid user activity body payload", err), logger)
			return
		}

		value, err := json.Marshal(activity)

		if err != nil {
			logger.Error("error marshalling activity struct", err)
			writeErrorResponse(w, http.StatusInternalServerError, formatError("error encoding payload", err), logger)
			return
		}

		record := &kgo.Record{Key: []byte(activity.UserID), Value: value}

		client.Produce(ctx, record, func(r *kgo.Record, err error) {
			if err != nil {
				logger.Error("failed to publish into kafka", "error", err, "user_id", activity.UserID)
				return
			}

			logger.Info("published activity into kafka",
				"user_id", activity.UserID,
				"activity_type", activity.ActivityType,
				"partition", r.Partition,
				"offset", r.Offset,
			)
		})

		writeResponse(w, http.StatusAccepted, map[string]string{"status": "ok"}, logger)
	}
}
