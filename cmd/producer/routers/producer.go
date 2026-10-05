package routers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Producer(ctx context.Context, client *kgo.Client, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /user_activities", handlePublish(ctx, client, logger))

	return mux
}

func formatError(errorMsg string, err error) map[string]string {
	if err != nil {
		formatted := fmt.Sprintf("%s: %s", errorMsg, err.Error())
		return map[string]string{"error": formatted}
	}

	return map[string]string{"error": errorMsg}
}

func writeResponse(w http.ResponseWriter, status int, body interface{}, logger *slog.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(body)

	if err != nil {
		logger.Error("error encoding body to json", "error", err)
	}
}

func writeErrorResponse(w http.ResponseWriter, status int, body map[string]string, logger *slog.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(body)

	if err != nil {
		logger.Error("error encoding body to json", "error", err)
	}
}
