package httputils

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
)

func FormatError(errorMsg string, err error) map[string]string {
	if err != nil {
		formatted := fmt.Sprintf("%s: %s", errorMsg, err.Error())
		return map[string]string{"error": formatted}
	}

	return map[string]string{"error": errorMsg}
}

func WriteResponse(w http.ResponseWriter, status int, body interface{}, logger *slog.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(body)

	if err != nil {
		logger.Error("error encoding body to json", "error", err)
	}
}

func WriteErrorResponse(w http.ResponseWriter, status int, body map[string]string, logger *slog.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	err := json.NewEncoder(w).Encode(body)

	if err != nil {
		logger.Error("error encoding body to json", "error", err)
	}
}
