package routers

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Producer(ctx context.Context, client *kgo.Client, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /user_activities", handlePublish(ctx, client, logger))

	return mux
}
