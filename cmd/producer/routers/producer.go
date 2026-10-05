package routers

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/twmb/franz-go/pkg/kgo"
)

type RecordProducer interface {
	Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error))
}

func Producer(ctx context.Context, client RecordProducer, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /user_activities", handlePublish(ctx, client, logger))

	return mux
}
