package main

import (
	"log/slog"

	"github.com/IBKnight/posts-statistic-service/internal/app"
)

func main() {
	if err := app.Init(); err != nil {
		slog.Error("failed to run app: ", "err", err)
	}
}
