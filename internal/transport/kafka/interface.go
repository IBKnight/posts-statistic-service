package kafka

import (
	"context"

	"github.com/IBKnight/posts-statistic-service/internal/snapshot"
	"github.com/IBKnight/posts-statistic-service/internal/storage"
)

type Store interface {
	Apply(e storage.Event) storage.ApplyResult
	Snapshot(dst []storage.PostStats) []storage.PostStats
	Restore(src []storage.PostStats) error
	BaseID() int64
}

type Snapshots interface {
	Backup(ctx context.Context, data []byte) error
	Load(ctx context.Context) (snapshot.State, error)
}
