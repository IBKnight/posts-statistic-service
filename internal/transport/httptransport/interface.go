package httptransport

import "github.com/IBKnight/posts-statistic-service/internal/storage"

type Store interface {
	Get(postID int64) (storage.PostStats, bool)
	GetMany(postIDs []int64) map[int64]storage.PostStats
	Snapshot(dst []storage.PostStats) []storage.PostStats
	Count() int
	Owns(postID int64) bool
	BaseID() int64
	MaxID() int64
}

type Readiness interface {
	Ready() (ready bool, lag int64)
}

type ReadinessFunc func() (bool, int64)

func (f ReadinessFunc) Ready() (bool, int64) {
	return f()
}

// TODO: change for real readiness from kafka consumer
func AlwaysReady() Readiness {
	return ReadinessFunc(func() (bool, int64) {
		return true, 0
	})
}
