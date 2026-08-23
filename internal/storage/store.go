package storage

import (
	"fmt"
	"sync"
)

const (
	ShardSize  = 10000
	BucketsNum = 10
)

type PostStats struct {
	Views   uint32
	Likes   uint32
	Shares  uint32
	Reports uint32
	Exists  bool
}

type Store struct {
	baseID  int64
	slots   []PostStats
	buckets [BucketsNum]sync.RWMutex
}

func New(baseID int64) *Store {
	return &Store{
		baseID: baseID,
		slots:  make([]PostStats, ShardSize),
	}
}

func NewForShard(ordinal int) *Store {
	return New(BaseIDForShard(ordinal))
}

func BaseIDForShard(ordinal int) int64 {
	return int64(ordinal)*ShardSize + 1
}

func ShardForPostID(postID int64) int {
	return int((postID - 1) / ShardSize)
}

func (s *Store) BaseID() int64 {
	return s.baseID
}

func (s *Store) MaxID() int64 {
	return s.baseID + ShardSize - 1
}

func (s *Store) Owns(postID int64) bool {
	return postID >= s.baseID && postID <= s.MaxID()
}

func (s *Store) index(postID int64) int {
	return int(postID - s.baseID)
}

func (s *Store) bucket(i int) *sync.RWMutex {
	return &s.buckets[i%BucketsNum]
}

func (s *Store) Apply(e Event) bool {
	if !s.Owns(e.PostID) {
		return false
	}

	i := s.index(e.PostID)
	mu := s.bucket(i)

	mu.Lock()
	defer mu.Unlock()

	slot := &s.slots[i]

	switch e.Type {
	case EventView:
		slot.Views++
	case EventLike:
		slot.Likes++
	case EventUnlike:
		if slot.Likes > 0 {
			slot.Likes--
		}
	case EventShare:
		slot.Shares++
	case EventReport:
		slot.Reports++
	default:
		return false
	}

	slot.Exists = true
	return true
}

func (s *Store) Get(postID int64) (PostStats, bool) {
	if !s.Owns(postID) {
		return PostStats{}, false
	}

	i := s.index(postID)
	mu := s.bucket(i)

	mu.RLock()
	defer mu.RUnlock()

	stats := s.slots[i]
	return stats, stats.Exists
}

func (s *Store) GetMany(postIDs []int64) map[int64]PostStats {
	out := make(map[int64]PostStats, len(postIDs))
	for _, id := range postIDs {
		if stats, ok := s.Get(id); ok {
			out[id] = stats
		}
	}
	return out
}

func (s *Store) Count() int {
	total := 0
	for b := 0; b < BucketsNum; b++ {
		s.buckets[b].RLock()
		for i := b; i < ShardSize; i += BucketsNum {
			if s.slots[i].Exists {
				total++
			}
		}
		s.buckets[b].RUnlock()
	}
	return total
}

func (s *Store) Snapshot(dst []PostStats) []PostStats {
	if cap(dst) < ShardSize {
		dst = make([]PostStats, ShardSize)
	}
	dst = dst[:ShardSize]

	s.rLockAll()
	copy(dst, s.slots)
	s.rUnlockAll()

	return dst
}

func (s *Store) Restore(src []PostStats) error {
	if len(src) != ShardSize {
		return fmt.Errorf("restore: got %d slots, want %d", len(src), ShardSize)
	}

	s.lockAll()
	copy(s.slots, src)
	s.unlockAll()

	return nil
}

func (s *Store) lockAll() {
	for i := 0; i < BucketsNum; i++ {
		s.buckets[i].Lock()
	}
}

func (s *Store) unlockAll() {
	for i := BucketsNum - 1; i >= 0; i-- {
		s.buckets[i].Unlock()
	}
}

func (s *Store) rLockAll() {
	for i := 0; i < BucketsNum; i++ {
		s.buckets[i].RLock()
	}
}

func (s *Store) rUnlockAll() {
	for i := BucketsNum - 1; i >= 0; i-- {
		s.buckets[i].RUnlock()
	}
}
