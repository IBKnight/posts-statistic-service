package storage

import (
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	ShardSize  = 10000
	BucketsNum = 10

	// dedupWindow is how many recently-applied EventIDs each post remembers.
	// Kafka is at-least-once and a crash replays everything since the last
	// backup, so without this, a redelivered EventID would double-apply a
	// non-idempotent op (Likes++) forever. 8 comfortably covers both producer
	// retries (adjacent offsets) and the replay window after a restart.
	dedupWindow = 8
)

type PostStats struct {
	Views   uint32
	Likes   uint32
	Shares  uint32
	Reports uint32
	Exists  bool
	Recent  [dedupWindow]uint64 // ring of xxhash(EventID) for recently-applied events
}

func (p *PostStats) seen(h uint64) bool {
	for _, v := range p.Recent {
		if v == h {
			return true
		}
	}
	return false
}

func (p *PostStats) remember(h uint64) {
	copy(p.Recent[1:], p.Recent[:dedupWindow-1])
	p.Recent[0] = h
}

type Store struct {
	baseID  int64
	slots   []PostStats
	buckets [BucketsNum]sync.RWMutex
}

func NewStore(baseID int64) *Store {
	return &Store{
		baseID: baseID,
		slots:  make([]PostStats, ShardSize),
	}
}

func NewForShard(ordinal int) *Store {
	return NewStore(BaseIDForShard(ordinal))
}

func BaseIDForShard(ordinal int) int64 {
	return int64(ordinal)*ShardSize + 1
}

// ShardForPostID is also the partitioning contract this service assumes of
// its Kafka producer: partition N of the "post-events" topic must contain
// exactly the events for which ShardForPostID(post_id) == N, and the topic
// must have one partition per shard. If the producer partitions differently
// (or the topic has fewer partitions than shards), events silently end up on
// a shard that doesn't own them and get counted as "skipped" — see the
// post_stats_events_skipped_total metric, which should be alerted on at any
// sustained non-zero rate.
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

type ApplyResult uint8

const (
	ApplyNotOwned ApplyResult = iota
	ApplyDuplicate
	ApplyOK
)

func (s *Store) Apply(e Event) ApplyResult {
	if !s.Owns(e.PostID) {
		return ApplyNotOwned
	}

	i := s.index(e.PostID)
	mu := s.bucket(i)

	mu.Lock()
	defer mu.Unlock()

	slot := &s.slots[i]

	h := xxhash.Sum64String(e.EventID)
	if slot.seen(h) {
		return ApplyDuplicate
	}

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
		// Unreachable via the normal path: ParseEvent validates Type before
		// Apply is ever called. Kept as a safe fallback for direct callers.
		return ApplyNotOwned
	}

	slot.remember(h)
	slot.Exists = true
	return ApplyOK
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
