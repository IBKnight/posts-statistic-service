package storage

import (
	"fmt"
	"sync"
	"testing"
)

func newTestStore() *Store {
	return NewStore(1)
}

func TestApply_DuplicateEventIDIsIgnored(t *testing.T) {
	s := newTestStore()
	e := Event{EventID: "evt-1", PostID: 1, Type: EventLike, CreatedAt: "2026-01-01T00:00:00Z"}

	if res := s.Apply(e); res != ApplyOK {
		t.Fatalf("first apply: got %v, want ApplyOK", res)
	}
	if res := s.Apply(e); res != ApplyDuplicate {
		t.Fatalf("replayed apply: got %v, want ApplyDuplicate", res)
	}

	stats, ok := s.Get(1)
	if !ok {
		t.Fatal("post not found after apply")
	}
	if stats.Likes != 1 {
		t.Fatalf("Likes = %d, want 1 (duplicate must not double-count)", stats.Likes)
	}
}

func TestApply_DistinctEventIDsBothCount(t *testing.T) {
	s := newTestStore()

	s.Apply(Event{EventID: "evt-1", PostID: 1, Type: EventLike, CreatedAt: "2026-01-01T00:00:00Z"})
	s.Apply(Event{EventID: "evt-2", PostID: 1, Type: EventLike, CreatedAt: "2026-01-01T00:00:00Z"})

	stats, _ := s.Get(1)
	if stats.Likes != 2 {
		t.Fatalf("Likes = %d, want 2", stats.Likes)
	}
}

func TestApply_DedupWindowIsPerPost(t *testing.T) {
	s := newTestStore()

	// Same EventID on two different posts must not collide.
	s.Apply(Event{EventID: "evt-1", PostID: 1, Type: EventLike, CreatedAt: "2026-01-01T00:00:00Z"})
	res := s.Apply(Event{EventID: "evt-1", PostID: 2, Type: EventLike, CreatedAt: "2026-01-01T00:00:00Z"})

	if res != ApplyOK {
		t.Fatalf("apply on different post: got %v, want ApplyOK", res)
	}
}

func TestApply_DedupWindowEvictsOldestEntry(t *testing.T) {
	s := newTestStore()
	postID := int64(1)

	// Fill the ring past its capacity with distinct EventIDs.
	for i := 0; i < dedupWindow+1; i++ {
		res := s.Apply(Event{
			EventID:   string(rune('a' + i)),
			PostID:    postID,
			Type:      EventView,
			CreatedAt: "2026-01-01T00:00:00Z",
		})
		if res != ApplyOK {
			t.Fatalf("apply #%d: got %v, want ApplyOK", i, res)
		}
	}

	// The very first EventID has been evicted, so replaying it must apply again.
	res := s.Apply(Event{EventID: "a", PostID: postID, Type: EventView, CreatedAt: "2026-01-01T00:00:00Z"})
	if res != ApplyOK {
		t.Fatalf("replay of evicted EventID: got %v, want ApplyOK", res)
	}

	// The most recent EventID is still remembered.
	res = s.Apply(Event{EventID: string(rune('a' + dedupWindow)), PostID: postID, Type: EventView, CreatedAt: "2026-01-01T00:00:00Z"})
	if res != ApplyDuplicate {
		t.Fatalf("replay of recent EventID: got %v, want ApplyDuplicate", res)
	}
}

func TestApply_NotOwned(t *testing.T) {
	s := newTestStore()
	e := Event{EventID: "evt-1", PostID: s.MaxID() + 1, Type: EventLike, CreatedAt: "2026-01-01T00:00:00Z"}

	if res := s.Apply(e); res != ApplyNotOwned {
		t.Fatalf("got %v, want ApplyNotOwned", res)
	}
}

func TestApply_UnlikeFloorsAtZero(t *testing.T) {
	s := newTestStore()
	s.Apply(Event{EventID: "evt-1", PostID: 1, Type: EventUnlike, CreatedAt: "2026-01-01T00:00:00Z"})

	stats, _ := s.Get(1)
	if stats.Likes != 0 {
		t.Fatalf("Likes = %d, want 0", stats.Likes)
	}
}

func TestShardForPostID(t *testing.T) {
	cases := []struct {
		postID int64
		want   int
	}{
		{1, 0},
		{ShardSize, 0},
		{ShardSize + 1, 1},
		{2 * ShardSize, 1},
	}

	for _, c := range cases {
		if got := ShardForPostID(c.postID); got != c.want {
			t.Errorf("ShardForPostID(%d) = %d, want %d", c.postID, got, c.want)
		}
	}
}

// TestStore_ConcurrentAccess exercises Apply/Get/Snapshot/Count from many
// goroutines at once. It carries no meaningful assertions of its own — its
// job is to run clean under `go test -race`, proving the per-bucket locking
// in Apply/Get/Snapshot/Restore actually holds up under contention.
func TestStore_ConcurrentAccess(t *testing.T) {
	s := newTestStore()

	const (
		writers      = 50
		opsPerWriter = 200
		posts        = 20
	)

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := 0; i < opsPerWriter; i++ {
				postID := int64(1 + (i % posts))
				s.Apply(Event{
					EventID:   fmt.Sprintf("w%d-e%d", w, i),
					PostID:    postID,
					Type:      EventLike,
					CreatedAt: "2026-01-01T00:00:00Z",
				})
			}
		}(w)
	}

	stop := make(chan struct{})
	var readersWG sync.WaitGroup
	for r := 0; r < 4; r++ {
		readersWG.Add(1)
		go func(r int) {
			defer readersWG.Done()
			var buf []PostStats
			for {
				select {
				case <-stop:
					return
				default:
					s.Get(int64(1 + r%posts))
					buf = s.Snapshot(buf)
					s.Count()
				}
			}
		}(r)
	}

	writersWG.Wait()
	close(stop)
	readersWG.Wait()

	stats, ok := s.Get(1)
	if !ok || stats.Likes == 0 {
		t.Fatalf("expected post 1 to have been liked, got %+v (ok=%v)", stats, ok)
	}
}

func TestStore_Owns(t *testing.T) {
	s := NewForShard(1)

	if s.Owns(ShardSize) {
		t.Errorf("shard 1 must not own postID %d", ShardSize)
	}
	if !s.Owns(ShardSize + 1) {
		t.Errorf("shard 1 must own postID %d", ShardSize+1)
	}
	if !s.Owns(2 * ShardSize) {
		t.Errorf("shard 1 must own postID %d", 2*ShardSize)
	}
}
