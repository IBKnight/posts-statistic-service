package snapshot

import (
	"testing"

	"github.com/IBKnight/posts-statistic-service/internal/storage"
)

// TestFullRestoreRoundTrip simulates exactly what the kafka consumer does
// around a restart: apply events to a live Store, Snapshot+Encode it (what
// a backup does), Decode+Restore into a brand new Store (what happens after
// a crash), and check the two stores agree. This is the test that would
// catch slotSize/PostStats drifting out of sync after a struct change.
func TestFullRestoreRoundTrip(t *testing.T) {
	const baseID = 1

	original := storage.NewStore(baseID)

	events := []storage.Event{
		{EventID: "e1", PostID: 1, Type: storage.EventLike, CreatedAt: "2026-01-01T00:00:00Z"},
		{EventID: "e2", PostID: 1, Type: storage.EventLike, CreatedAt: "2026-01-01T00:00:00Z"},
		{EventID: "e2", PostID: 1, Type: storage.EventLike, CreatedAt: "2026-01-01T00:00:00Z"}, // duplicate, must not double count
		{EventID: "e3", PostID: 1, Type: storage.EventView, CreatedAt: "2026-01-01T00:00:00Z"},
		{EventID: "e4", PostID: 42, Type: storage.EventShare, CreatedAt: "2026-01-01T00:00:00Z"},
		{EventID: "e5", PostID: 42, Type: storage.EventReport, CreatedAt: "2026-01-01T00:00:00Z"},
	}

	for _, e := range events {
		original.Apply(e)
	}

	snap := original.Snapshot(nil)

	data, err := Encode(State{BaseID: original.BaseID(), Offset: 999, Slots: snap})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	state, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if state.Offset != 999 {
		t.Errorf("Offset = %d, want 999", state.Offset)
	}

	restored := storage.NewStore(baseID)
	if err := restored.Restore(state.Slots); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	for _, postID := range []int64{1, 42} {
		want, wantOK := original.Get(postID)
		got, gotOK := restored.Get(postID)

		if wantOK != gotOK {
			t.Fatalf("post %d: found = %v, want %v", postID, gotOK, wantOK)
		}
		if want != got {
			t.Errorf("post %d: restored = %+v, want %+v", postID, got, want)
		}
	}

	// A duplicate replayed after restore must still be recognized as such —
	// this is the whole point of persisting the dedup ring across restarts.
	if res := restored.Apply(events[1]); res != storage.ApplyDuplicate {
		t.Errorf("replaying e2 after restore: got %v, want ApplyDuplicate", res)
	}
}
