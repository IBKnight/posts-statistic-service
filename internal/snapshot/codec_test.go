package snapshot

import (
	"testing"

	"github.com/IBKnight/posts-statistic-service/internal/storage"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	slots := make([]storage.PostStats, storage.ShardSize)
	slots[0] = storage.PostStats{
		Views:   3,
		Likes:   2,
		Shares:  1,
		Reports: 0,
		Exists:  true,
		Recent:  [8]uint64{1, 2, 3, 4, 5, 6, 7, 8},
	}
	slots[42] = storage.PostStats{Exists: true, Recent: [8]uint64{9}}

	state := State{BaseID: 1, Offset: 12345, Slots: slots}

	data, err := Encode(state)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if got.BaseID != state.BaseID {
		t.Errorf("BaseID = %d, want %d", got.BaseID, state.BaseID)
	}
	if got.Offset != state.Offset {
		t.Errorf("Offset = %d, want %d", got.Offset, state.Offset)
	}
	if len(got.Slots) != len(state.Slots) {
		t.Fatalf("len(Slots) = %d, want %d", len(got.Slots), len(state.Slots))
	}
	if got.Slots[0] != state.Slots[0] {
		t.Errorf("Slots[0] = %+v, want %+v", got.Slots[0], state.Slots[0])
	}
	if got.Slots[42] != state.Slots[42] {
		t.Errorf("Slots[42] = %+v, want %+v", got.Slots[42], state.Slots[42])
	}
}

func TestDecode_RejectsOldVersion(t *testing.T) {
	slots := make([]storage.PostStats, storage.ShardSize)
	data, err := Encode(State{BaseID: 1, Slots: slots})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Simulate a v1 snapshot (written before the dedup ring existed).
	data[4] = 1

	if _, err := Decode(data); err == nil {
		t.Fatal("Decode of a mismatched-version snapshot must fail, got nil error")
	}
}

func TestDecode_RejectsCorruptPayload(t *testing.T) {
	slots := make([]storage.PostStats, storage.ShardSize)
	data, err := Encode(State{BaseID: 1, Slots: slots})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	data[headerSize] ^= 0xFF

	if _, err := Decode(data); err == nil {
		t.Fatal("Decode of corrupted payload must fail checksum, got nil error")
	}
}
