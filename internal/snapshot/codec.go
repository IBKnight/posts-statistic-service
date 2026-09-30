package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/IBKnight/posts-statistic-service/internal/storage"
)

const (
	magic   uint32 = 0x50535431
	version uint32 = 1

	headerSize = 32
	slotSize   = 17
)

var ErrNotFound = errors.New("snapshot not found")

type State struct {
	BaseID int64
	Offset int64
	Slots  []storage.PostStats
}

func Encode(state State) ([]byte, error) {
	if len(state.Slots) != storage.ShardSize {
		return nil, fmt.Errorf("encode: got %d slots, want %d", len(state.Slots), storage.ShardSize)
	}

	buf := make([]byte, headerSize+len(state.Slots)*slotSize)
	payload := buf[headerSize:]

	for i, slot := range state.Slots {
		off := i * slotSize

		binary.LittleEndian.PutUint32(payload[off:], slot.Views)
		binary.LittleEndian.PutUint32(payload[off+4:], slot.Likes)
		binary.LittleEndian.PutUint32(payload[off+8:], slot.Shares)
		binary.LittleEndian.PutUint32(payload[off+12:], slot.Reports)

		if slot.Exists {
			payload[off+16] = 1
		}
	}

	binary.LittleEndian.PutUint32(buf[0:], magic)
	binary.LittleEndian.PutUint32(buf[4:], version)
	binary.LittleEndian.PutUint64(buf[8:], uint64(state.BaseID))
	binary.LittleEndian.PutUint64(buf[16:], uint64(state.Offset))
	binary.LittleEndian.PutUint32(buf[24:], uint32(len(state.Slots)))
	binary.LittleEndian.PutUint32(buf[28:], crc32.ChecksumIEEE(payload))

	return buf, nil
}

func Decode(data []byte) (State, error) {
	if len(data) < headerSize {
		return State{}, fmt.Errorf("decode: too short: %d bytes", len(data))
	}

	if got := binary.LittleEndian.Uint32(data[0:]); got != magic {
		return State{}, fmt.Errorf("decode: bad magic %#x", got)
	}

	if got := binary.LittleEndian.Uint32(data[4:]); got != version {
		return State{}, fmt.Errorf("decode: unsupported version %d, want %d", got, version)
	}

	baseID := int64(binary.LittleEndian.Uint64(data[8:]))
	offset := int64(binary.LittleEndian.Uint64(data[16:]))
	count := int(binary.LittleEndian.Uint32(data[24:]))
	checksum := binary.LittleEndian.Uint32(data[28:])

	if count != storage.ShardSize {
		return State{}, fmt.Errorf("decode: got %d slots, want %d", count, storage.ShardSize)
	}

	expected := headerSize + count*slotSize
	if len(data) != expected {
		return State{}, fmt.Errorf("decode: got %d bytes, want %d", len(data), expected)
	}

	payload := data[headerSize:]

	if got := crc32.ChecksumIEEE(payload); got != checksum {
		return State{}, errors.New("decode: checksum mismatch")
	}

	slots := make([]storage.PostStats, count)

	for i := range slots {
		off := i * slotSize

		slots[i] = storage.PostStats{
			Views:   binary.LittleEndian.Uint32(payload[off:]),
			Likes:   binary.LittleEndian.Uint32(payload[off+4:]),
			Shares:  binary.LittleEndian.Uint32(payload[off+8:]),
			Reports: binary.LittleEndian.Uint32(payload[off+12:]),
			Exists:  payload[off+16] == 1,
		}
	}

	return State{BaseID: baseID, Offset: offset, Slots: slots}, nil
}
