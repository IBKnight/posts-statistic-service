package storage

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventType string

const (
	EventView   EventType = "view"
	EventLike   EventType = "like"
	EventUnlike EventType = "unlike"
	EventShare  EventType = "share"
	EventReport EventType = "report"
)

const createdAtLayout = "2006-01-02T15:04"

type Event struct {
	EventID   string    `json:"EventID"`
	UserID    int64     `json:"UserID"`
	PostID    int64     `json:"PostID"`
	Type      EventType `json:"Type"`
	CreatedAt string    `json:"CreatedAt"`
}

func ParseEvent(data []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, fmt.Errorf("unmarshal event: %w", err)
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

func (e Event) Validate() error {
	if e.EventID == "" {
		return fmt.Errorf("empty EventID")
	}
	if e.PostID <= 0 {
		return fmt.Errorf("bad PostID: %d", e.PostID)
	}
	if !e.Type.Known() {
		return fmt.Errorf("unknown Type: %q", e.Type)
	}
	return nil
}

func (t EventType) Known() bool {
	switch t {
	case EventView, EventLike, EventUnlike, EventShare, EventReport:
		return true
	default:
		return false
	}
}

func (e Event) Time() (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, e.CreatedAt); err == nil {
		return t, nil
	}
	t, err := time.Parse(createdAtLayout, e.CreatedAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse CreatedAt %q: %w", e.CreatedAt, err)
	}
	return t, nil
}
