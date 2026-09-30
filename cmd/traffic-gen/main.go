// traffic-gen produces synthetic post-events onto Kafka for local load
// testing. It honors the sharding contract described in README.md: each
// event is published to partition storage.ShardForPostID(post_id), so the
// event lands on the shard that will actually own it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IBKnight/posts-statistic-service/internal/storage"
)

func main() {
	var (
		brokers   = flag.String("brokers", "localhost:9092", "comma-separated Kafka broker list")
		topic     = flag.String("topic", "post-events", "target topic")
		shards    = flag.Int("shards", 2, "number of shards (must match the topic's partition count)")
		rate      = flag.Float64("rate", 100, "events per second")
		duration  = flag.Duration("duration", 0, "how long to run (0 = until interrupted)")
		dupRate   = flag.Float64("dup-rate", 0.02, "fraction of events that resend a recent EventID (simulates producer retries)")
		badRate   = flag.Float64("bad-rate", 0, "fraction of events sent malformed (simulates poison messages)")
		postIDMin = flag.Int64("post-id-min", 1, "lowest post id to generate traffic for")
		seed      = flag.Int64("seed", time.Now().UnixNano(), "random seed")
		logEvery  = flag.Duration("log-every", 5*time.Second, "stats logging interval")
	)
	flag.Parse()

	maxPostID := *postIDMin + int64(*shards)*storage.ShardSize - 1

	rng := rand.New(rand.NewSource(*seed))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		slog.Error("failed to create kafka client", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	slog.Info("starting traffic generation",
		"brokers", *brokers,
		"topic", *topic,
		"shards", *shards,
		"post_id_range", [2]int64{*postIDMin, maxPostID},
		"rate", *rate,
	)

	var sent, failed, dup, bad atomic.Uint64

	recent := newRing(64)

	statsTicker := time.NewTicker(*logEvery)
	defer statsTicker.Stop()

	interval := time.Duration(float64(time.Second) / *rate)
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop

		case <-statsTicker.C:
			slog.Info("stats",
				"sent", sent.Load(),
				"failed", failed.Load(),
				"duplicate", dup.Load(),
				"malformed", bad.Load(),
			)

		case <-ticker.C:
			postID := *postIDMin + rng.Int63n(maxPostID-*postIDMin+1)
			shard := storage.ShardForPostID(postID)

			eventID := uuid.NewString()
			isDup := recent.len() > 0 && rng.Float64() < *dupRate
			if isDup {
				eventID = recent.pick(rng)
				dup.Add(1)
			} else {
				recent.push(eventID)
			}

			payload, malformed := encodeEvent(rng, eventID, postID, *badRate)
			if malformed {
				bad.Add(1)
			}

			record := &kgo.Record{
				Topic:     *topic,
				Partition: int32(shard),
				Value:     payload,
			}

			client.Produce(ctx, record, func(_ *kgo.Record, err error) {
				if err != nil {
					failed.Add(1)
					slog.Debug("produce failed", "err", err)
					return
				}
				sent.Add(1)
			})
		}
	}

	client.Flush(context.Background())

	slog.Info("stopped",
		"sent", sent.Load(),
		"failed", failed.Load(),
		"duplicate", dup.Load(),
		"malformed", bad.Load(),
	)
}

var eventTypes = []storage.EventType{
	storage.EventView,
	storage.EventView,
	storage.EventView,
	storage.EventView,
	storage.EventView,
	storage.EventView,
	storage.EventView,
	storage.EventLike,
	storage.EventLike,
	storage.EventUnlike,
	storage.EventShare,
	storage.EventReport,
}

// encodeEvent builds the JSON payload for one event. With probability
// badRate it returns a deliberately malformed payload (missing Type) to
// exercise the consumer's post_stats_events_failed handling.
func encodeEvent(rng *rand.Rand, eventID string, postID int64, badRate float64) ([]byte, bool) {
	if rng.Float64() < badRate {
		data, _ := json.Marshal(map[string]any{
			"EventID":   eventID,
			"UserID":    rng.Int63n(1_000_000) + 1,
			"PostID":    postID,
			"CreatedAt": time.Now().UTC().Format(time.RFC3339),
		})
		return data, true
	}

	e := storage.Event{
		EventID:   eventID,
		UserID:    rng.Int63n(1_000_000) + 1,
		PostID:    postID,
		Type:      eventTypes[rng.Intn(len(eventTypes))],
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	data, _ := json.Marshal(e)
	return data, false
}

// ring keeps the last n EventIDs so the generator can resend one to
// simulate at-least-once redelivery/producer retries.
type ring struct {
	ids  []string
	next int
}

func newRing(n int) *ring {
	return &ring{ids: make([]string, 0, n)}
}

func (r *ring) push(id string) {
	if len(r.ids) < cap(r.ids) {
		r.ids = append(r.ids, id)
		return
	}
	r.ids[r.next] = id
	r.next = (r.next + 1) % len(r.ids)
}

func (r *ring) len() int { return len(r.ids) }

func (r *ring) pick(rng *rand.Rand) string {
	return r.ids[rng.Intn(len(r.ids))]
}
