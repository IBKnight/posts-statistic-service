package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IBKnight/posts-statistic-service/internal/snapshot"
	"github.com/IBKnight/posts-statistic-service/internal/storage"
)

const offsetUnset int64 = -1

type Config struct {
	Brokers        []string
	Topic          string
	Partition      int32
	BackupInterval time.Duration
	UploadTimeout  time.Duration
	LagThreshold   int64
}

type Consumer struct {
	cfg   Config
	store Store
	snaps Snapshots

	offset int64
	buf    []storage.PostStats

	uploads     chan []byte
	uploadsDone chan struct{}

	lag       atomic.Int64
	lagKnown  atomic.Bool
	applied   atomic.Uint64
	skipped   atomic.Uint64
	duplicate atomic.Uint64
	failed    atomic.Uint64
}

func New(cfg Config, store Store, snaps Snapshots) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("no kafka brokers configured")
	}
	if cfg.Topic == "" {
		return nil, errors.New("no kafka topic configured")
	}
	if cfg.BackupInterval <= 0 {
		cfg.BackupInterval = time.Hour
	}
	if cfg.UploadTimeout <= 0 {
		cfg.UploadTimeout = 30 * time.Second
	}
	if cfg.LagThreshold <= 0 {
		cfg.LagThreshold = 1000
	}

	return &Consumer{
		cfg:         cfg,
		store:       store,
		snaps:       snaps,
		offset:      offsetUnset,
		uploads:     make(chan []byte, 1),
		uploadsDone: make(chan struct{}),
	}, nil
}

func (c *Consumer) Ready() (bool, int64) {
	if !c.lagKnown.Load() {
		return false, -1
	}

	lag := c.lag.Load()
	return lag <= c.cfg.LagThreshold, lag
}

func (c *Consumer) Stats() (applied, skipped, failed uint64) {
	return c.applied.Load(), c.skipped.Load(), c.failed.Load()
}

func (c *Consumer) Run(ctx context.Context) error {
	c.restore(ctx)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			c.cfg.Topic: {c.cfg.Partition: c.startOffset()},
		}),
		kgo.FetchMaxWait(time.Second),
	)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	go c.uploadLoop()
	defer c.finish()

	slog.Info("consumer started",
		"topic", c.cfg.Topic,
		"partition", c.cfg.Partition,
		"offset", c.offset,
	)

	backups := time.NewTicker(c.cfg.BackupInterval)
	defer backups.Stop()

	for {
		fetches := client.PollFetches(ctx)

		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("kafka fetch error", "topic", topic, "partition", partition, "err", err)
		})

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, record := range p.Records {
				c.handle(record)
			}

			c.updateLag(p.HighWatermark)
		})

		select {
		case <-backups.C:
			c.enqueueBackup()
		default:
		}
	}
}

func (c *Consumer) restore(ctx context.Context) {
	state, err := c.snaps.Load(ctx)

	switch {
	case errors.Is(err, snapshot.ErrNotFound):
		slog.Info("no snapshot found, reading partition from the beginning")
		return

	case err != nil:
		slog.Error("snapshot is unusable, reading partition from the beginning", "err", err)
		return
	}

	if state.BaseID != c.store.BaseID() {
		slog.Error("snapshot belongs to another shard, ignoring it",
			"snapshot_base_id", state.BaseID,
			"shard_base_id", c.store.BaseID(),
		)
		return
	}

	if err := c.store.Restore(state.Slots); err != nil {
		slog.Error("failed to restore snapshot, reading partition from the beginning", "err", err)
		return
	}

	c.offset = state.Offset

	slog.Info("restored from snapshot", "offset", state.Offset, "base_id", state.BaseID)
}

func (c *Consumer) startOffset() kgo.Offset {
	if c.offset <= offsetUnset {
		return kgo.NewOffset().AtStart()
	}

	return kgo.NewOffset().At(c.offset)
}

func (c *Consumer) handle(record *kgo.Record) {
	c.offset = record.Offset + 1

	event, err := storage.ParseEvent(record.Value)
	if err != nil {
		c.failed.Add(1)
		slog.Debug("malformed event", "offset", record.Offset, "err", err)
		return
	}

	switch c.store.Apply(event) {
	case storage.ApplyOK:
		c.applied.Add(1)
	case storage.ApplyDuplicate:
		c.duplicate.Add(1)
		slog.Debug("duplicate event ignored", "event_id", event.EventID, "post_id", event.PostID, "offset", record.Offset)
	default:
		c.skipped.Add(1)
		slog.Debug("event does not belong to this shard", "post_id", event.PostID, "offset", record.Offset)
	}
}

func (c *Consumer) updateLag(highWatermark int64) {
	lag := highWatermark - c.offset
	if lag < 0 {
		lag = 0
	}

	c.lag.Store(lag)
	c.lagKnown.Store(true)
}

func (c *Consumer) encode() ([]byte, error) {
	c.buf = c.store.Snapshot(c.buf)

	return snapshot.Encode(snapshot.State{
		BaseID: c.store.BaseID(),
		Offset: c.offset,
		Slots:  c.buf,
	})
}

func (c *Consumer) enqueueBackup() {
	data, err := c.encode()
	if err != nil {
		slog.Error("failed to encode snapshot", "err", err)
		return
	}

	select {
	case c.uploads <- data:
	default:
		slog.Warn("previous backup is still uploading, skipping this one")
	}
}

func (c *Consumer) uploadLoop() {
	defer close(c.uploadsDone)

	for data := range c.uploads {
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.UploadTimeout)

		if err := c.snaps.Backup(ctx, data); err != nil {
			slog.Error("failed to save backup", "err", err)
		} else {
			slog.Debug("backup saved", "bytes", len(data))
		}

		cancel()
	}
}

func (c *Consumer) finish() {
	data, err := c.encode()
	if err != nil {
		slog.Error("failed to encode final snapshot", "err", err)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.UploadTimeout)

		if err := c.snaps.Backup(ctx, data); err != nil {
			slog.Error("failed to save final backup", "err", err)
		} else {
			slog.Info("final backup saved", "offset", c.offset)
		}

		cancel()
	}

	close(c.uploads)
	<-c.uploadsDone
}
