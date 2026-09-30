package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Store interface {
	Count() int
}

type Consumer interface {
	Ready() (ready bool, lag int64)
	Applied() uint64
	Skipped() uint64
	Duplicate() uint64
	Failed() uint64
	LastBackupUnix() int64
	LastBackupDurationSeconds() float64
}

// Handler registers this shard's metrics against the default Prometheus
// registry and returns the scrape handler. Call once per process.
func Handler(shardOrdinal int, store Store, cons Consumer) http.Handler {
	labels := prometheus.Labels{"shard": strconv.Itoa(shardOrdinal)}

	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "post_stats_kafka_lag",
		Help:        "Estimated records behind the partition's high watermark.",
		ConstLabels: labels,
	}, func() float64 {
		_, lag := cons.Ready()
		return float64(lag)
	})

	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name:        "post_stats_events_applied_total",
		Help:        "Events applied to this shard's in-memory state.",
		ConstLabels: labels,
	}, func() float64 { return float64(cons.Applied()) })

	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name:        "post_stats_events_skipped_total",
		Help:        "Events consumed that do not belong to this shard. Any sustained non-zero rate means the producer's partitioning contract is being violated.",
		ConstLabels: labels,
	}, func() float64 { return float64(cons.Skipped()) })

	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name:        "post_stats_events_duplicate_total",
		Help:        "Events skipped because their EventID was already applied.",
		ConstLabels: labels,
	}, func() float64 { return float64(cons.Duplicate()) })

	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name:        "post_stats_events_failed_total",
		Help:        "Records that failed to parse as a valid event.",
		ConstLabels: labels,
	}, func() float64 { return float64(cons.Failed()) })

	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "post_stats_last_backup_unix_seconds",
		Help:        "Unix timestamp of the last successful backup upload. Alert on time() - this value exceeding the backup interval by a wide margin.",
		ConstLabels: labels,
	}, func() float64 { return float64(cons.LastBackupUnix()) })

	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "post_stats_last_backup_duration_seconds",
		Help:        "Duration of the last successful backup upload to MinIO.",
		ConstLabels: labels,
	}, func() float64 { return cons.LastBackupDurationSeconds() })

	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "post_stats_shard_posts",
		Help:        "Number of distinct posts currently tracked by this shard.",
		ConstLabels: labels,
	}, func() float64 { return float64(store.Count()) })

	return promhttp.Handler()
}
