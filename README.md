# posts-statistic-service
service for real-time post statistic calculation

## Local development

`docker compose up --build` brings up a full local stack: single-node Kafka (KRaft), MinIO, and two shards (`shard-0`, `shard-1`). A one-shot `kafka-init` service creates the `post-events` topic with 2 partitions — matching the 2 shards, per the partitioning contract below. Bump both together if you add a third shard.

Per-shard endpoints are published on different host ports so you can poke each one directly:

| Shard | Public API (`:8080`) | Internal (`:9090`) |
|---|---|---|
| shard-0 | `localhost:8081` | `localhost:9081` |
| shard-1 | `localhost:8082` | `localhost:9082` |

This is the environment to use for anything involving restart/restore behavior — it's not meaningfully testable any other way. A quick manual check:

```sh
# produce a like for post 5 (shard 0 owns ids 1-10000)
echo '{"EventID":"evt-1","UserID":1,"PostID":5,"Type":"like","CreatedAt":"2026-01-01T00:00:00Z"}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic post-events

curl localhost:9081/api/dump                 # see it applied
docker compose restart shard-0                # graceful stop -> final backup -> restart -> restore
curl localhost:9081/api/dump                  # same state, not lost, not doubled
```

Note that `kafka-console-producer` doesn't implement this service's partitioner, so a given event may land on either partition — that's fine for exercising restart/restore, but see "Sharding & the producer contract" for what a real producer must do.

## Sharding & the producer contract

Each shard owns a fixed, contiguous range of `ShardSize` (10000) post IDs, computed from its StatefulSet ordinal (`storage.BaseIDForShard`). The Kafka consumer for shard `N` reads **only** partition `N` of the `post-events` topic (`internal/transport/kafka`, `Config.Partition`).

This means the producer must partition by the same formula the service uses to assign shards — `storage.ShardForPostID(post_id) == (post_id-1)/10000` — and the topic must have exactly one partition per shard. This is an implicit contract with a service outside this repository; it is not (and cannot be) enforced here.

What *is* enforced: any event that lands in the wrong partition is detected — `Store.Apply` returns `ApplyNotOwned` and the consumer counts it in `post_stats_events_skipped_total`. **Any sustained non-zero rate on that metric means the partitioning contract is being violated** (wrong formula, or the topic has fewer partitions than shards) and should page. There's no way for a single shard to further distinguish "belongs to another shard" from "belongs to no shard" (it doesn't know the total shard count), but neither case should ever happen with a correctly partitioned topic, so the one metric covers both.

If the producer's partitioner can't be trusted to hold this contract, the alternative is for every shard to consume the *whole* topic and filter client-side by `Owns(post_id)` — not implemented here, since it multiplies consumption cost by the shard count.

## Routing a request to the right shard

There's no proxy in front of the shards — callers are expected to either compute the shard themselves or follow the redirect hint, matching how `getPostStatistic` already behaves (`internal/transport/httptransport/handler.go`): hit the wrong shard for a single post and you get `409 Misdirected Request` with an `X-Correct-Shard` header naming the shard that owns it.

With the headless Service in `deploy/k8s/service.yaml`, a client that wants to go straight to the right pod computes `shard = (post_id-1)/10000` and calls:

```
<statefulset-name>-<shard>.posts-statistic-service.<namespace>.svc.cluster.local:8080
```

For a batch of ids that might span shards, there's no cross-shard fan-out helper — `GET /api/post_stats?ids=...` (below) only returns whichever of the requested ids this particular shard owns.

## HTTP API

- `GET /api/post_stat/{id}` — single post.
- `GET /api/post_stats?ids=1,2,3` — batch (up to 500 ids/request). Ids not owned by this shard, or not found, are silently omitted from the response — same semantics as the underlying `Store.GetMany`.
- `GET /healthz`, `GET /readyz` — liveness/readiness (see probe notes in `deploy/k8s/statefulset.yaml`).
- `GET /api/dump`, `GET /api/status`, `GET /metrics` — internal port only, see "Ports".

## Ports

- `port` (default `8080`) — public API. Safe to expose via Service/Ingress.
- `internal_port` (default `9090`) — operational endpoints: `GET /api/dump`, `GET /api/status`, `GET /metrics`. `/api/dump` and `/api/status` snapshot the entire shard under a lock across every bucket, and none of these three has authentication of its own — **this port must not be published by any k8s Service or Ingress in front of the deployment.** `deploy/k8s/service.yaml` exposes it only as a headless (DNS-only) Service port for in-cluster scraping/debugging, never through an Ingress.

## Kubernetes

`deploy/k8s/` has a StatefulSet, the headless Service, and a Secret template for MinIO credentials:

```sh
docker build -t posts-statistic-service:latest .
kubectl apply -f deploy/k8s/secret.example.yaml   # edit the credentials first
kubectl apply -f deploy/k8s/service.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

Why a StatefulSet with no `volumeClaimTemplates`: state lives in memory and in MinIO backups, never on local disk, so pods don't need a persistent volume — this is purely about the stable `<name>-<ordinal>` pod identity that `shardOrdinal()` (`internal/app/app.go`) parses from `POD_NAME`, and the per-pod DNS the headless Service gives for routing (above). `replicas` must equal the topic's partition count.

The probe setup is deliberate, not boilerplate: `livenessProbe` hits `/healthz` (unconditional 200) with a short budget, so only a truly hung process gets killed. `readinessProbe` and `startupProbe` both hit `/readyz` (real consumer lag), but `startupProbe` gets a much larger failure budget — until it succeeds, Kubernetes doesn't evaluate liveness at all, so a shard replaying its entire partition from scratch (cold start, no usable snapshot) has room to catch up instead of getting killed mid-replay. Tune `startupProbe.failureThreshold` to how much data a full replay actually takes in your environment.

## Backups: retention & manual point-in-time restore

Backups accumulate at roughly one object per shard per `snapshot.backup_interval` (default 1h) forever unless bounded. `minio.backup_retention_days` (default 30, 0 disables it) sets a bucket lifecycle rule that expires them server-side — see `internal/snapshot/minio.go`. It's one rule for the whole bucket rather than one per shard: every object in this bucket is a backup, and `SetBucketLifecycle` *replaces* the whole config, so per-shard rules would mean whichever shard's startup runs last silently wins and drops every other shard's retention.

`Load()` always restores from the *latest* backup. To inspect or restore from an older one instead, `MinioStore` exposes `ListBackups(ctx)` (oldest first) and `LoadNamed(ctx, name)`. There's no runtime flag wired up to roll a live shard back to an arbitrary backup on startup — that's left as a manual procedure (list backups, pick one, decode and inspect via those two methods, or `mc cp`/`mc cat` the object directly) rather than a config knob, since a knob like that is easy to leave set and accidentally roll back state on the next unrelated restart.

## Operability signals

- `post_stats_last_backup_unix_seconds` — Unix timestamp of the last successful MinIO backup. The most important gauge here: alert on `time() - post_stats_last_backup_unix_seconds` exceeding a few multiples of `snapshot.backup_interval`. This is what catches "backups have been silently failing for three days," which is otherwise invisible until the next restart forces a full Kafka replay.
- `post_stats_events_skipped_total` — see the partitioning contract above.
- `post_stats_events_duplicate_total` — EventIDs seen more than once (producer retries, at-least-once redelivery). Expected to be non-zero occasionally; a sudden spike is worth investigating on the producer side.
- `post_stats_kafka_lag` — records behind the partition's high watermark; also drives `/readyz`. Refreshed independently of incoming traffic (`lagRefreshInterval` in `internal/transport/kafka/consumer.go`), so an idle-but-caught-up shard still reports ready instead of getting stuck on stale "unknown lag" from before its last restart.

## Configuration

See `configs/config.yml` for defaults; every key can be overridden via environment variable (`snapshot.backup_interval` → `SNAPSHOT_BACKUP_INTERVAL`, etc.) or a local `.env` file. `log.level` controls verbosity (`debug`/`info`/`warn`/`error`) without a rebuild.
