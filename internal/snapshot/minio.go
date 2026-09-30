package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

const contentType = "application/octet-stream"

type MinioConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	Ordinal   int

	// BackupRetentionDays, if set, expires backup objects older than this
	// via a bucket lifecycle rule (server-side S3/MinIO expiry, not a
	// hand-rolled delete loop). 0 disables it — backups accumulate forever.
	BackupRetentionDays int
}

type MinioStore struct {
	client       *minio.Client
	bucket       string
	backupPrefix string
}

func NewMinioStore(ctx context.Context, cfg MinioConfig) (*MinioStore, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket %q: %w", cfg.Bucket, err)
	}

	if !exists {
		// Every shard races to create the same bucket on startup (there's no
		// leader election here). The loser's MakeBucket fails with
		// "already own it" / "already exists" — that's success, not an error.
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			switch minio.ToErrorResponse(err).Code {
			case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			default:
				return nil, fmt.Errorf("create bucket %q: %w", cfg.Bucket, err)
			}
		}
	}

	prefix := fmt.Sprintf("shard-%d", cfg.Ordinal)
	backupPrefix := prefix + "/backups/"

	if cfg.BackupRetentionDays > 0 {
		// SetBucketLifecycle *replaces* the whole config, and every shard
		// calls this on its own startup with no coordination between them.
		// A per-shard prefix rule would mean whichever shard starts last
		// wins and silently drops every other shard's retention rule. Every
		// object in this bucket is a backup (nothing else is ever written
		// here), so one bucket-wide rule with a fixed ID covers all shards
		// identically — concurrent writers converge on the same config
		// instead of clobbering each other.
		lc := &lifecycle.Configuration{
			Rules: []lifecycle.Rule{
				{
					ID:         "expire-backups",
					Status:     "Enabled",
					RuleFilter: lifecycle.Filter{Prefix: ""},
					Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(cfg.BackupRetentionDays)},
				},
			},
		}
		if err := client.SetBucketLifecycle(ctx, cfg.Bucket, lc); err != nil {
			return nil, fmt.Errorf("set backup retention lifecycle: %w", err)
		}
	}

	return &MinioStore{
		client:       client,
		bucket:       cfg.Bucket,
		backupPrefix: backupPrefix,
	}, nil
}

func (s *MinioStore) Backup(ctx context.Context, data []byte) error {
	name := s.backupPrefix + time.Now().UTC().Format("20060102T150405Z") + ".bin"
	return s.put(ctx, name, data)
}

// Load restores from the most recent backup. Backups are the only objects
// this service ever writes to MinIO, so they are also the only restore source.
func (s *MinioStore) Load(ctx context.Context) (State, error) {
	keys, err := s.ListBackups(ctx)
	if err != nil {
		return State{}, err
	}
	if len(keys) == 0 {
		return State{}, ErrNotFound
	}

	return s.LoadNamed(ctx, keys[len(keys)-1])
}

// ListBackups returns this shard's backup object keys, oldest first. Backup
// names are a fixed-width UTC timestamp ("shard-N/backups/20060102T150405Z.bin"),
// so lexical order is chronological order. Pass any of these to LoadNamed to
// restore from a specific point in time instead of the latest — see README
// for the manual recovery procedure.
func (s *MinioStore) ListBackups(ctx context.Context) ([]string, error) {
	var keys []string

	for object := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    s.backupPrefix,
		Recursive: true,
	}) {
		if object.Err != nil {
			return nil, translate(object.Err)
		}
		keys = append(keys, object.Key)
	}

	sort.Strings(keys)

	return keys, nil
}

// LoadNamed loads and decodes one specific backup object, e.g. a key
// returned by ListBackups. Unlike Load, it does not pick "latest" for you.
func (s *MinioStore) LoadNamed(ctx context.Context, name string) (State, error) {
	object, err := s.client.GetObject(ctx, s.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		return State{}, translate(err)
	}
	defer object.Close()

	data, err := io.ReadAll(object)
	if err != nil {
		return State{}, translate(err)
	}

	return Decode(data)
}

func (s *MinioStore) put(ctx context.Context, name string, data []byte) error {
	_, err := s.client.PutObject(
		ctx,
		s.bucket,
		name,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType},
	)
	if err != nil {
		return fmt.Errorf("put %q: %w", name, err)
	}

	return nil
}

func translate(err error) error {
	switch minio.ToErrorResponse(err).Code {
	case "NoSuchKey", "NoSuchBucket":
		return ErrNotFound
	default:
		return err
	}
}
