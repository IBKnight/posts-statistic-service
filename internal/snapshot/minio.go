package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const contentType = "application/octet-stream"

type MinioConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	Ordinal   int
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
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create bucket %q: %w", cfg.Bucket, err)
		}
	}

	prefix := fmt.Sprintf("shard-%d", cfg.Ordinal)

	return &MinioStore{
		client:       client,
		bucket:       cfg.Bucket,
		backupPrefix: prefix + "/backups/",
	}, nil
}

func (s *MinioStore) Backup(ctx context.Context, data []byte) error {
	name := s.backupPrefix + time.Now().UTC().Format("20060102T150405Z") + ".bin"
	return s.put(ctx, name, data)
}

// Load restores from the most recent backup. Backups are the only objects
// this service ever writes to MinIO, so they are also the only restore source.
func (s *MinioStore) Load(ctx context.Context) (State, error) {
	name, err := s.latestBackup(ctx)
	if err != nil {
		return State{}, err
	}

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

// latestBackup finds the newest backup key. Backup names are a fixed-width
// UTC timestamp ("20060102T150405Z.bin"), so lexical order is chronological order.
func (s *MinioStore) latestBackup(ctx context.Context) (string, error) {
	var latest string

	for object := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    s.backupPrefix,
		Recursive: true,
	}) {
		if object.Err != nil {
			return "", translate(object.Err)
		}
		if object.Key > latest {
			latest = object.Key
		}
	}

	if latest == "" {
		return "", ErrNotFound
	}

	return latest, nil
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
