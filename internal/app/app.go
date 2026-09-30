package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"

	"github.com/IBKnight/posts-statistic-service/internal/metrics"
	"github.com/IBKnight/posts-statistic-service/internal/snapshot"
	"github.com/IBKnight/posts-statistic-service/internal/storage"
	"github.com/IBKnight/posts-statistic-service/internal/transport/httptransport"
	"github.com/IBKnight/posts-statistic-service/internal/transport/kafka"
)

func Init() error {
	if err := godotenv.Load(); err != nil {
		slog.Debug("no .env file, reading config from environment")
	}

	if err := initConfig(); err != nil {
		return fmt.Errorf("error occured while configs init: %w", err)
	}

	configureLogging()

	ordinal, err := shardOrdinal()
	if err != nil {
		return fmt.Errorf("error occured while resolving shard ordinal: %w", err)
	}

	store := storage.NewForShard(ordinal)

	slog.Info("shard resolved",
		"ordinal", ordinal,
		"base_id", store.BaseID(),
		"max_id", store.MaxID(),
	)

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStartup()

	snaps, err := snapshot.NewMinioStore(startupCtx, snapshot.MinioConfig{
		Endpoint:            viper.GetString("minio.endpoint"),
		AccessKey:           viper.GetString("minio.access_key"),
		SecretKey:           viper.GetString("minio.secret_key"),
		Bucket:              viper.GetString("minio.bucket"),
		UseSSL:              viper.GetBool("minio.use_ssl"),
		Ordinal:             ordinal,
		BackupRetentionDays: viper.GetInt("minio.backup_retention_days"),
	})
	if err != nil {
		return fmt.Errorf("error occured while minio init: %w", err)
	}

	cons, err := kafka.New(kafka.Config{
		Brokers:        viper.GetStringSlice("kafka.brokers"),
		Topic:          viper.GetString("kafka.topic"),
		Partition:      int32(ordinal),
		BackupInterval: viper.GetDuration("snapshot.backup_interval"),
		LagThreshold:   viper.GetInt64("kafka.lag_threshold"),
	}, store, snaps)
	if err != nil {
		return fmt.Errorf("error occured while consumer init: %w", err)
	}

	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})

	go func() {
		defer close(consumerDone)

		if err := cons.Run(consumerCtx); err != nil {
			slog.Error("error occured while running consumer", "err", err)
		}
	}()

	port := viper.GetString("port")
	internalPort := viper.GetString("internal_port")

	h := httptransport.NewHandler(store, cons)

	srv := NewServer(port, h.InitPublicRoutes())

	internalMux := h.InitInternalRoutes()
	internalMux.Handle("/metrics", metrics.Handler(ordinal, store, cons))
	internalSrv := NewServer(internalPort, internalMux)

	go func() {
		if err := srv.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("error occured while running http server", "err", err)
		}
	}()

	go func() {
		if err := internalSrv.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("error occured while running internal http server", "err", err)
		}
	}()

	slog.Info("posts statistic service started", "port", port, "internal_port", internalPort)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	slog.Info("posts statistic service shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("error occured on server shutting down", "err", err)
	}

	if err := internalSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("error occured on internal server shutting down", "err", err)
	}

	stopConsumer()
	<-consumerDone

	slog.Info("posts statistic service stopped",
		"applied", cons.Applied(),
		"skipped", cons.Skipped(),
		"duplicate", cons.Duplicate(),
		"failed", cons.Failed(),
	)

	return nil
}

func shardOrdinal() (int, error) {
	if viper.IsSet("shard.ordinal") {
		return viper.GetInt("shard.ordinal"), nil
	}

	name := os.Getenv("POD_NAME")
	if name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return 0, fmt.Errorf("read hostname: %w", err)
		}
		name = hostname
	}

	idx := strings.LastIndex(name, "-")
	if idx == -1 {
		return 0, fmt.Errorf("cannot parse ordinal from %q", name)
	}

	ordinal, err := strconv.Atoi(name[idx+1:])
	if err != nil {
		return 0, fmt.Errorf("cannot parse ordinal from %q: %w", name, err)
	}

	if ordinal < 0 {
		return 0, fmt.Errorf("negative ordinal in %q", name)
	}

	return ordinal, nil
}

func configureLogging() {
	level := slog.LevelInfo

	if raw := viper.GetString("log.level"); raw != "" {
		if err := level.UnmarshalText([]byte(raw)); err != nil {
			slog.Warn("unknown log level, defaulting to info", "level", raw)
			level = slog.LevelInfo
		}
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}

func initConfig() error {
	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	return viper.ReadInConfig()
}
