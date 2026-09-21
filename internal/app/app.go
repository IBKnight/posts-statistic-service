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
		Endpoint:  viper.GetString("minio.endpoint"),
		AccessKey: viper.GetString("minio.access_key"),
		SecretKey: viper.GetString("minio.secret_key"),
		Bucket:    viper.GetString("minio.bucket"),
		UseSSL:    viper.GetBool("minio.use_ssl"),
		Ordinal:   ordinal,
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

	h := httptransport.NewHandler(store, cons)
	srv := NewServer(port, h.InitRoutes())

	go func() {
		if err := srv.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("error occured while running http server", "err", err)
		}
	}()

	slog.Info("posts statistic service started", "port", port)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	slog.Info("posts statistic service shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("error occured on server shutting down", "err", err)
	}

	stopConsumer()
	<-consumerDone

	applied, skipped, failed := cons.Stats()

	slog.Info("posts statistic service stopped",
		"applied", applied,
		"skipped", skipped,
		"failed", failed,
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

func initConfig() error {
	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	return viper.ReadInConfig()
}
