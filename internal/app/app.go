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

	"github.com/IBKnight/posts-statistic-service/internal/storage"
	"github.com/IBKnight/posts-statistic-service/internal/transport/httptransport"
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

func Init() error {
	if err := godotenv.Load(); err != nil {
		slog.Debug("no .env file, reading config from environment")
	}

	if err := initConfig(); err != nil {
		return fmt.Errorf("error occured while configs init: %s", err.Error())
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

	port := viper.GetString("port")

	h := httptransport.NewHandler(store, httptransport.AlwaysReady())

	srv := NewServer(port, h.InitRoutes())

	go func() {
		if err := srv.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("error occured while running http server: ", "err", err)
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
		slog.Error("error occured on server shutting down: ", "err", err)
	}

	slog.Info("posts statistic service stopped")

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
