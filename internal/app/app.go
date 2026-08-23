package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

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

	port := viper.GetString("port")

	h := &httptransport.Handler{}

	srv := NewServer(port, h.InitRoutes())

	go func() {
		if err := srv.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("error occured while running http server: ", "err", err)
		}
	}()

	slog.Info("post stats service started")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	slog.Info("post stats service shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("error occured on server shutting down: ", "err", err)
	}

	slog.Info("post stats service stopped")

	return nil
}

func initConfig() error {
	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	return viper.ReadInConfig()
}
