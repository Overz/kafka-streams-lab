package main

import (
	"api/internal/config"
	"api/internal/gateway"
	"api/internal/platform"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger := config.NewLogger(cfg.Env, cfg.LogLevel)
	slog.SetDefault(logger)

	// Signals are the root of every startup context so a Ctrl+C during a stalled
	// broker dial exits at once instead of waiting out the timeout below.
	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer stopSignals()

	startupCtx, cancelStartup := context.WithTimeout(signalCtx, 90*time.Second)
	defer cancelStartup()

	if err = platform.KafkaConnect(startupCtx, cfg.Kafka.Brokers); err != nil {
		slog.Error("failed to connect to kafka", "error", err)
		os.Exit(1)
	}

	// One publisher for the whole API. The topic registry maps the logical
	// topics handlers know about to the real names coming from the environment.
	publisher, err := platform.NewKafkaPublisher(platform.KafkaConfig{
		Brokers:      cfg.Kafka.Brokers,
		WriteTimeout: cfg.Kafka.WriteTimeout,
		Topics: map[platform.Topic]string{
			platform.TopicOrderCreated: cfg.Kafka.Topics.Order.Topic,
		},
	})
	if err != nil {
		slog.Error("failed to create kafka publisher", "error", err)
		os.Exit(1)
	}

	defer func() {
		if closeErr := publisher.KafkaCloseConnection(); closeErr != nil {
			slog.Error("failed to close kafka publisher", "error", closeErr)
		}
	}()

	handlers := gateway.NewHandler(publisher)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", handlers.HealthCheckHandler)
	mux.HandleFunc("POST /api/orders", handlers.PostOrder)

	server := &http.Server{
		Addr:    ":" + cfg.HTTP.Port,
		Handler: mux,
	}

	go func() {
		slog.Info("http server listening", "port", cfg.HTTP.Port)

		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			slog.Error("http server failed", "error", serveErr)
			stopSignals()
		}
	}()

	<-signalCtx.Done()
	stopSignals()

	slog.Info("shutting down")

	// Background, not signalCtx: that one is already done — the signal is what
	// got us here — and a child of a done context would expire immediately,
	// turning the graceful drain into an abrupt close.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancelShutdown()

	if err = server.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shut down http server", "error", err)
	}
}
