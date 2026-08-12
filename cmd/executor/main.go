// Command executor wires together config, the registry, and the
// campaign-executor.execute consumer, then runs until shutdown.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"campaign-executor/internal/command"
	"campaign-executor/internal/config"
	"campaign-executor/internal/registry"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("executor: config load failed", "error", err)
		os.Exit(1)
	}

	reg := registry.New(cfg.RedisAddr)
	brokers := strings.Split(cfg.KafkaBrokers, ",")
	consumer := command.New(brokers, cfg.KafkaExecuteTopic, reg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("executor: starting command consumer",
		"topic", cfg.KafkaExecuteTopic,
		"brokers", cfg.KafkaBrokers)

	err = consumer.Run(ctx)

	slog.Info("executor: command consumer stopped", "error", err)
}
