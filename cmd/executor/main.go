// Command executor wires together config, the registry, the producer,
// and the execute/audience consumers, then runs them until shutdown.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"campaign-executor/internal/audience"
	"campaign-executor/internal/command"
	"campaign-executor/internal/config"
	"campaign-executor/internal/httpapi"
	"campaign-executor/internal/producer"
	"campaign-executor/internal/registry"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("executor: config load failed", "error", err)
		os.Exit(1)
	}

	brokers := strings.Split(cfg.KafkaBrokers, ",")

	reg := registry.New(cfg.RedisAddr)
	prod := producer.New(brokers, cfg.KafkaOutboundTopic, cfg.KafkaDeadletterTopic)

	cmdConsumer := command.New(brokers, cfg.KafkaExecuteTopic, reg)
	grace := time.Duration(cfg.AudienceGracePeriodSeconds) * time.Second
	audConsumer := audience.New(brokers, cfg.KafkaAudienceTopic, reg, prod, grace)

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	server := httpapi.New(cfg.HTTPPort, reg, redisClient, brokers)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("executor: starting consumers",
		"executeTopic", cfg.KafkaExecuteTopic,
		"audienceTopic", cfg.KafkaAudienceTopic,
		"brokers", cfg.KafkaBrokers)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		err := cmdConsumer.Run(groupCtx)
		slog.Info("executor: command consumer stopped", "error", err)
		return err
	})
	group.Go(func() error {
		err := audConsumer.Run(groupCtx)
		slog.Info("executor: audience consumer stopped", "error", err)
		return err
	})
	group.Go(func() error {
		err := server.Run(groupCtx)
		slog.Info("executor: http server stopped", "error", err)
		return err
	})

	err = group.Wait()

	if closeErr := prod.Close(); closeErr != nil {
		slog.Error("executor: producer close failed", "error", closeErr)
	}

	slog.Info("executor: shutdown complete", "error", err)
}
