package config

import (
	"fmt"
	"os"
)

type Config struct {
	KafkaBrokers               string
	KafkaExecuteTopic          string
	KafkaAudienceTopic         string
	KafkaOutboundTopic         string
	KafkaDeadletterTopic       string
	RedisAddr                  string
	HTTPPort                   string
	AudienceGracePeriodSeconds int
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:         getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaExecuteTopic:    getEnv("KAFKA_EXECUTE_TOPIC", "campaign-executor.execute"),
		KafkaAudienceTopic:   getEnv("KAFKA_AUDIENCE_TOPIC", "campaign.audience"),
		KafkaOutboundTopic:   getEnv("KAFKA_OUTBOUND_TOPIC", "notifications.outbound"),
		KafkaDeadletterTopic: getEnv("KAFKA_DEADLETTER_TOPIC", "campaign.deadletter"),
		RedisAddr:            getEnv("REDIS_ADDR", "localhost:6379"),
		HTTPPort:             getEnv("HTTP_PORT", "8080"),
	}

	if cfg.KafkaBrokers == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS must not be empty")
	}

	cfg.AudienceGracePeriodSeconds = 60 // default, could be made configurable later

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
