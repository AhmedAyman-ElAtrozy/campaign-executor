// Package command consumes campaign-executor.execute commands and
// registers each campaign's window with the registry.
package command

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"campaign-executor/internal/registry"

	"github.com/segmentio/kafka-go"
)

// ExecuteCommand is a campaign window command as received on the
// campaign-executor.execute topic.
type ExecuteCommand struct {
	CampaignID string    `json:"campaignId"`
	MessageID  string    `json:"messageId"`
	ExecuteAt  time.Time `json:"executeAt"`
	HardStopAt time.Time `json:"hardStopAt"`
}

// Consumer reads ExecuteCommand messages from Kafka and registers the
// campaign window described by each one.
type Consumer struct {
	reader *kafka.Reader
	reg    *registry.Registry
}

// New builds a Consumer reading topic from brokers under the
// campaign-executor-execute consumer group, registering commands into reg.
func New(brokers []string, topic string, reg *registry.Registry) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: "campaign-executor-execute",
		}),
		reg: reg,
	}
}

// Run reads and registers commands until ctx is cancelled. A single
// malformed or unregisterable message is logged and skipped rather than
// stopping the loop; ctx cancellation is treated as a clean exit.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("command: read message failed", "error", err)
			continue
		}

		var cmd ExecuteCommand
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			slog.Error("command: unmarshal failed", "error", err)
			continue
		}

		state := &registry.CampaignState{
			CampaignID: cmd.CampaignID,
			MessageID:  cmd.MessageID,
			ExecuteAt:  cmd.ExecuteAt,
			HardStopAt: cmd.HardStopAt,
		}

		if err := c.reg.Register(ctx, state); err != nil {
			slog.Error("command: register failed",
				"campaignId", cmd.CampaignID,
				"messageId", cmd.MessageID,
				"error", err)
			continue
		}

		slog.Info("command: registered campaign",
			"campaignId", cmd.CampaignID,
			"messageId", cmd.MessageID)
	}
}
