// Package audience consumes campaign.audience customer records,
// validates and processes each one sequentially, and produces the
// resulting notification or deadletter.
package audience

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"campaign-executor/internal/metrics"
	"campaign-executor/internal/processor"
	"campaign-executor/internal/producer"
	"campaign-executor/internal/registry"

	"github.com/segmentio/kafka-go"
)

// AudienceRecord is one customer record.
type AudienceRecord struct {
	EventType  string         `json:"eventType"`
	CampaignID string         `json:"campaignId"`
	CustomerID string         `json:"customerId"`
	MSISDN     string         `json:"msisdn"`
	Email      string         `json:"email"`
	Language   string         `json:"language"`
	Attributes map[string]any `json:"attributes"`
	TotalCount int            `json:"totalCount"`
}

// Consumer reads AudienceRecord messages from one partition, validates
// each against its campaign window, and produces the outcome.
type Consumer struct {
	reader *kafka.Reader
	reg    *registry.Registry
	prod   *producer.Producer
	grace  time.Duration
}

// New builds a Consumer reading topic from brokers under the
// campaign-executor-audience consumer group, using reg to look up
// campaign windows and prod to produce results.
func New(brokers []string, topic string, reg *registry.Registry, prod *producer.Producer, grace time.Duration) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: "campaign-executor-audience",
		}),
		reg:   reg,
		prod:  prod,
		grace: grace,
	}
}

// Run reads and processes audience records one at a time, in order,
// until ctx is cancelled. A single bad or failing record is logged
// and skipped rather than stopping the loop; ctx cancellation is a
// clean exit.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("audience: read message failed", "error", err)
			continue
		}

		var record AudienceRecord
		if err := json.Unmarshal(msg.Value, &record); err != nil {
			slog.Error("audience: unmarshal failed", "error", err)
			continue
		}

		if record.EventType == "END_OF_AUDIENCE" {
			slog.Info("audience: end of audience",
				"campaignId", record.CampaignID)
			continue
		}

		waitStart := time.Now()
		state, err := c.reg.WaitFor(ctx, record.CampaignID, c.grace)
		metrics.RegistryWaitSeconds.Observe(time.Since(waitStart).Seconds())
		if err != nil {
			c.deadletter(ctx, msg, record.CampaignID, "unknown_campaign")
			continue
		}

		if time.Now().After(state.HardStopAt) {
			metrics.RecordsProcessed.WithLabelValues("skipped").Inc()
			slog.Info("audience: skipped past hard stop",
				"campaignId", record.CampaignID,
				"customerId", record.CustomerID)
			continue
		}

		procRecord := processor.AudienceRecord{
			EventType:  record.EventType,
			CampaignID: record.CampaignID,
			CustomerID: record.CustomerID,
			MSISDN:     record.MSISDN,
			Email:      record.Email,
			Language:   record.Language,
			Attributes: record.Attributes,
			TotalCount: record.TotalCount,
		}

		req, err := processor.Process(procRecord, state.HardStopAt)
		if err != nil {
			c.deadletter(ctx, msg, record.CampaignID, err.Error())
			continue
		}

		if err := c.prod.SendNotification(ctx, req); err != nil {
			slog.Error("audience: send notification failed",
				"campaignId", record.CampaignID,
				"customerId", record.CustomerID,
				"error", err)
			c.deadletter(ctx, msg, record.CampaignID, "send_notification_failed: "+err.Error())
			continue
		}

		metrics.RecordsProcessed.WithLabelValues("ok").Inc()
		slog.Info("audience: notification sent",
			"campaignId", record.CampaignID,
			"customerId", record.CustomerID)
	}
}

// deadletter builds and sends a DeadLetter for a record that failed
// before or during processing, logging if the send itself fails.
func (c *Consumer) deadletter(ctx context.Context, msg kafka.Message, campaignID, lastError string) {
	dl := producer.DeadLetter{
		CampaignID:        campaignID,
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		LastError:         lastError,
		Attempts:          1,
		FailedAt:          time.Now(),
		Snapshot:          msg.Value,
	}
	if err := c.prod.SendDeadLetter(ctx, dl); err != nil {
		slog.Error("audience: send deadletter failed",
			"campaignId", campaignID,
			"error", err)
		return
	}
	metrics.RecordsProcessed.WithLabelValues("deadletter").Inc()
}
