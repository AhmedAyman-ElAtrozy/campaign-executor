package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"campaign-executor/internal/processor"
)

// Producer writes outbound notifications and deadletters to Kafka on
// behalf of the campaign executor; it owns one writer per topic.
type Producer struct {
	outbound   *kafka.Writer
	deadletter *kafka.Writer
}

// New builds a Producer with fully-acked writers for the
// given outbound and deadletter topics.
func New(brokers []string, outboundTopic, deadletterTopic string) *Producer {
	return &Producer{
		outbound:   newWriter(brokers, outboundTopic),
		deadletter: newWriter(brokers, deadletterTopic),
	}
}

// newWriter builds a kafka.Writer that waits for full ISR acknowledgement,
// shared by both the outbound and deadletter writers.
func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
	}
}

// DeadLetter mirrors the campaign.deadletter JSON contract for a
// record that failed to process or produce.
type DeadLetter struct {
	CampaignID        string    `json:"campaignId"`
	OriginalTopic     string    `json:"originalTopic"`
	OriginalPartition int       `json:"originalPartition"`
	OriginalOffset    int64     `json:"originalOffset"`
	LastError         string    `json:"lastError"`
	Attempts          int       `json:"attempts"`
	FailedAt          time.Time `json:"failedAt"`
	Snapshot          []byte    `json:"snapshot"`
}

// SendNotification marshals a NotificationRequest to JSON and writes it
// to the outbound topic, keyed by customer ID.
func (p *Producer) SendNotification(ctx context.Context, req *processor.NotificationRequest) error {
	value, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("producer: marshal notification for customer %s: %w", req.CustomerID, err)
	}
	if err := p.outbound.WriteMessages(ctx, kafka.Message{
		Key:   []byte(req.CustomerID),
		Value: value,
	}); err != nil {
		return fmt.Errorf("producer: write notification for customer %s: %w", req.CustomerID, err)
	}
	return nil
}

// SendDeadLetter marshals a DeadLetter to JSON and writes it to the
// deadletter topic, keyed by campaign ID.
func (p *Producer) SendDeadLetter(ctx context.Context, dl DeadLetter) error {
	value, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("producer: marshal deadletter for campaign %s: %w", dl.CampaignID, err)
	}
	if err := p.deadletter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(dl.CampaignID),
		Value: value,
	}); err != nil {
		return fmt.Errorf("producer: write deadletter for campaign %s: %w", dl.CampaignID, err)
	}
	return nil
}

// Close shuts down both the outbound and deadletter Kafka writers.
func (p *Producer) Close() error {
	if err := p.outbound.Close(); err != nil {
		return fmt.Errorf("producer: close outbound writer: %w", err)
	}
	if err := p.deadletter.Close(); err != nil {
		return fmt.Errorf("producer: close deadletter writer: %w", err)
	}
	return nil
}
