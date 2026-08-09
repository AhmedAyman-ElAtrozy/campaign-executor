package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"campaign-executor/internal/processor"
	"campaign-executor/internal/producer"
	"campaign-executor/internal/registry"
)

// TestFullFlow wires registry, processor, and producer together to
// simulate one campaign record moving end to end, without any Kafka
// consumer code in the loop.
func TestFullFlow(t *testing.T) {
	ctx := context.Background()

	// Step 1: registry pointed at local Redis.
	reg := registry.New("localhost:6379")
	t.Log("step 1: created registry pointed at localhost:6379")

	// Step 2: register a fake CampaignState with hardStopAt one hour out.
	hardStopAt := time.Now().Add(time.Hour)
	state := &registry.CampaignState{
		CampaignID: "cmp_test_999",
		MessageID:  "msg_test_999",
		ExecuteAt:  time.Now(),
		HardStopAt: hardStopAt,
	}
	if err := reg.Register(ctx, state); err != nil {
		t.Fatalf("registry.Register failed: %v", err)
	}
	t.Logf("step 2: registered campaign state: %+v", state)

	// Step 3: WaitFor should hit the in-memory fast path and return instantly.
	waitStart := time.Now()
	found, err := reg.WaitFor(ctx, "cmp_test_999", 5*time.Second)
	waitElapsed := time.Since(waitStart)
	if err != nil {
		t.Fatalf("registry.WaitFor failed: %v", err)
	}
	t.Logf("step 3: WaitFor returned in %s: %+v", waitElapsed, found)
	if waitElapsed > 100*time.Millisecond {
		t.Fatalf("expected WaitFor to return instantly from the in-memory map, took %s", waitElapsed)
	}

	// Step 4: build a fake AudienceRecord and process it.
	record := processor.AudienceRecord{
		EventType:  "audience.record",
		CampaignID: "cmp_test_999",
		CustomerID: "cust_test_1",
		MSISDN:     "+201012345678",
		Email:      "test@example.com",
		Language:   "en",
		Attributes: map[string]any{"firstName": "Test"},
		TotalCount: 1,
	}
	t.Logf("step 4a: built audience record: %+v", record)

	notification, err := processor.Process(record, found.HardStopAt)
	if err != nil {
		t.Fatalf("processor.Process failed: %v", err)
	}
	notificationJSON, err := json.MarshalIndent(notification, "", "  ")
	if err != nil {
		t.Fatalf("marshal notification failed: %v", err)
	}
	t.Logf("step 4b: processed notification request:\n%s", notificationJSON)

	// Step 5: producer pointed at local Kafka, send the notification.
	p := producer.New([]string{"localhost:9092"}, "notifications.outbound", "campaign.deadletter")
	defer p.Close()
	t.Log("step 5a: created producer pointed at localhost:9092")

	if err := p.SendNotification(ctx, notification); err != nil {
		t.Fatalf("producer.SendNotification failed: %v", err)
	}
	t.Log("step 5b: SendNotification succeeded")
}
