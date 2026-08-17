package registry

import (
	"context"
	"testing"
	"time"
)

func TestRegisterAndWaitFor(t *testing.T) {
	// NOTE: this needs a real Redis running on localhost:6379
	// (docker compose up -d) since Register() writes to Redis.
	reg := New("localhost:6379")

	fakeState := &CampaignState{
		CampaignID: "cmp_test_001",
		MessageID:  "msg_fake",
		ExecuteAt:  time.Now(),
		HardStopAt: time.Now().Add(1 * time.Hour),
	}

	ctx := context.Background()

	// Step 1: register it (writes to map + Redis)
	if err := reg.Register(ctx, fakeState); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Step 2: WaitFor should find it INSTANTLY via the map, no waiting
	found, err := reg.WaitFor(ctx, "cmp_test_001", 5*time.Second)
	if err != nil {
		t.Fatalf("WaitFor failed: %v", err)
	}
	if found.CampaignID != "cmp_test_001" {
		t.Errorf("got wrong campaign back: %s", found.CampaignID)
	}

	// Step 3: WaitFor on something that was NEVER registered
	// should time out and return an error after the grace period
	_, err = reg.WaitFor(ctx, "cmp_does_not_exist", 1*time.Second)
	if err == nil {
		t.Error("expected an error for unknown campaign, got nil")
	}
}

func TestRegisterIsIdempotentOnMessageID(t *testing.T) {
	// NOTE: this needs a real Redis running on localhost:6379
	// (docker compose up -d) since Register() writes to Redis.
	reg := New("localhost:6379")

	ctx := context.Background()
	originalHardStop := time.Now().Add(1 * time.Hour)

	first := &CampaignState{
		CampaignID: "cmp_test_dedup",
		MessageID:  "msg_dup",
		ExecuteAt:  time.Now(),
		HardStopAt: originalHardStop,
	}
	if err := reg.Register(ctx, first); err != nil {
		t.Fatalf("first Register failed: %v", err)
	}

	redelivered := &CampaignState{
		CampaignID: "cmp_test_dedup",
		MessageID:  "msg_dup",
		ExecuteAt:  time.Now(),
		HardStopAt: time.Now().Add(2 * time.Hour),
	}
	if err := reg.Register(ctx, redelivered); err != nil {
		t.Fatalf("second Register (duplicate messageId) returned error: %v", err)
	}

	found, err := reg.WaitFor(ctx, "cmp_test_dedup", 5*time.Second)
	if err != nil {
		t.Fatalf("WaitFor failed: %v", err)
	}
	if !found.HardStopAt.Equal(originalHardStop) {
		t.Errorf("duplicate messageId overwrote state: got HardStopAt %v, want %v", found.HardStopAt, originalHardStop)
	}
}
