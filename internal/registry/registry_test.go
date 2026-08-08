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
