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

func TestAssignChannel(t *testing.T) {
	// NOTE: this needs a real Redis running on localhost:6379
	// (docker compose up -d) since Register() writes to Redis.
	reg := New("localhost:6379")
	ctx := context.Background()

	state := &CampaignState{
		CampaignID:       "cmp_test_channels",
		MessageID:        "msg_channels",
		ExecuteAt:        time.Now(),
		HardStopAt:       time.Now().Add(1 * time.Hour),
		ChannelRemaining: map[string]int{"sms": 1, "whatsapp": 1, "email": 1},
	}
	if err := reg.Register(ctx, state); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// sms has quota and is higher priority than whatsapp for phone contacts.
	channel, ok := reg.AssignChannel("cmp_test_channels", true, false)
	if !ok || channel != "sms" {
		t.Fatalf("AssignChannel(phone) = %q, %v; want sms, true", channel, ok)
	}

	// sms quota is now exhausted, so the next phone contact falls back to whatsapp.
	channel, ok = reg.AssignChannel("cmp_test_channels", true, false)
	if !ok || channel != "whatsapp" {
		t.Fatalf("AssignChannel(phone) after sms exhausted = %q, %v; want whatsapp, true", channel, ok)
	}

	// sms and whatsapp are both exhausted now, so a phone-only contact has no candidate left.
	_, ok = reg.AssignChannel("cmp_test_channels", true, false)
	if ok {
		t.Fatal("expected AssignChannel to fail once sms and whatsapp quota is exhausted")
	}

	// email quota is untouched.
	channel, ok = reg.AssignChannel("cmp_test_channels", false, true)
	if !ok || channel != "email" {
		t.Fatalf("AssignChannel(email) = %q, %v; want email, true", channel, ok)
	}
}

func TestRecordOutcomeAndSetTotalCountMarkCompletion(t *testing.T) {
	// NOTE: this needs a real Redis running on localhost:6379
	// (docker compose up -d) since Register() writes to Redis.
	reg := New("localhost:6379")
	ctx := context.Background()

	state := &CampaignState{
		CampaignID: "cmp_test_completion",
		MessageID:  "msg_completion",
		ExecuteAt:  time.Now(),
		HardStopAt: time.Now().Add(1 * time.Hour),
	}
	if err := reg.Register(ctx, state); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// TotalCount not yet known: RecordOutcome must not report completion.
	if finished := reg.RecordOutcome("cmp_test_completion"); finished {
		t.Fatal("RecordOutcome reported finished before TotalCount was set")
	}
	if reg.IsCompleted("cmp_test_completion") {
		t.Fatal("IsCompleted reported true before completion")
	}

	// Learning TotalCount == Processed should mark completion immediately.
	finished := reg.SetTotalCount("cmp_test_completion", 1)
	if !finished {
		t.Fatal("SetTotalCount should report finished when Processed already equals totalCount")
	}
	if !reg.IsCompleted("cmp_test_completion") {
		t.Fatal("IsCompleted should be true after SetTotalCount detects completion")
	}
}

func TestRecordOutcomeMarksCompletionAfterTotalCountKnown(t *testing.T) {
	// NOTE: this needs a real Redis running on localhost:6379
	// (docker compose up -d) since Register() writes to Redis.
	reg := New("localhost:6379")
	ctx := context.Background()

	state := &CampaignState{
		CampaignID: "cmp_test_completion_2",
		MessageID:  "msg_completion_2",
		ExecuteAt:  time.Now(),
		HardStopAt: time.Now().Add(1 * time.Hour),
	}
	if err := reg.Register(ctx, state); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if finished := reg.SetTotalCount("cmp_test_completion_2", 2); finished {
		t.Fatal("SetTotalCount should not report finished before any records are processed")
	}

	if finished := reg.RecordOutcome("cmp_test_completion_2"); finished {
		t.Fatal("RecordOutcome should not report finished after only 1 of 2 records")
	}
	if reg.IsCompleted("cmp_test_completion_2") {
		t.Fatal("IsCompleted should be false before the total is reached")
	}

	if finished := reg.RecordOutcome("cmp_test_completion_2"); !finished {
		t.Fatal("RecordOutcome should report finished on reaching TotalCount")
	}
	if !reg.IsCompleted("cmp_test_completion_2") {
		t.Fatal("IsCompleted should be true after reaching TotalCount")
	}
}

func TestIsCompletedUnknownCampaign(t *testing.T) {
	reg := New("localhost:6379")
	if reg.IsCompleted("cmp_never_registered") {
		t.Fatal("IsCompleted should be false for an unknown campaign")
	}
}
