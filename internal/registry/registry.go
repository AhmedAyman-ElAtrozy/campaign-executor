package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// channelPriority is the fixed internal order in which channels are
// considered for quota assignment.
var channelPriority = []string{"sms", "whatsapp", "email"}

// CampaignState holds the fields from an ExecuteCommand that audience
// consumers need before they can process records for a campaign.
type CampaignState struct {
	CampaignID       string         `json:"campaignId"`
	MessageID        string         `json:"messageId"`
	ExecuteAt        time.Time      `json:"executeAt"`
	HardStopAt       time.Time      `json:"hardStopAt"`
	ChannelRemaining map[string]int `json:"channelRemaining"`
	Processed        int            `json:"processed"`
	TotalCount       int            `json:"totalCount"`
	Completed        bool           `json:"completed"`
}

// Registry keeps an in-memory map of CampaignState entries backed by Redis
// so that audience partitions can race against the command consumer.
type Registry struct {
	mu           sync.RWMutex
	states       map[string]*CampaignState
	seenMessages map[string]bool
	redisClient  *redis.Client
}

func New(redisAddr string) *Registry {
	return &Registry{
		states:       make(map[string]*CampaignState),
		seenMessages: make(map[string]bool),
		redisClient:  redis.NewClient(&redis.Options{Addr: redisAddr}),
	}
}

// Register writes state to the in-memory map first, then to Redis.
// TTL is set to (hardStopAt - now) + 5 min so Redis auto-expires stale entries.
// Idempotent on messageId: a redelivered command with a messageId already
// seen for its campaignId is logged and skipped rather than overwriting
// the existing state, per the architecture doc's requirement that
// Register be idempotent on messageId.
func (reg *Registry) Register(ctx context.Context, state *CampaignState) error {
	dedupKey := state.CampaignID + ":" + state.MessageID

	reg.mu.Lock()
	if reg.seenMessages[dedupKey] {
		reg.mu.Unlock()
		slog.Info("registry: duplicate command, skipping",
			"campaignId", state.CampaignID,
			"messageId", state.MessageID)
		return nil
	}
	reg.states[state.CampaignID] = state
	reg.seenMessages[dedupKey] = true
	reg.mu.Unlock()

	stateBytes, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("registry: marshal state for %s: %w", state.CampaignID, err)
	}

	if err := reg.redisClient.Set(ctx, redisKey(state.CampaignID), stateBytes, ttlFor(state.HardStopAt)).Err(); err != nil {
		return fmt.Errorf("registry: redis set %s: %w", state.CampaignID, err)
	}
	return nil
}

// WaitFor polls every 500 ms until the CampaignState for campaignID is
// available (checking in-memory map then Redis) or the grace period elapses.
func (reg *Registry) WaitFor(ctx context.Context, campaignID string, grace time.Duration) (*CampaignState, error) {
	deadline := time.Now().Add(grace)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		// Fast path: in-memory map (same process).
		reg.mu.RLock()
		found := reg.states[campaignID]
		reg.mu.RUnlock()
		if found != nil {
			return found, nil
		}

		// Slow path: Redis (command may have been registered by another pod).
		stateBytes, err := reg.redisClient.Get(ctx, redisKey(campaignID)).Bytes()
		if err == nil {
			var state CampaignState
			if jsonErr := json.Unmarshal(stateBytes, &state); jsonErr == nil {
				reg.mu.Lock()
				reg.states[campaignID] = &state
				reg.mu.Unlock()
				return &state, nil
			}
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("registry: campaign %s not registered within %s grace period", campaignID, grace)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// AssignChannel picks the next available channel for a customer reachable
// via the given contact methods, in fixed priority order (sms, whatsapp,
// email), decrementing that channel's remaining quota. It returns
// ("", false) if no eligible channel has quota left.
func (reg *Registry) AssignChannel(campaignID string, hasPhone, hasEmail bool) (string, bool) {
	var candidates []string
	if hasPhone {
		candidates = append(candidates, "sms", "whatsapp")
	}
	if hasEmail {
		candidates = append(candidates, "email")
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()

	state := reg.states[campaignID]
	if state == nil {
		return "", false
	}

	for _, want := range channelPriority {
		for _, candidate := range candidates {
			if candidate != want {
				continue
			}
			if state.ChannelRemaining[candidate] > 0 {
				state.ChannelRemaining[candidate]--
				reg.persistToRedis(state)
				return candidate, true
			}
		}
	}
	return "", false
}

// SetTotalCount records the total audience size for a campaign, as learned
// from an END_OF_AUDIENCE record, so RecordOutcome can detect completion.
// It also reports whether the campaign is already complete at this moment
// (i.e. every record was already processed before the total was known).
func (reg *Registry) SetTotalCount(campaignID string, totalCount int) bool {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	state := reg.states[campaignID]
	if state == nil {
		return false
	}
	state.TotalCount = totalCount
	finished := checkCompleted(state)
	reg.persistToRedis(state)
	return finished
}

// RecordOutcome increments the processed count for a campaign and reports
// whether this call caused it to reach a previously-set TotalCount. It
// returns false if TotalCount is not yet known (zero).
func (reg *Registry) RecordOutcome(campaignID string) bool {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	state := reg.states[campaignID]
	if state == nil {
		return false
	}
	state.Processed++
	finished := checkCompleted(state)
	reg.persistToRedis(state)
	return finished
}

// checkCompleted reports whether state has just reached completion
// (Processed equals a known, nonzero TotalCount), marking state.Completed
// if so. Callers must hold reg.mu.
func checkCompleted(state *CampaignState) bool {
	if state.TotalCount != 0 && state.Processed == state.TotalCount {
		state.Completed = true
		return true
	}
	return false
}

// IsCompleted safely reports whether campaignID has been marked complete,
// returning false if the campaign isn't found.
func (reg *Registry) IsCompleted(campaignID string) bool {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	state := reg.states[campaignID]
	if state == nil {
		return false
	}
	return state.Completed
}

// ttlFor computes the Redis TTL for a campaign's state: time remaining
// until hardStopAt plus a 5 minute buffer, so entries auto-expire shortly
// after their campaign window closes.
func ttlFor(hardStopAt time.Time) time.Duration {
	ttl := time.Until(hardStopAt) + 5*time.Minute
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return ttl
}

// persistToRedis marshals state and writes it to Redis under
// redisKey(state.CampaignID), reusing the same TTL logic as Register. It
// uses a short-lived background context so a slow or unavailable Redis
// can't block audience processing; failures are logged, not returned,
// matching the rest of this package's tolerance for Redis outages -- the
// in-memory map stays authoritative regardless. Callers must hold reg.mu.
func (reg *Registry) persistToRedis(state *CampaignState) {
	stateBytes, err := json.Marshal(state)
	if err != nil {
		slog.Error("registry: marshal state for redis persist failed",
			"campaignId", state.CampaignID,
			"error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := reg.redisClient.Set(ctx, redisKey(state.CampaignID), stateBytes, ttlFor(state.HardStopAt)).Err(); err != nil {
		slog.Error("registry: redis persist failed",
			"campaignId", state.CampaignID,
			"error", err)
	}
}

func redisKey(campaignID string) string {
	return "campaign:state:" + campaignID
}
