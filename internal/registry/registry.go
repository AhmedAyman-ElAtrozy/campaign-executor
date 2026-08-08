package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// CampaignState holds the fields from an ExecuteCommand that audience
// consumers need before they can process records for a campaign.
type CampaignState struct {
	CampaignID string    `json:"campaignId"`
	MessageID  string    `json:"messageId"`
	ExecuteAt  time.Time `json:"executeAt"`
	HardStopAt time.Time `json:"hardStopAt"`
}

// Registry keeps an in-memory map of CampaignState entries backed by Redis
// so that audience partitions can race against the command consumer.
type Registry struct {
	mu          sync.RWMutex
	states      map[string]*CampaignState
	redisClient *redis.Client
}

func New(redisAddr string) *Registry {
	return &Registry{
		states:      make(map[string]*CampaignState),
		redisClient: redis.NewClient(&redis.Options{Addr: redisAddr}),
	}
}

// Register writes state to the in-memory map first, then to Redis.
// TTL is set to (hardStopAt - now) + 5 min so Redis auto-expires stale entries.
func (reg *Registry) Register(ctx context.Context, state *CampaignState) error {
	reg.mu.Lock()
	reg.states[state.CampaignID] = state
	reg.mu.Unlock()

	stateBytes, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("registry: marshal state for %s: %w", state.CampaignID, err)
	}

	ttl := time.Until(state.HardStopAt) + 5*time.Minute
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	if err := reg.redisClient.Set(ctx, redisKey(state.CampaignID), stateBytes, ttl).Err(); err != nil {
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

func redisKey(campaignID string) string {
	return "campaign:state:" + campaignID
}
