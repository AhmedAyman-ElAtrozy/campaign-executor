package processor

import (
	"testing"
	"time"
)

func TestProcess(t *testing.T) {
	hardStop := time.Now().Add(time.Hour)

	tests := []struct {
		name    string
		record  AudienceRecord
		wantErr bool
		wantKey string
	}{
		{
			name: "valid record with E.164 msisdn",
			record: AudienceRecord{
				CampaignID: "camp-1",
				CustomerID: "cust-1",
				MSISDN:     "+15551234567",
				Language:   "en",
				Attributes: map[string]any{"tier": "gold"},
			},
			wantKey: "camp-1:cust-1",
		},
		{
			name: "valid record with email only",
			record: AudienceRecord{
				CampaignID: "camp-1",
				CustomerID: "cust-2",
				Email:      "user@example.com",
			},
			wantKey: "camp-1:cust-2",
		},
		{
			name: "invalid msisdn not E.164",
			record: AudienceRecord{
				CampaignID: "camp-1",
				CustomerID: "cust-3",
				MSISDN:     "05551234567",
			},
			wantErr: true,
		},
		{
			name: "no contact at all",
			record: AudienceRecord{
				CampaignID: "camp-1",
				CustomerID: "cust-4",
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Process(tc.record, hardStop, func(hasPhone, hasEmail bool) (string, bool) {
				return "sms", true
			})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.IdempotencyKey != tc.wantKey {
				t.Errorf("idempotencyKey = %q, want %q", result.IdempotencyKey, tc.wantKey)
			}
			if result.NotAfter != hardStop {
				t.Errorf("notAfter = %v, want %v", result.NotAfter, hardStop)
			}
			if result.Attributes != nil {
				if result.Attributes["tier"] != tc.record.Attributes["tier"] {
					t.Error("attributes not passed through unchanged")
				}
			}
			if result.Channel != "sms" {
				t.Errorf("channel = %q, want %q", result.Channel, "sms")
			}
		})
	}
}

func TestProcess_QuotaExhausted(t *testing.T) {
	hardStop := time.Now().Add(time.Hour)
	record := AudienceRecord{
		CampaignID: "camp-1",
		CustomerID: "cust-1",
		MSISDN:     "+15551234567",
	}

	_, err := Process(record, hardStop, func(hasPhone, hasEmail bool) (string, bool) {
		return "", false
	})
	if err == nil {
		t.Fatal("expected error when no channel has quota, got nil")
	}
}
