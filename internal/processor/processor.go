package processor

import (
	"fmt"
	"regexp"
	"time"
)

// e164EG matches a valid Egyptian mobile number: +20 then a valid
// prefix (10/11/12/15) then 8 digits.
var e164EG = regexp.MustCompile(`^\+20(10|11|12|15)\d{8}$`)

// AudienceRecord mirrors one customer record from the campaign.audience
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

// Contact holds the customer's reachable channels, passed downstream to the Notification Engine.
type Contact struct {
	MSISDN string `json:"msisdn,omitempty"`
	Email  string `json:"email,omitempty"`
}

// NotificationRequest is the message produced to notifications.outbound.
// The Notification Engine has no access to customer data, so this
// carries everything it needs: contact info, raw attributes, deadline.
type NotificationRequest struct {
	IdempotencyKey string         `json:"idempotencyKey"`
	CampaignID     string         `json:"campaignId"`
	CustomerID     string         `json:"customerId"`
	Contact        Contact        `json:"contact"`
	Language       string         `json:"language"`
	Attributes     map[string]any `json:"attributes"`
	NotAfter       time.Time      `json:"notAfter"`
}

// Process validates one AudienceRecord and builds the outbound
// NotificationRequest. Pure function: no Kafka, no Redis, no I/O.
func Process(record AudienceRecord, hardStopAt time.Time) (*NotificationRequest, error) {
	// Reject customers we have no way to reach at all.
	if record.MSISDN == "" && record.Email == "" {
		return nil, fmt.Errorf("processor: customer %s has no contact: msisdn or email required", record.CustomerID)
	}
	// If a phone number was given, it must be a valid Egyptian mobile number.
	if record.MSISDN != "" && !e164EG.MatchString(record.MSISDN) {
		return nil, fmt.Errorf("processor: msisdn %q for customer %s is not valid E.164", record.MSISDN, record.CustomerID)
	}
	return &NotificationRequest{
		IdempotencyKey: buildIdempotencyKey(record.CampaignID, record.CustomerID),
		CampaignID:     record.CampaignID,
		CustomerID:     record.CustomerID,
		Contact:        Contact{MSISDN: record.MSISDN, Email: record.Email},
		Language:       record.Language,
		Attributes:     record.Attributes,
		NotAfter:       hardStopAt,
	}, nil
}

// buildIdempotencyKey is the single source of truth for the idempotency key format: campaignId:customerId.
func buildIdempotencyKey(campaignID, customerID string) string {
	return campaignID + ":" + customerID
}
