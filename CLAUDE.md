# Campaign Executor — Agent Instructions

## What this service does

Consumes `campaign-executor.execute` (campaign window commands) and
`campaign.audience` (customer records), validates each customer,
produces `NotificationRequest` to `notifications.outbound`. Failures
go to `campaign.deadletter`. No template/channel logic lives here —
that belongs to the Notification Engine.

## Non-negotiable rules

1. Do NOT add `template_id`, `channels`, or `correlationId` anywhere.
   Removed intentionally in v2.2 — do not reintroduce them.
2. JSON field names are camelCase, exactly as defined in the shared
   structs. Never rename a field without checking this file first.
3. `idempotencyKey` = `campaignId + ":" + customerId`. One helper
   function, used everywhere. Never inline the format elsewhere.
4. Audience records within one Kafka partition are processed
   SEQUENTIALLY. Never add goroutines/worker pools inside a single
   partition's consume loop — this breaks per-campaign ordering.
5. Offsets commit ONLY after the produce ack for that record
   succeeds (or after a confirmed deadletter write). Never commit
   before producing.
6. `processor/processor.go` stays a pure function: input a struct,
   output a struct + error. No Kafka, no Redis, no network calls in
   this package — this is what makes it unit-testable.
7. Registry writes go to the in-memory map AND Redis, in that order,
   inside `Register()`. Never write to only one.
8. Do not implement Kubernetes/Helm, autoscaling, schema registry,
   or tracing — explicitly deferred. Docker/docker-compose only.

## Repository layout (do not restructure)

cmd/executor/main.go — wiring only, no business logic
internal/command/ — execute-command consumer
internal/audience/ — partition loop + WaitFor
internal/registry/ — map + Redis + deadline contexts
internal/processor/ — pure validation/build logic
internal/producer/ — outbound + deadletter producer
internal/httpapi/ — /healthz /readyz /metrics only
internal/config/ — env var loading only
test/synthetic/ — fake Scheduler + Customer Manager

## JSON contracts (exact field names — do not deviate)

ExecuteCommand: campaignId, messageId, executeAt, hardStopAt
AudienceRecord: eventType, campaignId, customerId, msisdn,
email, language, attributes, totalCount
NotificationRequest: idempotencyKey, campaignId, customerId,
contact{msisdn,email}, language, attributes,
notAfter
DeadLetter: campaignId, originalTopic, originalPartition,
originalOffset, lastError, attempts, failedAt,
snapshot

## Before writing code in any file

- Check whether the change belongs in that file per the layout above.
- Check the JSON contracts section above for exact field names.
- Run `go build ./...` after any change; `go mod tidy` after adding
  a new import.

## Style

- Standard Go idioms: error wrapping with `%w`, no panics outside
  main() startup, `context.Context` as first param on any function
  that does I/O.
- Structured logs via `log/slog`, always include `campaignId` and
  `customerId` (when known) on every log line touching a record.
