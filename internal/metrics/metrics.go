// Package metrics defines the Prometheus metrics shared across the
// campaign executor's consumers and HTTP server.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// RecordsProcessed counts audience records handled, labeled by outcome:
// ok, skipped, or deadletter.
var RecordsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "executor_records_processed_total",
	Help: "Total audience records processed, labeled by result.",
}, []string{"result"})

// ActiveCampaigns tracks how many campaigns currently have a
// registered, unexpired window.
var ActiveCampaigns = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "executor_active_campaigns",
	Help: "Number of campaigns currently registered.",
})

// RegistryWaitSeconds records how long audience records wait on
// registry.WaitFor for their campaign to be registered.
var RegistryWaitSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
	Name: "executor_registry_wait_seconds",
	Help: "Time spent waiting for a campaign to be registered.",
})
