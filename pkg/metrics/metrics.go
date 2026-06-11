// Package metrics exposes Prometheus counters/gauges for the collab service.
package metrics

import (
	"net/http"

	metric "github.com/luxfi/metric"
)

var (
	// Rooms is the live count of distinct doc rooms.
	Rooms = metric.NewGauge(metric.GaugeOpts{
		Name: "collab_rooms",
		Help: "Number of active collaboration rooms.",
	})
	// Peers is the live count of connected WS peers across all rooms.
	Peers = metric.NewGauge(metric.GaugeOpts{
		Name: "collab_peers",
		Help: "Number of currently connected WebSocket peers.",
	})
	// Messages counts relayed binary messages, partitioned by direction.
	Messages = metric.NewCounterVec(metric.CounterOpts{
		Name: "collab_messages_total",
		Help: "Total Y.js binary messages handled.",
	}, []string{"direction"})
	// Errors counts protocol/auth/storage failures by type.
	Errors = metric.NewCounterVec(metric.CounterOpts{
		Name: "collab_errors_total",
		Help: "Errors observed by the collab service.",
	}, []string{"kind"})
)

// Register hooks all metrics into the supplied registry, or the default if nil.
func Register(reg metric.Registerer) {
	if reg == nil {
		reg = metric.DefaultRegisterer
	}
	reg.MustRegister(Rooms, Peers, Messages, Errors)
}

// Handler is the /v1/metrics HTTP handler.
func Handler() http.Handler {
	return metric.NewHTTPHandler(metric.DefaultGatherer, metric.HandlerOpts{})
}
