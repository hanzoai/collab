// Package metrics exposes Prometheus counters/gauges for the collab service.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Rooms is the live count of distinct doc rooms.
	Rooms = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "collab_rooms",
		Help: "Number of active collaboration rooms.",
	})
	// Peers is the live count of connected WS peers across all rooms.
	Peers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "collab_peers",
		Help: "Number of currently connected WebSocket peers.",
	})
	// Messages counts relayed binary messages, partitioned by direction.
	Messages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "collab_messages_total",
		Help: "Total Y.js binary messages handled.",
	}, []string{"direction"})
	// Errors counts protocol/auth/storage failures by type.
	Errors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "collab_errors_total",
		Help: "Errors observed by the collab service.",
	}, []string{"kind"})
)

// Register hooks all metrics into the supplied registry, or the default if nil.
func Register(reg prometheus.Registerer) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	reg.MustRegister(Rooms, Peers, Messages, Errors)
}

// Handler is the /v1/metrics HTTP handler.
func Handler() http.Handler { return promhttp.Handler() }
