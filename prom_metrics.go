package pulsar

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// promMetrics holds the Prometheus instruments for the Pulsar plugin.
// It is embedded in PulsarClient and initialised once during startup.
type promMetrics struct {
	producerSent     prometheus.Counter
	producerFailed   prometheus.Counter
	producerLatency  prometheus.Histogram
	consumerReceived prometheus.Counter
	consumerFailed   prometheus.Counter
	consumerLatency  prometheus.Histogram
	connErrors       prometheus.Counter
	reconnections    prometheus.Counter
	healthErrors     prometheus.Counter
}

// newPromMetrics registers Prometheus instruments under the "lynx_pulsar"
// namespace. Duplicate registrations are silently resolved by reusing the
// already-registered collector, so the function is safe to call multiple times
// within the same process (e.g. in tests).
func newPromMetrics(reg prometheus.Registerer) *promMetrics {
	orReg := func(c prometheus.Collector) prometheus.Collector {
		if err := reg.Register(c); err != nil {
			var are prometheus.AlreadyRegisteredError
			if errors.As(err, &are) {
				return are.ExistingCollector
			}
			panic(err)
		}
		return c
	}

	pm := &promMetrics{}

	pm.producerSent = orReg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "producer",
		Name:      "messages_sent_total",
		Help:      "Total number of messages successfully sent by the producer.",
	})).(prometheus.Counter)

	pm.producerFailed = orReg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "producer",
		Name:      "messages_failed_total",
		Help:      "Total number of messages that failed to be sent by the producer.",
	})).(prometheus.Counter)

	pm.producerLatency = orReg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "producer",
		Name:      "send_duration_seconds",
		Help:      "Histogram of producer send latency in seconds.",
		Buckets:   prometheus.DefBuckets,
	})).(prometheus.Histogram)

	pm.consumerReceived = orReg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "consumer",
		Name:      "messages_received_total",
		Help:      "Total number of messages successfully processed by the consumer.",
	})).(prometheus.Counter)

	pm.consumerFailed = orReg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "consumer",
		Name:      "messages_failed_total",
		Help:      "Total number of messages that failed processing.",
	})).(prometheus.Counter)

	pm.consumerLatency = orReg(prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "consumer",
		Name:      "process_duration_seconds",
		Help:      "Histogram of consumer message-processing latency in seconds.",
		Buckets:   prometheus.DefBuckets,
	})).(prometheus.Histogram)

	pm.connErrors = orReg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "connection",
		Name:      "errors_total",
		Help:      "Total number of connection errors encountered.",
	})).(prometheus.Counter)

	pm.reconnections = orReg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "connection",
		Name:      "reconnections_total",
		Help:      "Total number of reconnection attempts.",
	})).(prometheus.Counter)

	pm.healthErrors = orReg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "lynx_pulsar",
		Subsystem: "health",
		Name:      "check_errors_total",
		Help:      "Total number of failed health checks.",
	})).(prometheus.Counter)

	return pm
}
