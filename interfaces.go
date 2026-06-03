package pulsar

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-lynx/lynx/plugins"
)

// MessageHandler is the callback invoked for each message received by a consumer.
// Return a non-nil error to trigger a negative acknowledgement and redelivery.
type MessageHandler func(ctx context.Context, msg pulsar.Message) error

// Producer defines the message-publishing surface of the Pulsar plugin.
type Producer interface {
	// Produce sends a single message to the topic associated with the named producer.
	Produce(ctx context.Context, topic string, key, value []byte) error

	// ProduceWithProperties sends a message with additional user-defined properties.
	ProduceWithProperties(ctx context.Context, topic string, key, value []byte, properties map[string]string) error

	// ProduceAsync sends a message asynchronously and invokes callback on completion.
	ProduceAsync(ctx context.Context, topic string, key, value []byte, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) error

	// ProduceBatch sends a batch of pre-built messages via the named producer.
	ProduceBatch(ctx context.Context, topic string, messages []*pulsar.ProducerMessage) error

	// ProduceWith sends a message using the producer identified by producerName.
	ProduceWith(ctx context.Context, producerName, topic string, key, value []byte) error

	// GetProducer returns the underlying Pulsar producer for the given name.
	GetProducer(name string) pulsar.Producer

	// IsProducerReady reports whether the named producer is initialised and ready.
	IsProducerReady(name string) bool

	// Close closes and removes the named producer.
	Close(name string) error
}

// Consumer defines the message-consuming surface of the Pulsar plugin.
type Consumer interface {
	// Subscribe creates a push-consumer for the given topics using the default consumer.
	Subscribe(ctx context.Context, topics []string, handler MessageHandler) error

	// SubscribeWith creates a push-consumer identified by consumerName.
	SubscribeWith(ctx context.Context, consumerName string, topics []string, handler MessageHandler) error

	// SubscribeWithRegex creates a push-consumer that subscribes to topics matching
	// the regular expression pattern.
	SubscribeWithRegex(ctx context.Context, topicsPattern string, handler MessageHandler) error

	// GetConsumer returns the underlying Pulsar consumer for the given name.
	GetConsumer(name string) pulsar.Consumer

	// IsConsumerReady reports whether the named consumer is initialised and ready.
	IsConsumerReady(name string) bool

	// Unsubscribe cancels the subscription of the named consumer and removes it.
	Unsubscribe(name string) error
}

// ClientInterface is the full capability surface exposed by the Pulsar plugin.
type ClientInterface interface {
	Producer
	Consumer

	// InitializeResources loads configuration and sets up internal managers.
	InitializeResources(rt plugins.Runtime) error

	// StartupTasks connects to Pulsar and initialises producers/consumers.
	StartupTasks() error

	// ShutdownTasks gracefully closes all producers, consumers, and the client.
	ShutdownTasks() error

	// GetMetrics returns the live Prometheus-backed metrics snapshot.
	GetMetrics() *Metrics

	// GetHealth returns the current health status of the plugin.
	GetHealth() *HealthStatus
}

// MetricsProvider is the observability interface for Pulsar metrics.
type MetricsProvider interface {
	// GetStats returns a map of current metric values keyed by metric name.
	GetStats() map[string]any

	// Reset zeroes all in-process counters.
	Reset()

	// RecordMessageSent records a successfully sent message with its size and latency.
	RecordMessageSent(topic string, size int, duration time.Duration)

	// RecordMessageReceived records a successfully received message with its size.
	RecordMessageReceived(topic string, size int)

	// RecordError records an error associated with a topic and error category.
	RecordError(topic string, errorType string)
}

// HealthCheckerInterface is the observability interface for the Pulsar health checker.
type HealthCheckerInterface interface {
	// Start begins periodic health checks.
	Start()

	// Stop halts periodic health checks.
	Stop()

	// IsHealthy reports whether the last health check succeeded.
	IsHealthy() bool

	// GetLastCheck returns the timestamp of the most recent health check.
	GetLastCheck() time.Time

	// GetErrorCount returns the cumulative number of health-check failures.
	GetErrorCount() int

	// GetLastError returns the error from the most recent failed health check.
	GetLastError() error
}

// ConnectionManagerInterface manages the lifecycle of broker connections.
type ConnectionManagerInterface interface {
	// Start marks the connection as active.
	Start()

	// Stop marks the connection as inactive.
	Stop()

	// IsConnected reports whether the manager believes a connection is established.
	IsConnected() bool

	// GetConnectionStats returns a map of current connection statistics.
	GetConnectionStats() map[string]any

	// Reconnect attempts to re-establish the broker connection.
	Reconnect() error
}

// RetryManagerInterface governs retry behaviour for Pulsar operations.
type RetryManagerInterface interface {
	// ShouldRetry reports whether attempt number attempt should be retried given err.
	ShouldRetry(attempt int, err error) bool

	// GetRetryDelay returns the delay to wait before the given retry attempt.
	GetRetryDelay(attempt int) time.Duration

	// RecordRetry logs a retry attempt for the named operation.
	RecordRetry(operation string, attempt int, err error)

	// GetRetryStats returns retry statistics keyed by operation name.
	GetRetryStats() map[string]any
}

// Metrics is a snapshot of Pulsar plugin counters, sizes, and latencies.
type Metrics struct {
	// Message counts
	MessagesSent     int64
	MessagesReceived int64
	MessagesFailed   int64

	// Message sizes
	TotalBytesSent     int64
	TotalBytesReceived int64

	// Latency
	AverageSendLatency    time.Duration
	AverageReceiveLatency time.Duration

	// Error counts
	SendErrors       int64
	ReceiveErrors    int64
	ConnectionErrors int64

	// Connection stats
	ActiveConnections int
	TotalConnections  int

	// Retry stats
	TotalRetries int64
	RetryErrors  int64
}

// HealthStatus captures the overall health of the Pulsar plugin at a point in time.
type HealthStatus struct {
	// Healthy is true when all components are operating normally.
	Healthy bool

	// LastCheck is the timestamp of the most recent health evaluation.
	LastCheck time.Time

	// ErrorCount is the number of health-check failures observed.
	ErrorCount int

	// LastError holds the most recent health-check error, or nil.
	LastError error

	// ProducerStatus maps each producer name to its readiness state.
	ProducerStatus map[string]bool

	// ConsumerStatus maps each consumer name to its readiness state.
	ConsumerStatus map[string]bool

	// ConnectionStatus reports whether the broker connection is active.
	ConnectionStatus bool

	// Metrics is the most recent metrics snapshot.
	Metrics *Metrics
}
