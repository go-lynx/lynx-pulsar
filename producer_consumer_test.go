package pulsar

import (
	"context"
	"errors"
	"testing"
	"time"

	pulsarlib "github.com/apache/pulsar-client-go/pulsar"
	"github.com/prometheus/client_golang/prometheus"
)

// newTestClient creates a PulsarClient with an isolated Prometheus registry.
func newTestClient() *PulsarClient {
	return &PulsarClient{
		producers:    make(map[string]pulsarlib.Producer),
		consumers:    make(map[string]pulsarlib.Consumer),
		closeChan:    make(chan struct{}),
		metrics:      &Metrics{},
		prom:         newPromMetrics(prometheus.NewRegistry()),
		healthStatus: &HealthStatus{},
	}
}

// stubMessage implements pulsarlib.Message (only Topic() matters for tests).
type stubMessage struct{ topic string }

func (m *stubMessage) Topic() string                                          { return m.topic }
func (m *stubMessage) Properties() map[string]string                          { return nil }
func (m *stubMessage) Payload() []byte                                        { return nil }
func (m *stubMessage) ID() pulsarlib.MessageID                                { return nil }
func (m *stubMessage) PublishTime() time.Time                                 { return time.Time{} }
func (m *stubMessage) EventTime() time.Time                                   { return time.Time{} }
func (m *stubMessage) Key() string                                            { return "" }
func (m *stubMessage) OrderingKey() string                                    { return "" }
func (m *stubMessage) GetSchemaValue(_ interface{}) error                     { return nil }
func (m *stubMessage) ProducerName() string                                   { return "" }
func (m *stubMessage) SequenceID() int64                                      { return 0 }
func (m *stubMessage) IsReplicated() bool                                     { return false }
func (m *stubMessage) GetReplicatedFrom() string                              { return "" }
func (m *stubMessage) GetReplicationClusters() []string                       { return nil }
func (m *stubMessage) RedeliveryCount() uint32                                { return 0 }
func (m *stubMessage) IsEncrypted() bool                                      { return false }
func (m *stubMessage) GetEncryptionContext() *pulsarlib.EncryptionContext      { return nil }
func (m *stubMessage) Index() *uint64                                         { return nil }
func (m *stubMessage) BrokerPublishTime() *time.Time                          { return nil }
func (m *stubMessage) HasNumMessagesInBatch() bool                            { return false }
func (m *stubMessage) NumMessagesInBatch() int32                              { return 0 }
func (m *stubMessage) SchemaVersion() []byte                                  { return nil }

// ---- invokeHandler tests (tests panic recovery + metrics) ---------------

func TestInvokeHandlerSuccess(t *testing.T) {
	client := newTestClient()
	msg := &stubMessage{topic: "test-topic"}
	handler := func(_ context.Context, _ pulsarlib.Message) error { return nil }

	err := client.invokeHandler(context.Background(), "test", msg, handler)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestInvokeHandlerError(t *testing.T) {
	client := newTestClient()
	msg := &stubMessage{topic: "test-topic"}
	handler := func(_ context.Context, _ pulsarlib.Message) error {
		return errors.New("processing failed")
	}

	err := client.invokeHandler(context.Background(), "test", msg, handler)
	if err == nil {
		t.Fatal("expected error from handler")
	}
}

func TestInvokeHandlerPanicRecovery(t *testing.T) {
	client := newTestClient()
	msg := &stubMessage{topic: "test-topic"}
	handler := func(_ context.Context, _ pulsarlib.Message) error {
		panic("simulated panic")
	}

	// Must not propagate the panic.
	err := client.invokeHandler(context.Background(), "test", msg, handler)
	if err == nil {
		t.Fatal("expected error after panic recovery")
	}
}

// ---- graceful shutdown --------------------------------------------------

func TestGracefulCleanupNoInstances(t *testing.T) {
	client := newTestClient()

	done := make(chan error, 1)
	go func() { done <- client.cleanupWithContext(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup timed out")
	}

	if !client.closed {
		t.Fatal("expected client.closed=true after cleanup")
	}
}

// ---- retry manager ------------------------------------------------------

func TestRetryManagerRecordRetryNilError(t *testing.T) {
	rm := NewRetryManager(defaultPulsarConfig().Retry)
	rm.RecordRetry("send", 1, nil) // must not panic
	if rm.GetRetryStats()["send"] == nil {
		t.Fatal("expected stats recorded for 'send'")
	}
}

func TestRetryManagerRecordRetryWithError(t *testing.T) {
	rm := NewRetryManager(defaultPulsarConfig().Retry)
	rm.RecordRetry("consume", 2, errors.New("broker unavailable"))

	opStats, ok := rm.GetRetryStats()["consume"].(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", rm.GetRetryStats()["consume"])
	}
	if opStats["last_error"] != "broker unavailable" {
		t.Fatalf("unexpected last_error: %v", opStats["last_error"])
	}
}

// ---- Prometheus isolation -----------------------------------------------

func TestPrometheusMetricsIsolation(t *testing.T) {
	// Two clients with isolated registries — no duplicate-registration panics.
	c1 := newTestClient()
	c2 := newTestClient()
	c1.prom.producerSent.Inc()
	_ = c2.prom.producerSent
}
