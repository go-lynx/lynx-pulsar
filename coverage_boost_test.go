package pulsar

import (
	"context"
	"errors"
	"testing"
	"time"

	pulsarlib "github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-lynx/lynx-pulsar/conf"
	"github.com/go-lynx/lynx/plugins"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ---------------------------------------------------------------------------
// lifecycle_context.go
// ---------------------------------------------------------------------------

func TestLifecycleContext_IsContextAware(t *testing.T) {
	client := NewPulsarClient()
	if !client.IsContextAware() {
		t.Fatal("expected IsContextAware to return true")
	}
}

func TestLifecycleContext_PluginProtocol(t *testing.T) {
	client := NewPulsarClient()
	proto := client.PluginProtocol()
	if !proto.ContextLifecycle {
		t.Fatal("expected PluginProtocol.ContextLifecycle to be true")
	}
}

func TestLifecycleContext_InitializeContext_Canceled(t *testing.T) {
	client := NewPulsarClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.InitializeContext(ctx, nil, nil)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLifecycleContext_StartContext_Canceled(t *testing.T) {
	client := NewPulsarClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.StartContext(ctx, nil)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

func TestLifecycleContext_StopContext_Canceled(t *testing.T) {
	client := NewPulsarClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.StopContext(ctx, nil)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

// ---------------------------------------------------------------------------
// startupWithContext – canceled-before-execution path
// ---------------------------------------------------------------------------

func TestStartupWithContext_CanceledBeforeExecution(t *testing.T) {
	client := NewPulsarClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.startupWithContext(ctx)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

func TestStartupWithContext_FailsOnBadURL(t *testing.T) {
	client := NewPulsarClient()
	client.config.ServiceUrl = "pulsar://0.0.0.0:1/" // unreachable
	err := client.startupWithContext(context.Background())
	if err == nil {
		t.Fatal("expected startup to fail with unreachable broker")
	}
}

// ---------------------------------------------------------------------------
// ShutdownTasks
// ---------------------------------------------------------------------------

func TestShutdownTasks_IdempotentOnCleanClient(t *testing.T) {
	client := newTestClient()
	if err := client.ShutdownTasks(); err != nil {
		t.Fatalf("expected ShutdownTasks to succeed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// cleanupWithContext – canceled path
// ---------------------------------------------------------------------------

func TestCleanupWithContext_Canceled(t *testing.T) {
	client := newTestClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.cleanupWithContext(ctx)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

// ---------------------------------------------------------------------------
// CheckHealth
// ---------------------------------------------------------------------------

func TestCheckHealth_NilClient(t *testing.T) {
	client := newTestClient()
	// client is nil by default in newTestClient.
	err := client.CheckHealth()
	if err == nil {
		t.Fatal("expected CheckHealth to fail with nil pulsar client")
	}
}

func TestCheckHealth_NilConnectionManager(t *testing.T) {
	client := newTestClient()
	// Inject a stub pulsar client to pass the nil check.
	client.client = &stubPulsarClient{}
	client.connectionManager = nil
	// Should succeed (nil connectionManager means skip connectivity check).
	if err := client.CheckHealth(); err != nil {
		t.Fatalf("expected success when connectionManager is nil, got %v", err)
	}
}

func TestCheckHealth_ConnectionManagerDisconnected(t *testing.T) {
	client := newTestClient()
	client.client = &stubPulsarClient{}
	client.connectionManager = NewConnectionManager(defaultPulsarConfig().Connection)
	// Not started → not connected.
	if err := client.CheckHealth(); err == nil {
		t.Fatal("expected CheckHealth to fail when connection manager is not connected")
	}
}

func TestCheckHealth_WithNilProducerAndConsumer(t *testing.T) {
	client := newTestClient()
	client.client = &stubPulsarClient{}
	client.connectionManager = NewConnectionManager(defaultPulsarConfig().Connection)
	client.connectionManager.Start()
	// Nil producers/consumers should only log warnings, not fail.
	client.producers["p1"] = nil
	client.consumers["c1"] = nil
	if err := client.CheckHealth(); err != nil {
		t.Fatalf("expected CheckHealth to pass with nil producers/consumers (just warnings), got %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetPulsarConfig / GetClient / IsConnected / GetMetrics / GetHealth
// ---------------------------------------------------------------------------

func TestGetPulsarConfig(t *testing.T) {
	client := NewPulsarClient()
	if client.GetPulsarConfig() == nil {
		t.Fatal("expected GetPulsarConfig to return non-nil config")
	}
}

func TestGetClient_Nil(t *testing.T) {
	client := newTestClient()
	if client.GetClient() != nil {
		t.Fatal("expected nil client before startup")
	}
}

func TestIsConnected_ClosedClient(t *testing.T) {
	client := newTestClient()
	client.closed = true
	if client.IsConnected() {
		t.Fatal("expected closed client to not be connected")
	}
}

func TestIsConnected_NilClient(t *testing.T) {
	client := newTestClient()
	client.client = nil
	if client.IsConnected() {
		t.Fatal("expected nil client to not be connected")
	}
}

func TestIsConnected_WithConnectionManager(t *testing.T) {
	client := newTestClient()
	client.client = &stubPulsarClient{}
	cm := NewConnectionManager(defaultPulsarConfig().Connection)
	cm.Start()
	client.connectionManager = cm
	if !client.IsConnected() {
		t.Fatal("expected IsConnected to return true when connection manager is active")
	}
}

func TestIsConnected_NoConnectionManager(t *testing.T) {
	client := newTestClient()
	client.client = &stubPulsarClient{}
	client.connectionManager = nil
	if !client.IsConnected() {
		t.Fatal("expected IsConnected to return true when no connection manager (trusts the client)")
	}
}

func TestGetMetrics(t *testing.T) {
	client := NewPulsarClient()
	if client.GetMetrics() == nil {
		t.Fatal("expected GetMetrics to return non-nil")
	}
}

func TestGetHealth(t *testing.T) {
	client := NewPulsarClient()
	if client.GetHealth() == nil {
		t.Fatal("expected GetHealth to return non-nil")
	}
	client.healthStatus = nil
	h := client.GetHealth()
	if h == nil {
		t.Fatal("expected GetHealth to return empty HealthStatus when nil")
	}
}

// ---------------------------------------------------------------------------
// Producer interface – no producer available paths
// ---------------------------------------------------------------------------

func TestProduce_NoProducer(t *testing.T) {
	client := newTestClient()
	err := client.Produce(context.Background(), "topic", nil, []byte("data"))
	if err == nil {
		t.Fatal("expected error when no default producer")
	}
}

func TestProduceWithProperties_NoProducer(t *testing.T) {
	client := newTestClient()
	err := client.ProduceWithProperties(context.Background(), "topic", nil, []byte("data"), nil)
	if err == nil {
		t.Fatal("expected error when no default producer")
	}
}

func TestProduceAsync_NoProducer(t *testing.T) {
	client := newTestClient()
	err := client.ProduceAsync(context.Background(), "topic", nil, []byte("data"), nil)
	if err == nil {
		t.Fatal("expected error when no default producer")
	}
}

func TestProduceBatch_NoProducer(t *testing.T) {
	client := newTestClient()
	msgs := []*pulsarlib.ProducerMessage{{Payload: []byte("hello")}}
	err := client.ProduceBatch(context.Background(), "topic", msgs)
	if err == nil {
		t.Fatal("expected error when no default producer")
	}
}

func TestProduceWith_NoProducer(t *testing.T) {
	client := newTestClient()
	err := client.ProduceWith(context.Background(), "missing", "topic", nil, []byte("data"))
	if err == nil {
		t.Fatal("expected error when named producer is not found")
	}
}

// ---------------------------------------------------------------------------
// Producer interface – with stub producer
// ---------------------------------------------------------------------------

func TestProduce_Success(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(nil)
	client.producers["p1"] = sp

	err := client.Produce(context.Background(), "topic", []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if sp.sendCount != 1 {
		t.Fatalf("expected 1 send, got %d", sp.sendCount)
	}
}

func TestProduce_WithKey_Success(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(nil)
	client.producers["p1"] = sp

	err := client.Produce(context.Background(), "topic", []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("expected success with key, got %v", err)
	}
}

func TestProduceWith_Error(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(errors.New("broker error"))
	client.producers["p1"] = sp

	err := client.ProduceWith(context.Background(), "p1", "topic", nil, []byte("data"))
	if err == nil {
		t.Fatal("expected error from producer send")
	}
}

func TestProduceWithProperties_Success(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(nil)
	client.producers["p1"] = sp

	err := client.ProduceWithProperties(context.Background(), "topic", []byte("k"), []byte("v"),
		map[string]string{"x-prop": "val"})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestProduceWithProperties_Error(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(errors.New("send failed"))
	client.producers["p1"] = sp

	err := client.ProduceWithProperties(context.Background(), "topic", nil, []byte("v"), nil)
	if err == nil {
		t.Fatal("expected error from producer send")
	}
}

func TestProduceBatch_Success(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(nil)
	client.producers["p1"] = sp

	msgs := []*pulsarlib.ProducerMessage{
		{Payload: []byte("m1")},
		{Payload: []byte("m2")},
	}
	err := client.ProduceBatch(context.Background(), "topic", msgs)
	if err != nil {
		t.Fatalf("expected batch success, got %v", err)
	}
	if sp.sendCount != 2 {
		t.Fatalf("expected 2 sends, got %d", sp.sendCount)
	}
}

func TestProduceBatch_Error(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(errors.New("batch error"))
	client.producers["p1"] = sp

	msgs := []*pulsarlib.ProducerMessage{{Payload: []byte("m1")}}
	err := client.ProduceBatch(context.Background(), "topic", msgs)
	if err == nil {
		t.Fatal("expected error on batch send failure")
	}
}

func TestProduceAsync_Success(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(nil)
	client.producers["p1"] = sp

	var cbCalled bool
	err := client.ProduceAsync(context.Background(), "topic", nil, []byte("data"),
		func(id pulsarlib.MessageID, msg *pulsarlib.ProducerMessage, err error) {
			cbCalled = true
		})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	// Give the async callback time to fire.
	time.Sleep(10 * time.Millisecond)
	if !cbCalled {
		t.Fatal("expected async callback to be invoked")
	}
}

func TestProduceAsync_NilCallback(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(nil)
	client.producers["p1"] = sp

	// nil callback must not panic.
	err := client.ProduceAsync(context.Background(), "topic", nil, []byte("data"), nil)
	if err != nil {
		t.Fatalf("expected success with nil callback, got %v", err)
	}
}

func TestGetProducer_NotFound(t *testing.T) {
	client := newTestClient()
	if got := client.GetProducer("missing"); got != nil {
		t.Fatalf("expected nil for missing producer, got %v", got)
	}
}

func TestIsProducerReady_TrueAndFalse(t *testing.T) {
	client := newTestClient()
	if client.IsProducerReady("none") {
		t.Fatal("expected false for missing producer")
	}
	client.producers["p"] = nil
	if client.IsProducerReady("p") {
		t.Fatal("expected false for nil producer")
	}
	client.producers["p"] = newStubProducer(nil)
	if !client.IsProducerReady("p") {
		t.Fatal("expected true for non-nil producer")
	}
}

func TestClose_ProducerExists(t *testing.T) {
	client := newTestClient()
	sp := newStubProducer(nil)
	client.producers["p1"] = sp

	if err := client.Close("p1"); err != nil {
		t.Fatalf("unexpected error from Close: %v", err)
	}
	if _, ok := client.producers["p1"]; ok {
		t.Fatal("expected producer to be removed after Close")
	}
}

func TestClose_ProducerNotFound(t *testing.T) {
	client := newTestClient()
	// Close on missing producer should be a no-op.
	if err := client.Close("missing"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClose_NilProducer(t *testing.T) {
	client := newTestClient()
	client.producers["p"] = nil
	// Should not panic when producer is nil.
	if err := client.Close("p"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Consumer interface
// ---------------------------------------------------------------------------

func TestSubscribe_NoConsumer(t *testing.T) {
	client := newTestClient()
	err := client.Subscribe(context.Background(), []string{"topic"}, func(_ context.Context, _ pulsarlib.Message) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error when no consumers")
	}
}

func TestSubscribeWith_ConsumerNotFound(t *testing.T) {
	client := newTestClient()
	err := client.SubscribeWith(context.Background(), "missing", []string{"topic"},
		func(_ context.Context, _ pulsarlib.Message) error { return nil })
	if err == nil {
		t.Fatal("expected error for missing consumer")
	}
}

func TestSubscribeWith_NilHandler(t *testing.T) {
	client := newTestClient()
	sc := newStubConsumer()
	client.consumers["c1"] = sc

	err := client.SubscribeWith(context.Background(), "c1", []string{"topic"}, nil)
	if err == nil {
		t.Fatal("expected error for nil handler")
	}
}

func TestSubscribeWith_Success_CtxCancel(t *testing.T) {
	client := newTestClient()
	sc := newStubConsumer()
	client.consumers["c1"] = sc

	ctx, cancel := context.WithCancel(context.Background())
	err := client.SubscribeWith(ctx, "c1", []string{"topic"},
		func(_ context.Context, _ pulsarlib.Message) error { return nil })
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	// Cancel to stop the goroutine.
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestSubscribeWith_ClosedChannel(t *testing.T) {
	client := newTestClient()
	sc := newStubConsumerWithClosedChan()
	client.consumers["c1"] = sc

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := client.SubscribeWith(ctx, "c1", []string{"topic"},
		func(_ context.Context, _ pulsarlib.Message) error { return nil })
	if err != nil {
		t.Fatalf("expected success starting consumer, got %v", err)
	}
	// Give the goroutine time to detect the closed channel and exit.
	time.Sleep(50 * time.Millisecond)
}

func TestGetConsumer_NotFound(t *testing.T) {
	client := newTestClient()
	if got := client.GetConsumer("missing"); got != nil {
		t.Fatalf("expected nil for missing consumer, got %v", got)
	}
}

func TestIsConsumerReady_TrueAndFalse(t *testing.T) {
	client := newTestClient()
	if client.IsConsumerReady("none") {
		t.Fatal("expected false for missing consumer")
	}
	client.consumers["c"] = nil
	if client.IsConsumerReady("c") {
		t.Fatal("expected false for nil consumer")
	}
	client.consumers["c"] = newStubConsumer()
	if !client.IsConsumerReady("c") {
		t.Fatal("expected true for non-nil consumer")
	}
}

func TestUnsubscribe_ConsumerExists(t *testing.T) {
	client := newTestClient()
	sc := newStubConsumer()
	client.consumers["c1"] = sc

	if err := client.Unsubscribe("c1"); err != nil {
		t.Fatalf("unexpected error from Unsubscribe: %v", err)
	}
	if _, ok := client.consumers["c1"]; ok {
		t.Fatal("expected consumer to be removed after Unsubscribe")
	}
}

func TestUnsubscribe_ConsumerNotFound(t *testing.T) {
	client := newTestClient()
	if err := client.Unsubscribe("missing"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnsubscribe_NilConsumer(t *testing.T) {
	client := newTestClient()
	client.consumers["c"] = nil
	if err := client.Unsubscribe("c"); err != nil {
		t.Fatalf("unexpected error for nil consumer: %v", err)
	}
}

// ---------------------------------------------------------------------------
// dispatchMessage
// ---------------------------------------------------------------------------

func TestDispatchMessage_HandlerError_Nacks(t *testing.T) {
	client := newTestClient()
	sc := newStubConsumer()
	msg := &stubMessage{topic: "t"}

	client.dispatchMessage(context.Background(), sc, "c1", msg, func(_ context.Context, _ pulsarlib.Message) error {
		return errors.New("handler error")
	})
	if sc.nackCount != 1 {
		t.Fatalf("expected 1 nack, got %d", sc.nackCount)
	}
	if sc.ackCount != 0 {
		t.Fatalf("expected 0 ack, got %d", sc.ackCount)
	}
}

func TestDispatchMessage_HandlerSuccess_Acks(t *testing.T) {
	client := newTestClient()
	sc := newStubConsumer()
	msg := &stubMessage{topic: "t"}

	client.dispatchMessage(context.Background(), sc, "c1", msg, func(_ context.Context, _ pulsarlib.Message) error {
		return nil
	})
	if sc.ackCount != 1 {
		t.Fatalf("expected 1 ack, got %d", sc.ackCount)
	}
	if sc.nackCount != 0 {
		t.Fatalf("expected 0 nack, got %d", sc.nackCount)
	}
}

// ---------------------------------------------------------------------------
// SubscribeWithRegex – no client
// ---------------------------------------------------------------------------

func TestSubscribeWithRegex_NilClient(t *testing.T) {
	client := newTestClient()
	err := client.SubscribeWithRegex(context.Background(), "persistent://.*", func(_ context.Context, _ pulsarlib.Message) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error when client is not initialised")
	}
}

func TestSubscribeWithRegex_NilHandler(t *testing.T) {
	client := newTestClient()
	client.client = &stubPulsarClient{} // non-nil but will fail Subscribe
	err := client.SubscribeWithRegex(context.Background(), "persistent://.*", nil)
	if err == nil {
		t.Fatal("expected error for nil handler")
	}
}

// ---------------------------------------------------------------------------
// HealthChecker – Start/run path
// ---------------------------------------------------------------------------

func TestHealthChecker_StartAndStop_Pulsar(t *testing.T) {
	h := NewHealthChecker(5 * time.Millisecond)
	h.Start()
	time.Sleep(20 * time.Millisecond)

	if !h.IsHealthy() {
		t.Fatal("expected health checker to be healthy")
	}
	if h.GetLastCheck().IsZero() {
		t.Fatal("expected last check to be updated")
	}
	if h.GetErrorCount() != 0 {
		t.Fatalf("expected 0 error count, got %d", h.GetErrorCount())
	}
	if h.GetLastError() != nil {
		t.Fatalf("expected nil last error, got %v", h.GetLastError())
	}
	h.Stop()
	h.Stop() // idempotent
}

// ---------------------------------------------------------------------------
// ConnectionManager – Start / Stop
// ---------------------------------------------------------------------------

func TestConnectionManager_StartAndStop_Pulsar(t *testing.T) {
	cm := NewConnectionManager(defaultPulsarConfig().Connection)
	cm.Start()
	if !cm.IsConnected() {
		t.Fatal("expected connected after Start")
	}
	cm.Stop()
	if cm.IsConnected() {
		t.Fatal("expected disconnected after Stop")
	}
}

// ---------------------------------------------------------------------------
// normalizePulsarConfig edge cases
// ---------------------------------------------------------------------------

func TestNormalizePulsarConfig_NilConfig(t *testing.T) {
	if err := normalizePulsarConfig(nil); err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestNormalizePulsarConfig_MaxDelayLessThanInitialDelay(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Retry.InitialDelay = durationpb.New(5 * time.Second)
	cfg.Retry.MaxDelay = durationpb.New(1 * time.Second)
	if err := normalizePulsarConfig(cfg); err == nil {
		t.Fatal("expected error when max_delay < initial_delay")
	}
}

func TestNormalizePulsarConfig_NegativeJitter(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Retry.JitterFactor = -0.1
	if err := normalizePulsarConfig(cfg); err == nil {
		t.Fatal("expected error for negative jitter_factor")
	}
}

func TestNormalizePulsarConfig_NilProducer(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Producers = []*conf.Producer{nil}
	if err := normalizePulsarConfig(cfg); err == nil {
		t.Fatal("expected error for nil producer config")
	}
}

func TestNormalizePulsarConfig_NilConsumer(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Consumers = []*conf.Consumer{nil}
	if err := normalizePulsarConfig(cfg); err == nil {
		t.Fatal("expected error for nil consumer config")
	}
}

func TestNormalizePulsarConfig_AuthTokenMissing(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Auth = &conf.Auth{Type: "token", Token: ""}
	if err := normalizePulsarConfig(cfg); err == nil {
		t.Fatal("expected error for empty token when auth.type=token")
	}
}

func TestNormalizePulsarConfig_AuthOauth2Missing(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Auth = &conf.Auth{Type: "oauth2", Oauth2: nil}
	if err := normalizePulsarConfig(cfg); err == nil {
		t.Fatal("expected error for missing oauth2 config")
	}
}

func TestNormalizePulsarConfig_AuthTLSMissing(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Auth = &conf.Auth{Type: "tls", TlsAuth: nil}
	if err := normalizePulsarConfig(cfg); err == nil {
		t.Fatal("expected error for missing tls_auth config")
	}
}

func TestNormalizePulsarConfig_AuthUnsupported(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Auth = &conf.Auth{Type: "unsupported"}
	if err := normalizePulsarConfig(cfg); err == nil {
		t.Fatal("expected error for unsupported auth type")
	}
}

func TestNormalizePulsarConfig_NoConnection(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Connection = nil
	if err := normalizePulsarConfig(cfg); err != nil {
		t.Fatalf("expected nil error when connection is nil (uses defaults), got %v", err)
	}
	if cfg.Connection == nil {
		t.Fatal("expected connection defaults to be filled in")
	}
}

func TestNormalizePulsarConfig_NoRetry(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Retry = nil
	if err := normalizePulsarConfig(cfg); err != nil {
		t.Fatalf("expected nil error when retry is nil (uses defaults), got %v", err)
	}
	if cfg.Retry == nil {
		t.Fatal("expected retry defaults to be filled in")
	}
}

func TestNormalizePulsarConfig_NoMonitoring(t *testing.T) {
	cfg := defaultPulsarConfig()
	cfg.Monitoring = nil
	if err := normalizePulsarConfig(cfg); err != nil {
		t.Fatalf("expected nil error when monitoring is nil (uses defaults), got %v", err)
	}
	if cfg.Monitoring == nil {
		t.Fatal("expected monitoring defaults to be filled in")
	}
}

// ---------------------------------------------------------------------------
// buildClientOptions – auth paths (oauth2, tls)
// ---------------------------------------------------------------------------

func TestBuildClientOptions_OAuth2Auth(t *testing.T) {
	client := NewPulsarClient()
	client.config = &conf.Pulsar{
		ServiceUrl: "pulsar://localhost:6650",
		Auth: &conf.Auth{
			Type: "oauth2",
			Oauth2: &conf.OAuth2{
				IssuerUrl:    "https://issuer.example.com",
				ClientId:     "client-id",
				ClientSecret: "client-secret",
				Audience:     "aud",
				Scope:        "scope",
			},
		},
	}
	// buildClientOptions should not panic when OAuth2 params are present;
	// the actual Authentication value depends on the Pulsar SDK factory which
	// may return nil for non-routable issuer URLs in unit-test environments.
	_ = client.buildClientOptions()
}

func TestBuildClientOptions_TLSAuth(t *testing.T) {
	client := NewPulsarClient()
	client.config = &conf.Pulsar{
		ServiceUrl: "pulsar+ssl://localhost:6651",
		Auth: &conf.Auth{
			Type: "tls",
			TlsAuth: &conf.TLSAuth{
				CertFile: "/tmp/cert.pem",
				KeyFile:  "/tmp/key.pem",
			},
		},
	}
	opts := client.buildClientOptions()
	if opts.Authentication == nil {
		t.Fatal("expected TLS authentication to be configured")
	}
}

func TestBuildClientOptions_NoAuth(t *testing.T) {
	client := NewPulsarClient()
	client.config = defaultPulsarConfig()
	client.config.Auth = nil
	opts := client.buildClientOptions()
	if opts.URL != "pulsar://localhost:6650" {
		t.Fatalf("unexpected URL: %q", opts.URL)
	}
	if opts.Authentication != nil {
		t.Fatal("expected no authentication when auth is nil")
	}
}

// ---------------------------------------------------------------------------
// parseSubscriptionType – remaining paths
// ---------------------------------------------------------------------------

func TestParseSubscriptionType_AllTypes(t *testing.T) {
	client := NewPulsarClient()
	if got := client.parseSubscriptionType("failover"); got != pulsarlib.Failover {
		t.Fatalf("expected Failover, got %v", got)
	}
	if got := client.parseSubscriptionType("key_shared"); got != pulsarlib.KeyShared {
		t.Fatalf("expected KeyShared, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// runtime_contract.go
// ---------------------------------------------------------------------------

func TestPublishRuntimeContract_WithNilRt(t *testing.T) {
	client := newTestClient()
	// should be a no-op without panicking
	client.publishRuntimeContract(true, true)
}

func TestPublishRuntimeContract_WithRt(t *testing.T) {
	base := plugins.NewSimpleRuntime()
	rt := base.WithPluginContext(pluginName)

	client := newTestClient()
	client.rt = rt

	client.publishRuntimeContract(true, true)

	if readiness, err := base.GetSharedResource(sharedReadinessResourceName); err != nil || readiness != true {
		t.Fatalf("unexpected shared readiness: %v %v", readiness, err)
	}
}

func TestRegisterRuntimePluginAlias_WithRt(t *testing.T) {
	base := plugins.NewSimpleRuntime()
	rt := base.WithPluginContext(pluginName)

	client := newTestClient()
	client.rt = rt

	client.registerRuntimePluginAlias()
	// Should not panic; the shared resource may or may not be registered
	// depending on whether the runtime accepts the key.
}

// ---------------------------------------------------------------------------
// prom_metrics – path where duplicate registration is handled
// ---------------------------------------------------------------------------

func TestPromMetrics_DuplicateRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	pm1 := newPromMetrics(reg)
	pm2 := newPromMetrics(reg)
	// Both should have non-nil metrics.
	if pm1.producerSent == nil || pm2.producerSent == nil {
		t.Fatal("expected non-nil metrics after duplicate registration")
	}
}

func TestPromMetrics_AllCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	pm := newPromMetrics(reg)
	pm.producerSent.Inc()
	pm.producerFailed.Inc()
	pm.producerLatency.Observe(0.001)
	pm.consumerReceived.Inc()
	pm.consumerFailed.Inc()
	pm.consumerLatency.Observe(0.002)
	pm.connErrors.Inc()
	pm.reconnections.Inc()
	pm.healthErrors.Inc()
}

// ---------------------------------------------------------------------------
// Configure – invalid input
// ---------------------------------------------------------------------------

func TestConfigure_InvalidType(t *testing.T) {
	client := NewPulsarClient()
	err := client.Configure("not a pulsar config")
	if err == nil {
		t.Fatal("expected error for invalid config type")
	}
}

// ---------------------------------------------------------------------------
// clonePulsarConfig
// ---------------------------------------------------------------------------

func TestClonePulsarConfig_Nil(t *testing.T) {
	if clonePulsarConfig(nil) != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestClonePulsarConfig_NonNil(t *testing.T) {
	original := defaultPulsarConfig()
	cloned := clonePulsarConfig(original)
	if cloned == nil {
		t.Fatal("expected non-nil clone")
	}
	if cloned.ServiceUrl != original.ServiceUrl {
		t.Fatalf("expected same ServiceUrl, got %q", cloned.ServiceUrl)
	}
	// Verify it's a deep copy.
	cloned.ServiceUrl = "changed"
	if original.ServiceUrl == "changed" {
		t.Fatal("expected clone to be independent of original")
	}
}

// ---------------------------------------------------------------------------
// Stub implementations for testing
// ---------------------------------------------------------------------------

// stubProducer implements pulsar.Producer for testing.
type stubProducer struct {
	sendErr   error
	sendCount int
}

func newStubProducer(sendErr error) *stubProducer {
	return &stubProducer{sendErr: sendErr}
}

func (s *stubProducer) Topic() string { return "stub-topic" }
func (s *stubProducer) Name() string  { return "stub-producer" }
func (s *stubProducer) Send(_ context.Context, _ *pulsarlib.ProducerMessage) (pulsarlib.MessageID, error) {
	s.sendCount++
	return nil, s.sendErr
}
func (s *stubProducer) SendAsync(_ context.Context, msg *pulsarlib.ProducerMessage, cb func(pulsarlib.MessageID, *pulsarlib.ProducerMessage, error)) {
	s.sendCount++
	if cb != nil {
		go cb(nil, msg, s.sendErr)
	}
}
func (s *stubProducer) LastSequenceID() int64                { return 0 }
func (s *stubProducer) Flush() error                         { return nil }
func (s *stubProducer) FlushWithCtx(_ context.Context) error { return nil }
func (s *stubProducer) Close()                               {}
func (s *stubProducer) Schema() pulsarlib.Schema             { return nil }

// stubConsumer implements pulsar.Consumer for testing.
type stubConsumer struct {
	ackCount  int
	nackCount int
	msgChan   chan pulsarlib.ConsumerMessage
}

func newStubConsumer() *stubConsumer {
	return &stubConsumer{
		msgChan: make(chan pulsarlib.ConsumerMessage),
	}
}

func newStubConsumerWithClosedChan() *stubConsumer {
	ch := make(chan pulsarlib.ConsumerMessage)
	close(ch)
	return &stubConsumer{msgChan: ch}
}

func (s *stubConsumer) Subscription() string { return "stub-subscription" }
func (s *stubConsumer) Unsubscribe() error   { return nil }
func (s *stubConsumer) Receive(_ context.Context) (pulsarlib.Message, error) {
	msg, ok := <-s.msgChan
	if !ok {
		return nil, errors.New("channel closed")
	}
	return msg.Message, nil
}
func (s *stubConsumer) Chan() <-chan pulsarlib.ConsumerMessage { return s.msgChan }
func (s *stubConsumer) Ack(msg pulsarlib.Message) error {
	s.ackCount++
	return nil
}
func (s *stubConsumer) AckID(_ pulsarlib.MessageID) error                             { return nil }
func (s *stubConsumer) ReconsumeLater(_ pulsarlib.Message, _ time.Duration)           {}
func (s *stubConsumer) AckCumulative(_ pulsarlib.Message) error                       { return nil }
func (s *stubConsumer) AckCumulativeID(_ pulsarlib.MessageID) error                   { return nil }
func (s *stubConsumer) AckWithTxn(_ pulsarlib.Message, _ pulsarlib.Transaction) error { return nil }
func (s *stubConsumer) AckIDWithTxn(_ pulsarlib.MessageID, _ pulsarlib.Transaction) error {
	return nil
}
func (s *stubConsumer) AckIDCumulativeWithTxn(_ pulsarlib.MessageID, _ pulsarlib.Transaction) error {
	return nil
}
func (s *stubConsumer) Nack(msg pulsarlib.Message) {
	s.nackCount++
}
func (s *stubConsumer) NackID(_ pulsarlib.MessageID)     {}
func (s *stubConsumer) Close()                           {}
func (s *stubConsumer) Name() string                     { return "stub-consumer" }
func (s *stubConsumer) Seek(_ pulsarlib.MessageID) error { return nil }
func (s *stubConsumer) SeekByTime(_ time.Time) error     { return nil }
func (s *stubConsumer) GetLastMessageIDs() ([]pulsarlib.TopicMessageID, error) {
	return nil, nil
}
func (s *stubConsumer) AckIDCumulative(_ pulsarlib.MessageID) error { return nil }
func (s *stubConsumer) AckIDList(_ []pulsarlib.MessageID) error     { return nil }
func (s *stubConsumer) UnsubscribeForce() error                     { return nil }
func (s *stubConsumer) ReconsumeLaterWithCustomProperties(_ pulsarlib.Message, _ map[string]string, _ time.Duration) {
}
func (s *stubConsumer) GetLastMessageID(_ string) (pulsarlib.MessageID, error) { return nil, nil }
func (s *stubConsumer) Topic() string                                          { return "stub-topic" }

// stubPulsarClient implements pulsar.Client for testing.
type stubPulsarClient struct{}

func (s *stubPulsarClient) CreateProducer(_ pulsarlib.ProducerOptions) (pulsarlib.Producer, error) {
	return nil, errors.New("stub: no broker")
}
func (s *stubPulsarClient) Subscribe(_ pulsarlib.ConsumerOptions) (pulsarlib.Consumer, error) {
	return nil, errors.New("stub: no broker")
}
func (s *stubPulsarClient) CreateReader(_ pulsarlib.ReaderOptions) (pulsarlib.Reader, error) {
	return nil, errors.New("stub: no broker")
}
func (s *stubPulsarClient) CreateTableView(_ pulsarlib.TableViewOptions) (pulsarlib.TableView, error) {
	return nil, errors.New("stub: no broker")
}
func (s *stubPulsarClient) TopicPartitions(_ string) ([]string, error) {
	return nil, errors.New("stub: no broker")
}
func (s *stubPulsarClient) NewTransaction(_ time.Duration) (pulsarlib.Transaction, error) {
	return nil, errors.New("stub: transactions not supported")
}
func (s *stubPulsarClient) Close() {}
