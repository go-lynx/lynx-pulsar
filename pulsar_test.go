package pulsar

import (
	"context"
	"errors"
	"testing"
	"time"

	pulsarlib "github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx-pulsar/conf"
	"github.com/go-lynx/lynx/plugins"
	"google.golang.org/protobuf/types/known/durationpb"
)

type staticConfigSource struct {
	content string
}

func (s staticConfigSource) Load() ([]*config.KeyValue, error) {
	return []*config.KeyValue{{
		Key:    "runtime.yaml",
		Value:  []byte(s.content),
		Format: "yaml",
	}}, nil
}

func (s staticConfigSource) Watch() (config.Watcher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &staticConfigWatcher{ctx: ctx, cancel: cancel}, nil
}

type staticConfigWatcher struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (w *staticConfigWatcher) Next() ([]*config.KeyValue, error) {
	<-w.ctx.Done()
	return nil, w.ctx.Err()
}

func (w *staticConfigWatcher) Stop() error {
	w.cancel()
	return nil
}

func newRuntimeWithConfigFile(t *testing.T, content string) plugins.Runtime {
	t.Helper()

	cfg := config.New(config.WithSource(staticConfigSource{content: content}))
	if err := cfg.Load(); err != nil {
		t.Fatalf("load config: %v", err)
	}
	t.Cleanup(func() {
		_ = cfg.Close()
	})

	rt := plugins.NewSimpleRuntime()
	rt.SetConfig(cfg)
	return rt
}

func TestNewPulsarClientDefaults(t *testing.T) {
	client := NewPulsarClient()

	if client.config == nil {
		t.Fatal("expected default config to be initialized")
	}
	if client.config.ServiceUrl != "pulsar://localhost:6650" {
		t.Fatalf("unexpected service URL: %q", client.config.ServiceUrl)
	}
	if got := len(client.GetEnabledProducers()); got != 1 {
		t.Fatalf("expected 1 enabled producer, got %d", got)
	}
	if got := len(client.GetEnabledConsumers()); got != 1 {
		t.Fatalf("expected 1 enabled consumer, got %d", got)
	}
	if client.config.Monitoring == nil || !client.config.Monitoring.EnableMetrics || !client.config.Monitoring.EnableHealthCheck {
		t.Fatal("expected monitoring defaults to enable metrics and health checks")
	}
}

func TestBuildClientOptionsAndParsers(t *testing.T) {
	client := NewPulsarClient()
	client.config = &conf.Pulsar{
		ServiceUrl: "pulsar+ssl://broker:6651",
		Connection: &conf.Connection{
			ConnectionTimeout:       durationpb.New(5 * time.Second),
			OperationTimeout:        durationpb.New(7 * time.Second),
			KeepAliveInterval:       durationpb.New(9 * time.Second),
			MaxConnectionsPerHost:   4,
			EnableConnectionPooling: true,
		},
		Tls: &conf.TLS{
			Enable:                  true,
			AllowInsecureConnection: true,
			TrustCertsFile:          "/tmp/ca.pem",
			VerifyHostname:          true,
		},
		Auth: &conf.Auth{
			Type:  "token",
			Token: "secret-token",
		},
	}

	options := client.buildClientOptions()
	if options.URL != "pulsar+ssl://broker:6651" {
		t.Fatalf("unexpected URL: %q", options.URL)
	}
	if options.ConnectionTimeout != 5*time.Second {
		t.Fatalf("unexpected connection timeout: %s", options.ConnectionTimeout)
	}
	if options.OperationTimeout != 7*time.Second {
		t.Fatalf("unexpected operation timeout: %s", options.OperationTimeout)
	}
	if options.KeepAliveInterval != 9*time.Second {
		t.Fatalf("unexpected keep alive interval: %s", options.KeepAliveInterval)
	}
	if options.MaxConnectionsPerBroker != 4 {
		t.Fatalf("unexpected max connections per broker: %d", options.MaxConnectionsPerBroker)
	}
	if !options.TLSAllowInsecureConnection {
		t.Fatal("expected TLSAllowInsecureConnection to be true")
	}
	if options.TLSTrustCertsFilePath != "/tmp/ca.pem" {
		t.Fatalf("unexpected trust certs file path: %q", options.TLSTrustCertsFilePath)
	}
	if !options.TLSValidateHostname {
		t.Fatal("expected TLSValidateHostname to be true")
	}
	if options.Authentication == nil {
		t.Fatal("expected token authentication to be configured")
	}

	if got := client.parseSubscriptionType("shared"); got != pulsarlib.Shared {
		t.Fatalf("expected shared subscription type, got %v", got)
	}
	if got := client.parseSubscriptionType("unknown"); got != pulsarlib.Exclusive {
		t.Fatalf("expected fallback subscription type, got %v", got)
	}
	if got := client.parseSubscriptionInitialPosition("earliest"); got != pulsarlib.SubscriptionPositionEarliest {
		t.Fatalf("expected earliest subscription position, got %v", got)
	}
	if got := client.parseSubscriptionInitialPosition("unknown"); got != pulsarlib.SubscriptionPositionLatest {
		t.Fatalf("expected fallback subscription position, got %v", got)
	}
}

func TestPulsarManagersAndRetryLogic(t *testing.T) {
	connectionConf := &conf.Connection{
		ConnectionTimeout:       durationpb.New(2 * time.Second),
		OperationTimeout:        durationpb.New(3 * time.Second),
		KeepAliveInterval:       durationpb.New(4 * time.Second),
		MaxConnectionsPerHost:   2,
		EnableConnectionPooling: true,
	}
	manager := NewConnectionManager(connectionConf)
	if manager.IsConnected() {
		t.Fatal("expected new connection manager to start disconnected")
	}

	manager.Start()
	if !manager.IsConnected() {
		t.Fatal("expected connection manager to be connected after Start")
	}

	stats := manager.GetConnectionStats()
	if stats["connected"] != true {
		t.Fatalf("expected connected stats to be true, got %#v", stats["connected"])
	}
	if stats["max_connections_per_host"] != int32(2) {
		t.Fatalf("unexpected max_connections_per_host stat: %#v", stats["max_connections_per_host"])
	}
	if stats["connection_timeout"] != 2*time.Second {
		t.Fatalf("unexpected connection timeout stat: %#v", stats["connection_timeout"])
	}

	manager.Stop()
	manager.Stop()
	if manager.IsConnected() {
		t.Fatal("expected connection manager to be disconnected after Stop")
	}
	if err := manager.Reconnect(); err != nil {
		t.Fatalf("Reconnect returned error: %v", err)
	}
	if !manager.IsConnected() {
		t.Fatal("expected connection manager to reconnect successfully")
	}

	healthChecker := NewHealthChecker(10 * time.Millisecond)
	if !healthChecker.IsHealthy() {
		t.Fatal("expected new health checker to start healthy")
	}
	healthChecker.performHealthCheck()
	if healthChecker.GetLastCheck().IsZero() {
		t.Fatal("expected performHealthCheck to update last check time")
	}
	if healthChecker.GetLastError() != nil {
		t.Fatalf("expected no last error after performHealthCheck, got %v", healthChecker.GetLastError())
	}
	healthChecker.Stop()
	healthChecker.Stop()

	retryManager := NewRetryManager(&conf.Retry{
		Enable:               true,
		MaxAttempts:          3,
		InitialDelay:         durationpb.New(100 * time.Millisecond),
		MaxDelay:             durationpb.New(250 * time.Millisecond),
		RetryDelayMultiplier: 2,
	})
	if !retryManager.ShouldRetry(0, errors.New("boom")) {
		t.Fatal("expected first attempt to be retryable")
	}
	if retryManager.ShouldRetry(3, errors.New("boom")) {
		t.Fatal("expected attempt at max retries to stop retrying")
	}
	if got := retryManager.GetRetryDelay(0); got != 100*time.Millisecond {
		t.Fatalf("unexpected initial retry delay: %s", got)
	}
	if got := retryManager.GetRetryDelay(1); got != 200*time.Millisecond {
		t.Fatalf("unexpected second retry delay: %s", got)
	}
	if got := retryManager.GetRetryDelay(2); got != 250*time.Millisecond {
		t.Fatalf("expected retry delay to be capped at max delay, got %s", got)
	}

	client := NewPulsarClient()
	client.config.Producers = []*conf.Producer{
		{Name: "enabled", Enabled: true},
		{Name: "disabled", Enabled: false},
	}
	client.config.Consumers = []*conf.Consumer{
		{Name: "enabled", Enabled: true},
		{Name: "disabled", Enabled: false},
	}
	if got := len(client.GetEnabledProducers()); got != 1 {
		t.Fatalf("expected 1 enabled producer after filtering, got %d", got)
	}
	if got := len(client.GetEnabledConsumers()); got != 1 {
		t.Fatalf("expected 1 enabled consumer after filtering, got %d", got)
	}
}

func TestInitializeResourcesLoadsRuntimeConfig(t *testing.T) {
	rt := newRuntimeWithConfigFile(t, `
lynx:
  pulsar:
    service_url: "pulsar://broker:6650"
    monitoring:
      enable_health_check: false
    producers:
      - name: "orders-producer"
        enabled: true
        topic: "orders"
    consumers:
      - name: "orders-consumer"
        enabled: true
        topics:
          - "orders"
        subscription_name: "orders-sub"
`)

	client := NewPulsarClient()
	if err := client.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources returned error: %v", err)
	}

	if client.config.ServiceUrl != "pulsar://broker:6650" {
		t.Fatalf("unexpected service_url: %q", client.config.ServiceUrl)
	}
	if client.config.Connection == nil {
		t.Fatal("expected connection defaults to be initialized")
	}
	if client.config.Connection.ConnectionTimeout.AsDuration() != 30*time.Second {
		t.Fatalf("unexpected default connection timeout: %s", client.config.Connection.ConnectionTimeout.AsDuration())
	}
	if client.config.Retry == nil || client.config.Retry.MaxAttempts != 3 {
		t.Fatalf("expected retry defaults to be applied, got %#v", client.config.Retry)
	}
	if client.config.Monitoring == nil {
		t.Fatal("expected monitoring config to be initialized")
	}
	if client.config.Monitoring.EnableHealthCheck {
		t.Fatal("expected runtime config to disable health check")
	}
	if client.config.Monitoring.HealthCheckInterval.AsDuration() != 30*time.Second {
		t.Fatalf("unexpected default health check interval: %s", client.config.Monitoring.HealthCheckInterval.AsDuration())
	}
	if client.healthChecker == nil || client.connectionManager == nil || client.retryManager == nil {
		t.Fatal("expected InitializeResources to create all managers")
	}
	if client.config.Producers[0].Options == nil || client.config.Producers[0].Options.SendTimeout.AsDuration() != 30*time.Second {
		t.Fatalf("expected producer defaults to be applied, got %#v", client.config.Producers[0].Options)
	}
	if client.config.Consumers[0].Options == nil || client.config.Consumers[0].Options.ReceiverQueueSize != 1000 {
		t.Fatalf("expected consumer defaults to be applied, got %#v", client.config.Consumers[0].Options)
	}
}

func TestConfigureNormalizesPartialConfig(t *testing.T) {
	client := NewPulsarClient()

	err := client.Configure(&conf.Pulsar{
		ServiceUrl: "pulsar://broker:6650",
		Monitoring: &conf.Monitoring{
			EnableHealthCheck: false,
		},
		Producers: []*conf.Producer{
			{
				Name:    "orders-producer",
				Enabled: true,
				Topic:   "orders",
			},
		},
		Consumers: []*conf.Consumer{
			{
				Name:             "orders-consumer",
				Enabled:          true,
				Topics:           []string{"orders"},
				SubscriptionName: "orders-sub",
			},
		},
	})
	if err != nil {
		t.Fatalf("Configure returned error: %v", err)
	}

	if client.config.Connection == nil || client.config.Connection.MaxConnectionsPerHost != 1 {
		t.Fatalf("expected Configure to populate connection defaults, got %#v", client.config.Connection)
	}
	if client.config.Monitoring == nil || client.config.Monitoring.EnableHealthCheck {
		t.Fatalf("expected Configure to preserve explicit disabled health check, got %#v", client.config.Monitoring)
	}
	if client.config.Producers[0].Options == nil || client.config.Producers[0].Options.SendTimeout.AsDuration() != 30*time.Second {
		t.Fatalf("expected Configure to populate producer option defaults, got %#v", client.config.Producers[0].Options)
	}
	if client.config.Consumers[0].Options == nil || client.config.Consumers[0].Options.NegativeAckDelay.AsDuration() != time.Minute {
		t.Fatalf("expected Configure to populate consumer option defaults, got %#v", client.config.Consumers[0].Options)
	}
}

func TestConfigureRejectsInvalidConfig(t *testing.T) {
	client := NewPulsarClient()

	err := client.Configure(&conf.Pulsar{
		ServiceUrl: "pulsar://broker:6650",
		Producers: []*conf.Producer{
			{
				Name:    "broken-producer",
				Enabled: true,
			},
		},
	})
	if err == nil {
		t.Fatal("expected Configure to reject enabled producer without topic")
	}
}
