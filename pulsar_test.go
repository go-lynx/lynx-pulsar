package pulsar

import (
	"errors"
	"testing"
	"time"

	pulsarlib "github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-lynx/lynx-pulsar/conf"
	"google.golang.org/protobuf/types/known/durationpb"
)

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
