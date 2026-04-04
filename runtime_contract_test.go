package pulsar

import (
	"testing"
	"time"

	"github.com/go-lynx/lynx-pulsar/conf"
	"github.com/go-lynx/lynx/plugins"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPulsarRuntimeContract_NoInstanceLifecycle(t *testing.T) {
	base := plugins.NewSimpleRuntime()
	rt := base.WithPluginContext(pluginName)

	client := NewPulsarClient()
	client.rt = rt
	client.config = &conf.Pulsar{
		ServiceUrl: "pulsar://127.0.0.1:6650",
		Connection: &conf.Connection{
			ConnectionTimeout:       durationpb.New(100 * time.Millisecond),
			OperationTimeout:        durationpb.New(100 * time.Millisecond),
			KeepAliveInterval:       durationpb.New(time.Second),
			MaxConnectionsPerHost:   1,
			EnableConnectionPooling: true,
		},
		Retry: &conf.Retry{
			Enable:               false,
			MaxAttempts:          1,
			InitialDelay:         durationpb.New(time.Millisecond),
			MaxDelay:             durationpb.New(time.Millisecond),
			RetryDelayMultiplier: 1,
		},
		Monitoring: &conf.Monitoring{
			EnableMetrics:       true,
			EnableHealthCheck:   false,
			HealthCheckInterval: durationpb.New(time.Second),
		},
	}
	client.healthChecker = NewHealthChecker(time.Second)
	client.connectionManager = NewConnectionManager(client.config.Connection)
	client.retryManager = NewRetryManager(client.config.Retry)

	if err := client.StartupTasks(); err != nil {
		t.Fatalf("StartupTasks failed: %v", err)
	}

	if alias, err := base.GetSharedResource(sharedPluginResourceName); err != nil || alias != client {
		t.Fatalf("unexpected shared plugin alias: value=%#v err=%v", alias, err)
	}
	if readiness, err := base.GetSharedResource(sharedReadinessResourceName); err != nil || readiness != true {
		t.Fatalf("unexpected shared readiness: value=%#v err=%v", readiness, err)
	}
	if health, err := base.GetSharedResource(sharedHealthResourceName); err != nil || health != true {
		t.Fatalf("unexpected shared health: value=%#v err=%v", health, err)
	}
	if _, err := rt.GetPrivateResource("config"); err != nil {
		t.Fatalf("private config resource missing: %v", err)
	}

	if err := client.CleanupTasks(); err != nil {
		t.Fatalf("CleanupTasks failed: %v", err)
	}

	if readiness, err := base.GetSharedResource(sharedReadinessResourceName); err != nil || readiness != false {
		t.Fatalf("unexpected shared readiness after cleanup: value=%#v err=%v", readiness, err)
	}
	if health, err := base.GetSharedResource(sharedHealthResourceName); err != nil || health != false {
		t.Fatalf("unexpected shared health after cleanup: value=%#v err=%v", health, err)
	}
}
