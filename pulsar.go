// Package pulsar provides a production-grade Apache Pulsar client plugin for the
// go-lynx framework. It supports multiple named producer and consumer instances,
// TLS and authentication, configurable retry with exponential back-off, Prometheus
// metrics, and graceful shutdown.
package pulsar

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	kratosconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx-pulsar/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Plugin metadata constants
const (
	pluginName        = "pulsar.client"
	pluginVersion     = "v1.6.1"
	pluginDescription = "Apache Pulsar client plugin for lynx framework"
	confPrefix        = "lynx.pulsar"
)

// PulsarClient represents the main Pulsar client plugin instance.
type PulsarClient struct {
	*plugins.BasePlugin
	config            *conf.Pulsar
	rt                plugins.Runtime
	client            pulsar.Client
	producers         map[string]pulsar.Producer
	consumers         map[string]pulsar.Consumer
	producerMutex     sync.RWMutex
	consumerMutex     sync.RWMutex
	closeChan         chan struct{}
	closeOnce         sync.Once // protect against multiple close operations
	// stateMutex guards closed and client against concurrent access from
	// CheckHealth/IsConnected/SubscribeWithRegex and cleanupWithContext.
	stateMutex sync.RWMutex
	closed     bool
	// recvWG tracks all running receive goroutines so cleanup can wait for
	// them to drain before closing consumers/producers/client.
	recvWG sync.WaitGroup
	// subscriptions maps a consumer name to the cancel func of its active
	// receive loop, used to prevent two goroutines draining the same
	// consumer.Chan() (split stream / competing Ack).
	subscriptions map[string]context.CancelFunc
	metrics           *Metrics
	prom              *promMetrics
	healthStatus      *HealthStatus
	healthChecker     *HealthChecker
	connectionManager *ConnectionManager
	retryManager      *RetryManager
}

// NewPulsarClient creates a new Pulsar client plugin instance.
func NewPulsarClient() *PulsarClient {
	c := &PulsarClient{
		config:       defaultPulsarConfig(),
		producers:     make(map[string]pulsar.Producer),
		consumers:     make(map[string]pulsar.Consumer),
		subscriptions: make(map[string]context.CancelFunc),
		closeChan:     make(chan struct{}),
		closed:        false,
		metrics:      &Metrics{},
		prom:         newPromMetrics(prometheus.DefaultRegisterer),
		healthStatus: &HealthStatus{},
	}

	c.BasePlugin = plugins.NewBasePlugin(
		plugins.GeneratePluginID("", pluginName, pluginVersion),
		pluginName,
		pluginDescription,
		pluginVersion,
		confPrefix,
		103, // Weight for Pulsar
	)

	return c
}

func defaultPulsarConfig() *conf.Pulsar {
	return &conf.Pulsar{
		ServiceUrl: "pulsar://localhost:6650",
		Connection: &conf.Connection{
			ConnectionTimeout:       durationpb.New(30 * time.Second),
			OperationTimeout:        durationpb.New(30 * time.Second),
			KeepAliveInterval:       durationpb.New(30 * time.Second),
			MaxConnectionsPerHost:   1,
			EnableConnectionPooling: true,
		},
		Retry: &conf.Retry{
			Enable:               true,
			MaxAttempts:          3,
			InitialDelay:         durationpb.New(100 * time.Millisecond),
			MaxDelay:             durationpb.New(30 * time.Second),
			RetryDelayMultiplier: 2.0,
			JitterFactor:         0.1,
		},
		Monitoring: &conf.Monitoring{
			EnableMetrics:       true,
			MetricsNamespace:    "lynx_pulsar",
			EnableHealthCheck:   true,
			HealthCheckInterval: durationpb.New(30 * time.Second),
		},
		Producers: []*conf.Producer{
			{
				Name:    "default-producer",
				Enabled: true,
				Topic:   "default-topic",
				Options: &conf.ProducerOptions{
					SendTimeout:             durationpb.New(30 * time.Second),
					MaxPendingMessages:      1000,
					BatchingEnabled:         true,
					BatchingMaxPublishDelay: durationpb.New(10 * time.Millisecond),
					BatchingMaxMessages:     1000,
					CompressionType:         "none",
					HashingScheme:           "java_string_hash",
					MessageRoutingMode:      "round_robin",
				},
			},
		},
		Consumers: []*conf.Consumer{
			{
				Name:             "default-consumer",
				Enabled:          true,
				Topics:           []string{"default-topic"},
				SubscriptionName: "default-subscription",
				Options: &conf.ConsumerOptions{
					SubscriptionType:            "exclusive",
					SubscriptionInitialPosition: "latest",
					SubscriptionMode:            "durable",
					ReceiverQueueSize:           1000,
					EnableRetryOnMessageFailure: true,
					RetryEnable:                 true,
					NegativeAckDelay:            durationpb.New(1 * time.Minute),
					CryptoFailureAction:         "fail",
				},
			},
		},
	}
}

func clonePulsarConfig(cfg *conf.Pulsar) *conf.Pulsar {
	if cfg == nil {
		return nil
	}
	cloned, ok := proto.Clone(cfg).(*conf.Pulsar)
	if !ok {
		return nil
	}
	return cloned
}

// setClient stores the underlying Pulsar client under the state lock.
func (p *PulsarClient) setClient(client pulsar.Client) {
	p.stateMutex.Lock()
	p.client = client
	p.stateMutex.Unlock()
}

// getClient returns the underlying Pulsar client under the state lock.
func (p *PulsarClient) getClient() pulsar.Client {
	p.stateMutex.RLock()
	defer p.stateMutex.RUnlock()
	return p.client
}

// isClosed reports whether the plugin has been shut down, under the state lock.
func (p *PulsarClient) isClosed() bool {
	p.stateMutex.RLock()
	defer p.stateMutex.RUnlock()
	return p.closed
}

// stopReceivers signals every active receive loop to stop (without holding any
// other lock) and waits for all of them to drain. It must be called before any
// consumer/producer/client is closed so goroutines cannot iterate
// consumer.Chan() or call Ack/Nack on a closed consumer.
func (p *PulsarClient) stopReceivers() {
	p.consumerMutex.Lock()
	cancels := make([]context.CancelFunc, 0, len(p.subscriptions))
	for name, cancel := range p.subscriptions {
		cancels = append(cancels, cancel)
		delete(p.subscriptions, name)
	}
	p.consumerMutex.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	p.recvWG.Wait()
}

// closeAllResources tears down consumers, producers and the client. It first
// stops and awaits all receive goroutines so nothing races with Close(). Safe
// to call during a failed startup to avoid leaking partially-created resources.
func (p *PulsarClient) closeAllResources() {
	p.stopReceivers()

	p.consumerMutex.Lock()
	for name, consumer := range p.consumers {
		if consumer != nil {
			consumer.Close()
			log.Infof("consumer %s closed", name)
		}
	}
	p.consumers = make(map[string]pulsar.Consumer)
	p.consumerMutex.Unlock()

	p.producerMutex.Lock()
	for name, producer := range p.producers {
		if producer != nil {
			producer.Close()
			log.Infof("producer %s closed", name)
		}
	}
	p.producers = make(map[string]pulsar.Producer)
	p.producerMutex.Unlock()

	p.stateMutex.Lock()
	if p.client != nil {
		p.client.Close()
		p.client = nil
	}
	p.stateMutex.Unlock()
}

// Configure updates Pulsar configuration
func (p *PulsarClient) Configure(c any) error {
	if pulsarConf, ok := c.(*conf.Pulsar); ok {
		normalized := clonePulsarConfig(pulsarConf)
		if normalized == nil {
			return plugins.ErrInvalidConfiguration
		}
		if err := normalizePulsarConfig(normalized); err != nil {
			return err
		}
		p.config = normalized
		return nil
	}
	return plugins.ErrInvalidConfiguration
}

// InitializeResources initializes the plugin with configuration
func (p *PulsarClient) InitializeResources(rt plugins.Runtime) error {
	// Initialize base plugin
	if err := p.BasePlugin.InitializeResources(rt); err != nil {
		return err
	}
	p.rt = rt

	cfg := defaultPulsarConfig()
	if rt != nil && rt.GetConfig() != nil {
		// Scan into a default-populated config so omitted YAML fields keep production-safe defaults.
		if err := rt.GetConfig().Value(confPrefix).Scan(cfg); err != nil && !errors.Is(err, kratosconfig.ErrNotFound) {
			return fmt.Errorf("failed to scan Pulsar configuration: %w", err)
		}
	}
	if err := normalizePulsarConfig(cfg); err != nil {
		return err
	}
	p.config = cfg

	// Initialize managers
	if p.config.Monitoring != nil {
		p.healthChecker = NewHealthChecker(p.config.Monitoring.HealthCheckInterval.AsDuration())
	}
	if p.config.Connection != nil {
		p.connectionManager = NewConnectionManager(p.config.Connection)
	}
	if p.config.Retry != nil {
		p.retryManager = NewRetryManager(p.config.Retry)
	}

	return nil
}

func normalizePulsarConfig(cfg *conf.Pulsar) error {
	if cfg == nil {
		return fmt.Errorf("pulsar configuration is required")
	}
	if cfg.ServiceUrl == "" {
		cfg.ServiceUrl = "pulsar://localhost:6650"
	}

	if cfg.Connection == nil {
		cfg.Connection = defaultPulsarConfig().Connection
	} else {
		if cfg.Connection.ConnectionTimeout == nil || cfg.Connection.ConnectionTimeout.AsDuration() <= 0 {
			cfg.Connection.ConnectionTimeout = durationpb.New(30 * time.Second)
		}
		if cfg.Connection.OperationTimeout == nil || cfg.Connection.OperationTimeout.AsDuration() <= 0 {
			cfg.Connection.OperationTimeout = durationpb.New(30 * time.Second)
		}
		if cfg.Connection.KeepAliveInterval == nil || cfg.Connection.KeepAliveInterval.AsDuration() <= 0 {
			cfg.Connection.KeepAliveInterval = durationpb.New(30 * time.Second)
		}
		if cfg.Connection.MaxConnectionsPerHost <= 0 {
			cfg.Connection.MaxConnectionsPerHost = 1
		}
	}

	if cfg.Retry == nil {
		cfg.Retry = defaultPulsarConfig().Retry
	} else {
		if cfg.Retry.MaxAttempts <= 0 {
			cfg.Retry.MaxAttempts = 3
		}
		if cfg.Retry.InitialDelay == nil || cfg.Retry.InitialDelay.AsDuration() <= 0 {
			cfg.Retry.InitialDelay = durationpb.New(100 * time.Millisecond)
		}
		if cfg.Retry.MaxDelay == nil || cfg.Retry.MaxDelay.AsDuration() <= 0 {
			cfg.Retry.MaxDelay = durationpb.New(30 * time.Second)
		}
		if cfg.Retry.MaxDelay.AsDuration() < cfg.Retry.InitialDelay.AsDuration() {
			return fmt.Errorf("pulsar retry.max_delay must be greater than or equal to retry.initial_delay")
		}
		if cfg.Retry.RetryDelayMultiplier <= 0 {
			cfg.Retry.RetryDelayMultiplier = 2.0
		}
		if cfg.Retry.JitterFactor < 0 {
			return fmt.Errorf("pulsar retry.jitter_factor must be non-negative")
		}
	}

	if cfg.Monitoring == nil {
		cfg.Monitoring = defaultPulsarConfig().Monitoring
	} else {
		if cfg.Monitoring.MetricsNamespace == "" {
			cfg.Monitoring.MetricsNamespace = "lynx_pulsar"
		}
		if cfg.Monitoring.HealthCheckInterval == nil || cfg.Monitoring.HealthCheckInterval.AsDuration() <= 0 {
			cfg.Monitoring.HealthCheckInterval = durationpb.New(30 * time.Second)
		}
	}

	for i, producer := range cfg.Producers {
		if producer == nil {
			return fmt.Errorf("pulsar producers[%d] configuration is nil", i)
		}
		if producer.Enabled {
			if producer.Name == "" {
				return fmt.Errorf("pulsar producers[%d].name is required when enabled", i)
			}
			if producer.Topic == "" {
				return fmt.Errorf("pulsar producers[%d].topic is required when enabled", i)
			}
		}
		if producer.Options == nil {
			producer.Options = &conf.ProducerOptions{}
		}
		if producer.Options.SendTimeout == nil || producer.Options.SendTimeout.AsDuration() <= 0 {
			producer.Options.SendTimeout = durationpb.New(30 * time.Second)
		}
		if producer.Options.MaxPendingMessages <= 0 {
			producer.Options.MaxPendingMessages = 1000
		}
		if producer.Options.BatchingMaxPublishDelay == nil || producer.Options.BatchingMaxPublishDelay.AsDuration() <= 0 {
			producer.Options.BatchingMaxPublishDelay = durationpb.New(10 * time.Millisecond)
		}
		if producer.Options.BatchingMaxMessages <= 0 {
			producer.Options.BatchingMaxMessages = 1000
		}
		if producer.Options.CompressionType == "" {
			producer.Options.CompressionType = "none"
		}
		if producer.Options.HashingScheme == "" {
			producer.Options.HashingScheme = "java_string_hash"
		}
		if producer.Options.MessageRoutingMode == "" {
			producer.Options.MessageRoutingMode = "round_robin"
		}
	}

	for i, consumer := range cfg.Consumers {
		if consumer == nil {
			return fmt.Errorf("pulsar consumers[%d] configuration is nil", i)
		}
		if consumer.Enabled {
			if consumer.Name == "" {
				return fmt.Errorf("pulsar consumers[%d].name is required when enabled", i)
			}
			if len(consumer.Topics) == 0 {
				return fmt.Errorf("pulsar consumers[%d].topics is required when enabled", i)
			}
			if consumer.SubscriptionName == "" {
				return fmt.Errorf("pulsar consumers[%d].subscription_name is required when enabled", i)
			}
		}
		if consumer.Options == nil {
			consumer.Options = &conf.ConsumerOptions{}
		}
		if consumer.Options.SubscriptionType == "" {
			consumer.Options.SubscriptionType = "exclusive"
		}
		if consumer.Options.SubscriptionInitialPosition == "" {
			consumer.Options.SubscriptionInitialPosition = "latest"
		}
		if consumer.Options.SubscriptionMode == "" {
			consumer.Options.SubscriptionMode = "durable"
		}
		if consumer.Options.ReceiverQueueSize <= 0 {
			consumer.Options.ReceiverQueueSize = 1000
		}
		if consumer.Options.NegativeAckDelay == nil || consumer.Options.NegativeAckDelay.AsDuration() <= 0 {
			consumer.Options.NegativeAckDelay = durationpb.New(1 * time.Minute)
		}
		if consumer.Options.CryptoFailureAction == "" {
			consumer.Options.CryptoFailureAction = "fail"
		}
	}

	if cfg.Auth != nil {
		switch cfg.Auth.Type {
		case "", "token":
			if cfg.Auth.Type == "token" && cfg.Auth.Token == "" {
				return fmt.Errorf("pulsar auth.token is required when auth.type=token")
			}
		case "oauth2":
			if cfg.Auth.Oauth2 == nil {
				return fmt.Errorf("pulsar auth.oauth2 is required when auth.type=oauth2")
			}
			if cfg.Auth.Oauth2.IssuerUrl == "" || cfg.Auth.Oauth2.ClientId == "" || cfg.Auth.Oauth2.ClientSecret == "" {
				return fmt.Errorf("pulsar auth.oauth2 issuer_url, client_id, and client_secret are required")
			}
		case "tls":
			if cfg.Auth.TlsAuth == nil {
				return fmt.Errorf("pulsar auth.tls_auth is required when auth.type=tls")
			}
			if cfg.Auth.TlsAuth.CertFile == "" || cfg.Auth.TlsAuth.KeyFile == "" {
				return fmt.Errorf("pulsar auth.tls_auth cert_file and key_file are required")
			}
		default:
			return fmt.Errorf("unsupported pulsar auth.type %q", cfg.Auth.Type)
		}
	}

	return nil
}

// StartupTasks initializes Pulsar client and performs health check
func (p *PulsarClient) StartupTasks() error {
	return p.startupWithContext(context.Background())
}

func (p *PulsarClient) startupWithContext(ctx context.Context) error {
	log.Infof("initializing Apache Pulsar client")
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pulsar startup canceled before execution: %w", err)
	}
	p.publishRuntimeContract(false, false)

	// Create Pulsar client
	clientOptions := p.buildClientOptions()
	client, err := pulsar.NewClient(clientOptions)
	if err != nil {
		p.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to create Pulsar client: %w", err)
	}
	p.setClient(client)
	if err := ctx.Err(); err != nil {
		p.closeAllResources()
		p.publishRuntimeContract(false, false)
		return fmt.Errorf("pulsar startup canceled after client creation: %w", err)
	}

	// Initialize producers
	if err := p.initializeProducers(); err != nil {
		p.closeAllResources()
		p.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to initialize producers: %w", err)
	}
	if err := ctx.Err(); err != nil {
		p.closeAllResources()
		p.publishRuntimeContract(false, false)
		return fmt.Errorf("pulsar startup canceled after producer init: %w", err)
	}

	// Initialize consumers
	if err := p.initializeConsumers(); err != nil {
		p.closeAllResources()
		p.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to initialize consumers: %w", err)
	}

	// Start health checker
	if p.config.Monitoring != nil && p.config.Monitoring.EnableHealthCheck && p.healthChecker != nil {
		p.healthChecker.Start()
	}

	// Start connection manager
	if p.connectionManager != nil {
		p.connectionManager.Start()
	}

	if p.rt != nil {
		if err := p.rt.RegisterSharedResource(pluginName, p); err != nil {
			p.publishRuntimeContract(false, false)
			return fmt.Errorf("failed to register Pulsar shared resource: %w", err)
		}
		p.registerRuntimePluginAlias()
		if err := p.rt.RegisterPrivateResource("config", p.config); err != nil {
			log.Warnf("failed to register Pulsar private config resource: %v", err)
		}
		if p.client != nil {
			if err := p.rt.RegisterPrivateResource("client", p.client); err != nil {
				log.Warnf("failed to register Pulsar private client resource: %v", err)
			}
		}
		if len(p.producers) > 0 {
			if err := p.rt.RegisterPrivateResource("producers", p.producers); err != nil {
				log.Warnf("failed to register Pulsar private producers resource: %v", err)
			}
		}
		if len(p.consumers) > 0 {
			if err := p.rt.RegisterPrivateResource("consumers", p.consumers); err != nil {
				log.Warnf("failed to register Pulsar private consumers resource: %v", err)
			}
		}
		if p.connectionManager != nil {
			if err := p.rt.RegisterPrivateResource("connection_manager", p.connectionManager); err != nil {
				log.Warnf("failed to register Pulsar private connection manager resource: %v", err)
			}
		}
		if p.healthChecker != nil {
			if err := p.rt.RegisterPrivateResource("health_checker", p.healthChecker); err != nil {
				log.Warnf("failed to register Pulsar private health checker resource: %v", err)
			}
		}
		if p.retryManager != nil {
			if err := p.rt.RegisterPrivateResource("retry_manager", p.retryManager); err != nil {
				log.Warnf("failed to register Pulsar private retry manager resource: %v", err)
			}
		}
		if p.metrics != nil {
			if err := p.rt.RegisterPrivateResource("metrics", p.metrics); err != nil {
				log.Warnf("failed to register Pulsar private metrics resource: %v", err)
			}
		}
	}

	if err := p.CheckHealth(); err != nil {
		p.publishRuntimeContract(false, false)
		return err
	}
	p.publishRuntimeContract(true, true)

	log.Infof("Apache Pulsar client successfully initialized")
	return nil
}

// ShutdownTasks gracefully shuts down the plugin.
// It is an alias for CleanupTasks and satisfies the ClientInterface contract.
func (p *PulsarClient) ShutdownTasks() error {
	return p.CleanupTasks()
}

// CleanupTasks gracefully shuts down the plugin.
func (p *PulsarClient) CleanupTasks() error {
	return p.cleanupWithContext(context.Background())
}

func (p *PulsarClient) cleanupWithContext(ctx context.Context) error {
	log.Infof("shutting down Apache Pulsar client plugin")
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pulsar cleanup canceled before execution: %w", err)
	}
	p.publishRuntimeContract(false, false)

	// Signal background tasks to stop (protected against multiple calls)
	p.closeOnce.Do(func() {
		close(p.closeChan)
	})
	p.stateMutex.Lock()
	p.closed = true
	p.stateMutex.Unlock()

	// Stop health checker
	if p.healthChecker != nil {
		p.healthChecker.Stop()
	}

	// Stop connection manager
	if p.connectionManager != nil {
		p.connectionManager.Stop()
	}

	// Stop and await all receive goroutines, then close consumers, producers
	// and the client. Awaiting the loops first prevents a use-after-close race
	// where a goroutine iterates consumer.Chan() / calls Ack/Nack on a consumer
	// that is being closed.
	p.closeAllResources()

	log.Infof("Apache Pulsar client plugin successfully shut down")
	return nil
}

// CheckHealth performs a health check on the Pulsar client. It verifies that
// the underlying Pulsar client is initialised and that the connection manager
// reports an active connection. Individual nil producers/consumers are logged
// as warnings but do not cause the check to fail.
func (p *PulsarClient) CheckHealth() error {
	if p.getClient() == nil {
		return fmt.Errorf("pulsar client not initialized")
	}

	// connectionManager is optional: if it was not created (e.g. config.Connection
	// was nil), we skip the connectivity check.
	if p.connectionManager != nil && !p.connectionManager.IsConnected() {
		return fmt.Errorf("pulsar client not connected")
	}

	// Log nil producers/consumers as warnings without failing the health check.
	p.producerMutex.RLock()
	for name, producer := range p.producers {
		if producer == nil {
			log.Warnf("producer %s is nil", name)
		}
	}
	p.producerMutex.RUnlock()

	p.consumerMutex.RLock()
	for name, consumer := range p.consumers {
		if consumer == nil {
			log.Warnf("consumer %s is nil", name)
		}
	}
	p.consumerMutex.RUnlock()

	return nil
}

// buildClientOptions builds Pulsar client options from configuration
func (p *PulsarClient) buildClientOptions() pulsar.ClientOptions {
	options := pulsar.ClientOptions{
		URL: p.config.ServiceUrl,
	}

	// Connection options
	if p.config.Connection != nil {
		options.ConnectionTimeout = p.config.Connection.ConnectionTimeout.AsDuration()
		options.OperationTimeout = p.config.Connection.OperationTimeout.AsDuration()
		options.KeepAliveInterval = p.config.Connection.KeepAliveInterval.AsDuration()
		options.MaxConnectionsPerBroker = int(p.config.Connection.MaxConnectionsPerHost)
	}

	// TLS options
	if p.config.Tls != nil && p.config.Tls.Enable {
		options.TLSAllowInsecureConnection = p.config.Tls.AllowInsecureConnection
		if p.config.Tls.TrustCertsFile != "" {
			options.TLSTrustCertsFilePath = p.config.Tls.TrustCertsFile
		}
		options.TLSValidateHostname = p.config.Tls.VerifyHostname
	}

	// Authentication options
	if p.config.Auth != nil {
		switch p.config.Auth.Type {
		case "token":
			if p.config.Auth.Token != "" {
				options.Authentication = pulsar.NewAuthenticationToken(p.config.Auth.Token)
			}
		case "oauth2":
			if p.config.Auth.Oauth2 != nil {
				oauth2 := p.config.Auth.Oauth2
				authParams := map[string]string{
					"issuerEndpoint": oauth2.IssuerUrl,
					"clientId":       oauth2.ClientId,
					"clientSecret":   oauth2.ClientSecret,
					"audience":       oauth2.Audience,
					"scope":          oauth2.Scope,
				}
				options.Authentication = pulsar.NewAuthenticationOAuth2(authParams)
			}
		case "tls":
			if p.config.Auth.TlsAuth != nil {
				tlsAuth := p.config.Auth.TlsAuth
				options.Authentication = pulsar.NewAuthenticationTLS(
					tlsAuth.CertFile,
					tlsAuth.KeyFile,
				)
			}
		}
	}

	return options
}

// initializeProducers initializes all configured producers
func (p *PulsarClient) initializeProducers() error {
	for _, producerConfig := range p.GetEnabledProducers() {
		if err := p.createProducer(producerConfig); err != nil {
			return fmt.Errorf("failed to create producer %s: %w", producerConfig.Name, err)
		}
	}
	return nil
}

// initializeConsumers initializes all configured consumers
func (p *PulsarClient) initializeConsumers() error {
	for _, consumerConfig := range p.GetEnabledConsumers() {
		if err := p.createConsumer(consumerConfig); err != nil {
			return fmt.Errorf("failed to create consumer %s: %w", consumerConfig.Name, err)
		}
	}
	return nil
}

// createProducer creates a Pulsar producer
func (p *PulsarClient) createProducer(config *conf.Producer) error {
	options := pulsar.ProducerOptions{
		Topic: config.Topic,
	}

	if config.Options != nil {
		if config.Options.ProducerName != "" {
			options.Name = config.Options.ProducerName
		}
		if config.Options.SendTimeout != nil {
			options.SendTimeout = config.Options.SendTimeout.AsDuration()
		}
		if config.Options.MaxPendingMessages > 0 {
			options.MaxPendingMessages = int(config.Options.MaxPendingMessages)
		}
		if config.Options.BatchingEnabled {
			if config.Options.BatchingMaxPublishDelay != nil {
				options.BatchingMaxPublishDelay = config.Options.BatchingMaxPublishDelay.AsDuration()
			}
			options.BatchingMaxMessages = uint(config.Options.BatchingMaxMessages)
			options.BatchingMaxSize = uint(config.Options.BatchingMaxSize)
		}
		if config.Options.EnableChunking {
			options.EnableChunking = true
			options.ChunkMaxMessageSize = uint(config.Options.ChunkMaxSize)
		}
		if config.Options.CompressionType != "" {
			options.CompressionType = p.parseCompressionType(config.Options.CompressionType)
		}
		if config.Options.HashingScheme != "" {
			options.HashingScheme = p.parseHashingScheme(config.Options.HashingScheme)
		}
		// Note: pulsar-client-go v0.19.0 has no MessageRoutingMode field on
		// ProducerOptions (only a custom MessageRouter func). round_robin vs
		// single_partition is selected internally based on whether a message
		// key is set, so the configured routing mode cannot be mapped directly.
	}

	client := p.getClient()
	if client == nil {
		return fmt.Errorf("pulsar client not initialized")
	}
	producer, err := client.CreateProducer(options)
	if err != nil {
		return err
	}

	p.producerMutex.Lock()
	p.producers[config.Name] = producer
	p.producerMutex.Unlock()

	log.Infof("producer %s created for topic %s", config.Name, config.Topic)
	return nil
}

// createConsumer creates a Pulsar consumer
func (p *PulsarClient) createConsumer(config *conf.Consumer) error {
	options := pulsar.ConsumerOptions{
		Topics:           config.Topics,
		SubscriptionName: config.SubscriptionName,
	}

	if config.Options != nil {
		if config.Options.ConsumerName != "" {
			options.Name = config.Options.ConsumerName
		}
		if config.Options.SubscriptionType != "" {
			options.Type = p.parseSubscriptionType(config.Options.SubscriptionType)
		}
		if config.Options.SubscriptionInitialPosition != "" {
			options.SubscriptionInitialPosition = p.parseSubscriptionInitialPosition(config.Options.SubscriptionInitialPosition)
		}
		if config.Options.SubscriptionMode != "" {
			options.SubscriptionMode = p.parseSubscriptionMode(config.Options.SubscriptionMode)
		}
		if config.Options.ReceiverQueueSize > 0 {
			options.ReceiverQueueSize = int(config.Options.ReceiverQueueSize)
		}
		if config.Options.NegativeAckDelay != nil {
			options.NackRedeliveryDelay = config.Options.NegativeAckDelay.AsDuration()
		}
		if config.Options.ReadCompacted {
			options.ReadCompacted = true
		}
		// Retry / automatic redelivery via the retry-letter topic. Both
		// retry_enable and enable_retry_on_message_failure enable the feature.
		if config.Options.RetryEnable || config.Options.EnableRetryOnMessageFailure {
			options.RetryEnable = true
		}
		// Dead letter policy: route poison messages to a DLQ after the
		// configured number of redeliveries.
		if config.Options.DeadLetterPolicy != nil {
			dlp := config.Options.DeadLetterPolicy
			options.DLQ = &pulsar.DLQPolicy{
				MaxDeliveries:           uint32(dlp.MaxRedeliverCount),
				DeadLetterTopic:         dlp.DeadLetterTopic,
				InitialSubscriptionName: dlp.InitialSubscriptionName,
			}
		}
		// Note: CryptoFailureAction is only meaningful alongside a configured
		// message decryptor (Decryption.MessageCrypto). Since no key reader /
		// MessageCrypto is configured here, it cannot be wired in isolation
		// without breaking decryption; it is validated but not applied.
		if config.Options.Properties != nil {
			options.Properties = config.Options.Properties
		}
	}

	client := p.getClient()
	if client == nil {
		return fmt.Errorf("pulsar client not initialized")
	}
	consumer, err := client.Subscribe(options)
	if err != nil {
		return err
	}

	p.consumerMutex.Lock()
	p.consumers[config.Name] = consumer
	p.consumerMutex.Unlock()

	log.Infof("consumer %s created for topics %v with subscription %s",
		config.Name, config.Topics, config.SubscriptionName)
	return nil
}

// parseSubscriptionType parses subscription type string to Pulsar type
func (p *PulsarClient) parseSubscriptionType(subType string) pulsar.SubscriptionType {
	switch subType {
	case "exclusive":
		return pulsar.Exclusive
	case "shared":
		return pulsar.Shared
	case "failover":
		return pulsar.Failover
	case "key_shared":
		return pulsar.KeyShared
	default:
		return pulsar.Exclusive
	}
}

// parseSubscriptionMode parses subscription mode string to Pulsar mode.
func (p *PulsarClient) parseSubscriptionMode(mode string) pulsar.SubscriptionMode {
	switch mode {
	case "durable":
		return pulsar.Durable
	case "non_durable":
		return pulsar.NonDurable
	default:
		return pulsar.Durable
	}
}

// parseCompressionType maps a compression type string to the Pulsar enum.
func (p *PulsarClient) parseCompressionType(ct string) pulsar.CompressionType {
	switch ct {
	case "none":
		return pulsar.NoCompression
	case "lz4":
		return pulsar.LZ4
	case "zlib":
		return pulsar.ZLib
	case "zstd":
		return pulsar.ZSTD
	case "snappy":
		return pulsar.SNAPPY
	default:
		return pulsar.NoCompression
	}
}

// parseHashingScheme maps a hashing scheme string to the Pulsar enum.
func (p *PulsarClient) parseHashingScheme(hs string) pulsar.HashingScheme {
	switch hs {
	case "java_string_hash":
		return pulsar.JavaStringHash
	case "murmur3_32hash":
		return pulsar.Murmur3_32Hash
	default:
		return pulsar.JavaStringHash
	}
}

// parseSubscriptionInitialPosition parses subscription initial position
func (p *PulsarClient) parseSubscriptionInitialPosition(pos string) pulsar.SubscriptionInitialPosition {
	switch pos {
	case "earliest":
		return pulsar.SubscriptionPositionEarliest
	case "latest":
		return pulsar.SubscriptionPositionLatest
	default:
		return pulsar.SubscriptionPositionLatest
	}
}

// GetPulsarConfig returns the current Pulsar configuration
func (p *PulsarClient) GetPulsarConfig() *conf.Pulsar {
	return p.config
}

// GetClient returns the underlying Pulsar client
func (p *PulsarClient) GetClient() pulsar.Client {
	return p.getClient()
}

// IsConnected reports whether the Pulsar client is ready and connected.
func (p *PulsarClient) IsConnected() bool {
	p.stateMutex.RLock()
	closed := p.closed
	client := p.client
	p.stateMutex.RUnlock()
	if closed || client == nil {
		return false
	}
	if p.connectionManager != nil {
		return p.connectionManager.IsConnected()
	}
	return true
}

// GetMetrics returns the current in-process metrics snapshot.
func (p *PulsarClient) GetMetrics() *Metrics {
	return p.metrics
}

// GetHealth returns the current health status of the plugin.
func (p *PulsarClient) GetHealth() *HealthStatus {
	if p.healthStatus == nil {
		return &HealthStatus{}
	}
	return p.healthStatus
}

// GetEnabledProducers returns all enabled producers
func (p *PulsarClient) GetEnabledProducers() []*conf.Producer {
	var enabled []*conf.Producer
	for _, producer := range p.config.Producers {
		if producer.Enabled {
			enabled = append(enabled, producer)
		}
	}
	return enabled
}

// GetEnabledConsumers returns all enabled consumers
func (p *PulsarClient) GetEnabledConsumers() []*conf.Consumer {
	var enabled []*conf.Consumer
	for _, consumer := range p.config.Consumers {
		if consumer.Enabled {
			enabled = append(enabled, consumer)
		}
	}
	return enabled
}
