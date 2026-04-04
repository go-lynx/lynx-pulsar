package pulsar

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	kratosconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx-pulsar/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Plugin metadata constants
const (
	pluginName        = "pulsar.client"
	pluginVersion     = "v1.6.0-beta"
	pluginDescription = "Apache Pulsar client plugin for lynx framework"
	confPrefix        = "lynx.pulsar"
)

// PulsarClient represents the main Pulsar client plugin instance
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
	closeOnce         sync.Once // Protect against multiple close operations
	closed            bool
	metrics           *Metrics
	healthStatus      *HealthStatus
	healthChecker     *HealthChecker
	connectionManager *ConnectionManager
	retryManager      *RetryManager
}

// NewPulsarClient creates a new Pulsar client plugin instance
func NewPulsarClient() *PulsarClient {
	c := &PulsarClient{
		config:       defaultPulsarConfig(),
		producers:    make(map[string]pulsar.Producer),
		consumers:    make(map[string]pulsar.Consumer),
		closeChan:    make(chan struct{}),
		closed:       false,
		metrics:      &Metrics{},
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
	log.Infof("initializing Apache Pulsar client")

	// Create Pulsar client
	clientOptions := p.buildClientOptions()
	client, err := pulsar.NewClient(clientOptions)
	if err != nil {
		return fmt.Errorf("failed to create Pulsar client: %w", err)
	}
	p.client = client

	// Initialize producers
	if err := p.initializeProducers(); err != nil {
		return fmt.Errorf("failed to initialize producers: %w", err)
	}

	// Initialize consumers
	if err := p.initializeConsumers(); err != nil {
		return fmt.Errorf("failed to initialize consumers: %w", err)
	}

	// Start health checker
	if p.config.Monitoring.EnableHealthCheck {
		p.healthChecker.Start()
	}

	// Start connection manager
	p.connectionManager.Start()

	if p.rt != nil {
		if err := p.rt.RegisterSharedResource(pluginName, p); err != nil {
			return fmt.Errorf("failed to register Pulsar shared resource: %w", err)
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
	}

	log.Infof("Apache Pulsar client successfully initialized")
	return nil
}

// CleanupTasks gracefully shuts down the plugin
func (p *PulsarClient) CleanupTasks() error {
	log.Infof("shutting down Apache Pulsar client plugin")

	// Signal background tasks to stop (protected against multiple calls)
	p.closeOnce.Do(func() {
		close(p.closeChan)
	})
	p.closed = true

	// Stop health checker
	if p.healthChecker != nil {
		p.healthChecker.Stop()
	}

	// Stop connection manager
	if p.connectionManager != nil {
		p.connectionManager.Stop()
	}

	// Close consumers
	p.consumerMutex.Lock()
	for name, consumer := range p.consumers {
		consumer.Close()
		log.Infof("consumer %s closed", name)
	}
	p.consumers = make(map[string]pulsar.Consumer)
	p.consumerMutex.Unlock()

	// Close producers
	p.producerMutex.Lock()
	for name, producer := range p.producers {
		producer.Close()
		log.Infof("producer %s closed", name)
	}
	p.producers = make(map[string]pulsar.Producer)
	p.producerMutex.Unlock()

	// Close client
	if p.client != nil {
		p.client.Close()
	}

	log.Infof("Apache Pulsar client plugin successfully shut down")
	return nil
}

// CheckHealth performs health check on Pulsar client
func (p *PulsarClient) CheckHealth() error {
	if p.client == nil {
		return fmt.Errorf("pulsar client not initialized")
	}

	// Check connection status
	if !p.connectionManager.IsConnected() {
		return fmt.Errorf("pulsar client not connected")
	}

	// Check producer status
	p.producerMutex.RLock()
	for name, producer := range p.producers {
		if producer == nil {
			log.Warnf("producer %s is nil", name)
		}
	}
	p.producerMutex.RUnlock()

	// Check consumer status
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
	}

	producer, err := p.client.CreateProducer(options)
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
		if config.Options.ReceiverQueueSize > 0 {
			options.ReceiverQueueSize = int(config.Options.ReceiverQueueSize)
		}
		if config.Options.NegativeAckDelay != nil {
			options.NackRedeliveryDelay = config.Options.NegativeAckDelay.AsDuration()
		}
		if config.Options.Properties != nil {
			options.Properties = config.Options.Properties
		}
	}

	consumer, err := p.client.Subscribe(options)
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
	return p.client
}

// IsConnected checks if the Pulsar client is connected
func (p *PulsarClient) IsConnected() bool {
	return !p.closed && p.client != nil && p.connectionManager.IsConnected()
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
