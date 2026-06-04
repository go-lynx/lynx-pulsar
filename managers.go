package pulsar

import (
	"sync"
	"time"

	"github.com/go-lynx/lynx-pulsar/conf"
	"github.com/go-lynx/lynx/log"
)

// HealthChecker runs a periodic background health probe and records the latest
// result for CheckHealth/metrics to read.
type HealthChecker struct {
	interval   time.Duration
	stopChan   chan struct{}
	stopOnce   sync.Once // Protect against multiple close operations
	healthy    bool
	lastCheck  time.Time
	errorCount int
	lastError  error
	mu         sync.RWMutex
	stopped    bool
}

// NewHealthChecker creates a checker that probes every interval, starting healthy.
func NewHealthChecker(interval time.Duration) *HealthChecker {
	return &HealthChecker{
		interval: interval,
		stopChan: make(chan struct{}),
		healthy:  true,
	}
}

// Start launches the probe loop in a background goroutine.
func (h *HealthChecker) Start() {
	go h.run()
}

// Stop halts the probe loop; safe to call more than once.
func (h *HealthChecker) Stop() {
	h.mu.Lock()
	stopped := h.stopped
	h.mu.Unlock()

	if !stopped {
		h.stopOnce.Do(func() {
			close(h.stopChan)
			h.mu.Lock()
			h.stopped = true
			h.mu.Unlock()
		})
	}
}

func (h *HealthChecker) IsHealthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.healthy
}

func (h *HealthChecker) GetLastCheck() time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastCheck
}

func (h *HealthChecker) GetErrorCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.errorCount
}

func (h *HealthChecker) GetLastError() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastError
}

func (h *HealthChecker) run() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.performHealthCheck()
		case <-h.stopChan:
			return
		}
	}
}

// performHealthCheck is a placeholder probe that currently always reports
// healthy; a real implementation would ping the broker here.
func (h *HealthChecker) performHealthCheck() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastCheck = time.Now()
	h.healthy = true
	h.lastError = nil
}

// ConnectionManager tracks a best-effort connected flag for the Pulsar client.
type ConnectionManager struct {
	config    *conf.Connection
	connected bool
	stopChan  chan struct{}
	stopOnce  sync.Once // Protect against multiple close operations
	mu        sync.RWMutex
	stopped   bool
}

func NewConnectionManager(config *conf.Connection) *ConnectionManager {
	return &ConnectionManager{
		config:    config,
		connected: false,
		stopChan:  make(chan struct{}),
	}
}

// Start marks the connection active.
func (c *ConnectionManager) Start() {
	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
	log.Infof("Pulsar connection manager started")
}

// Stop marks the connection inactive; safe to call more than once.
func (c *ConnectionManager) Stop() {
	c.mu.Lock()
	c.connected = false
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	ch := c.stopChan
	c.mu.Unlock()
	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
	log.Infof("Pulsar connection manager stopped")
}

func (c *ConnectionManager) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// GetConnectionStats returns connection statistics.
func (c *ConnectionManager) GetConnectionStats() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]any{
		"connected":                 c.connected,
		"max_connections_per_host":  c.config.MaxConnectionsPerHost,
		"enable_connection_pooling": c.config.EnableConnectionPooling,
		"connection_timeout":        c.config.ConnectionTimeout.AsDuration(),
		"operation_timeout":         c.config.OperationTimeout.AsDuration(),
		"keep_alive_interval":       c.config.KeepAliveInterval.AsDuration(),
	}
}

// Reconnect re-marks the connection active.
func (c *ConnectionManager) Reconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Infof("Attempting to reconnect to Pulsar")
	c.connected = true
	return nil
}

// RetryManager manages retry behaviour for Pulsar operations.
type RetryManager struct {
	config *conf.Retry
	stats  map[string]any
	mu     sync.RWMutex
}

func NewRetryManager(config *conf.Retry) *RetryManager {
	return &RetryManager{
		config: config,
		stats:  make(map[string]any),
	}
}

// ShouldRetry reports whether another attempt is allowed: retries must be
// enabled and attempt must be below MaxAttempts.
func (r *RetryManager) ShouldRetry(attempt int, err error) bool {
	if !r.config.Enable {
		return false
	}
	return attempt < int(r.config.MaxAttempts)
}

// GetRetryDelay returns the backoff for the given attempt: InitialDelay scaled
// by RetryDelayMultiplier per attempt, capped at MaxDelay.
func (r *RetryManager) GetRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return r.config.InitialDelay.AsDuration()
	}

	delay := r.config.InitialDelay.AsDuration()
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * float64(r.config.RetryDelayMultiplier))
		if delay > r.config.MaxDelay.AsDuration() {
			delay = r.config.MaxDelay.AsDuration()
			break
		}
	}

	return delay
}

// RecordRetry logs a retry attempt for the named operation.
// err may be nil if the retry eventually succeeded.
func (r *RetryManager) RecordRetry(operation string, attempt int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stats[operation] == nil {
		r.stats[operation] = make(map[string]any)
	}
	if opStats, ok := r.stats[operation].(map[string]any); ok {
		opStats["attempts"] = attempt
		if err != nil {
			opStats["last_error"] = err.Error()
		} else {
			opStats["last_error"] = nil
		}
		opStats["last_retry"] = time.Now()
	}
}

// GetRetryStats returns retry statistics keyed by operation name.
func (r *RetryManager) GetRetryStats() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := make(map[string]any)
	for k, v := range r.stats {
		stats[k] = v
	}
	return stats
}
