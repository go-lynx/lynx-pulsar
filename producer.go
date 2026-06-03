package pulsar

import (
	"context"
	"fmt"
	"time"

	pulsarlib "github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-lynx/lynx/log"
)

// Produce sends a single message to the topic owned by the default (first)
// enabled producer. key may be nil.
func (p *PulsarClient) Produce(ctx context.Context, topic string, key, value []byte) error {
	return p.ProduceWith(ctx, p.defaultProducerName(), topic, key, value)
}

// ProduceWithProperties sends a message with additional user-defined properties.
func (p *PulsarClient) ProduceWithProperties(ctx context.Context, topic string, key, value []byte, properties map[string]string) error {
	producer := p.GetProducer(p.defaultProducerName())
	if producer == nil {
		return fmt.Errorf("pulsar: no default producer available")
	}

	msg := &pulsarlib.ProducerMessage{
		Payload:    value,
		Properties: properties,
	}
	if len(key) > 0 {
		msg.Key = string(key)
	}

	start := time.Now()
	_, err := producer.Send(ctx, msg)
	if err != nil {
		p.prom.producerFailed.Inc()
		log.Errorf("pulsar: failed to send message with properties to topic %s: %v", topic, err)
		return fmt.Errorf("pulsar: failed to send message: %w", err)
	}

	p.prom.producerSent.Inc()
	p.prom.producerLatency.Observe(time.Since(start).Seconds())
	return nil
}

// ProduceAsync sends a message asynchronously. The callback is invoked with the
// message ID and any error once the broker acknowledges (or rejects) the message.
func (p *PulsarClient) ProduceAsync(ctx context.Context, topic string, key, value []byte, callback func(pulsarlib.MessageID, *pulsarlib.ProducerMessage, error)) error {
	producer := p.GetProducer(p.defaultProducerName())
	if producer == nil {
		return fmt.Errorf("pulsar: no default producer available")
	}

	msg := &pulsarlib.ProducerMessage{Payload: value}
	if len(key) > 0 {
		msg.Key = string(key)
	}

	producer.SendAsync(ctx, msg, func(id pulsarlib.MessageID, pm *pulsarlib.ProducerMessage, err error) {
		if err != nil {
			p.prom.producerFailed.Inc()
		} else {
			p.prom.producerSent.Inc()
		}
		if callback != nil {
			callback(id, pm, err)
		}
	})
	return nil
}

// ProduceBatch sends a slice of pre-built messages via the default producer.
func (p *PulsarClient) ProduceBatch(ctx context.Context, topic string, messages []*pulsarlib.ProducerMessage) error {
	producer := p.GetProducer(p.defaultProducerName())
	if producer == nil {
		return fmt.Errorf("pulsar: no default producer available")
	}

	for i, msg := range messages {
		start := time.Now()
		if _, err := producer.Send(ctx, msg); err != nil {
			p.prom.producerFailed.Inc()
			return fmt.Errorf("pulsar: batch send failed at message %d: %w", i, err)
		}
		p.prom.producerSent.Inc()
		p.prom.producerLatency.Observe(time.Since(start).Seconds())
	}
	return nil
}

// ProduceWith sends a message using the producer identified by producerName.
func (p *PulsarClient) ProduceWith(ctx context.Context, producerName, topic string, key, value []byte) error {
	p.producerMutex.RLock()
	producer, ok := p.producers[producerName]
	p.producerMutex.RUnlock()
	if !ok || producer == nil {
		return fmt.Errorf("pulsar: producer %q not found", producerName)
	}

	msg := &pulsarlib.ProducerMessage{Payload: value}
	if len(key) > 0 {
		msg.Key = string(key)
	}

	start := time.Now()
	if _, err := producer.Send(ctx, msg); err != nil {
		p.prom.producerFailed.Inc()
		log.Errorf("pulsar: producer %s failed to send to topic %s: %v", producerName, topic, err)
		return fmt.Errorf("pulsar: failed to send message: %w", err)
	}

	p.prom.producerSent.Inc()
	p.prom.producerLatency.Observe(time.Since(start).Seconds())
	return nil
}

// GetProducer returns the underlying Pulsar producer for the given name, or nil.
func (p *PulsarClient) GetProducer(name string) pulsarlib.Producer {
	p.producerMutex.RLock()
	defer p.producerMutex.RUnlock()
	return p.producers[name]
}

// IsProducerReady reports whether the named producer is registered and non-nil.
func (p *PulsarClient) IsProducerReady(name string) bool {
	p.producerMutex.RLock()
	defer p.producerMutex.RUnlock()
	producer, ok := p.producers[name]
	return ok && producer != nil
}

// Close closes and removes the named producer.
func (p *PulsarClient) Close(name string) error {
	p.producerMutex.Lock()
	producer, ok := p.producers[name]
	if ok {
		delete(p.producers, name)
	}
	p.producerMutex.Unlock()

	if ok && producer != nil {
		producer.Close()
	}
	return nil
}

// defaultProducerName returns the name of the first enabled producer, or an
// empty string when none is configured.
func (p *PulsarClient) defaultProducerName() string {
	p.producerMutex.RLock()
	defer p.producerMutex.RUnlock()
	for name := range p.producers {
		return name
	}
	return ""
}
