package pulsar

import (
	"context"
	"fmt"
	"time"

	pulsarlib "github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-lynx/lynx/log"
)

// Subscribe creates a push-consumer loop using the default (first) enabled
// consumer and dispatches each message to handler. It blocks until ctx is
// cancelled or a fatal receive error occurs.
func (p *PulsarClient) Subscribe(ctx context.Context, topics []string, handler MessageHandler) error {
	return p.SubscribeWith(ctx, p.defaultConsumerName(), topics, handler)
}

// SubscribeWith starts a receive loop for the consumer identified by
// consumerName. The handler is called for each message; a non-nil return value
// triggers a negative acknowledgement (redelivery). Panics inside the handler
// are recovered and counted as failures.
func (p *PulsarClient) SubscribeWith(ctx context.Context, consumerName string, _ []string, handler MessageHandler) error {
	p.consumerMutex.RLock()
	consumer, ok := p.consumers[consumerName]
	p.consumerMutex.RUnlock()
	if !ok || consumer == nil {
		return fmt.Errorf("pulsar: consumer %q not found", consumerName)
	}
	if handler == nil {
		return fmt.Errorf("pulsar: message handler must not be nil")
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, open := <-consumer.Chan():
				if !open {
					return
				}
				p.dispatchMessage(ctx, consumer, consumerName, msg, handler)
			}
		}
	}()
	return nil
}

// SubscribeWithRegex creates a push-consumer that matches topics by a regular
// expression pattern and starts the same receive loop as SubscribeWith.
func (p *PulsarClient) SubscribeWithRegex(ctx context.Context, topicsPattern string, handler MessageHandler) error {
	if p.client == nil {
		return fmt.Errorf("pulsar: client not initialised")
	}
	if handler == nil {
		return fmt.Errorf("pulsar: message handler must not be nil")
	}

	consumer, err := p.client.Subscribe(pulsarlib.ConsumerOptions{
		TopicsPattern:    topicsPattern,
		SubscriptionName: "lynx-regex-subscription",
	})
	if err != nil {
		return fmt.Errorf("pulsar: failed to subscribe with regex %q: %w", topicsPattern, err)
	}

	name := "regex:" + topicsPattern
	p.consumerMutex.Lock()
	p.consumers[name] = consumer
	p.consumerMutex.Unlock()

	go func() {
		defer func() {
			p.consumerMutex.Lock()
			delete(p.consumers, name)
			p.consumerMutex.Unlock()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, open := <-consumer.Chan():
				if !open {
					return
				}
				p.dispatchMessage(ctx, consumer, name, msg, handler)
			}
		}
	}()
	return nil
}

// GetConsumer returns the underlying Pulsar consumer for the given name, or nil.
func (p *PulsarClient) GetConsumer(name string) pulsarlib.Consumer {
	p.consumerMutex.RLock()
	defer p.consumerMutex.RUnlock()
	return p.consumers[name]
}

// IsConsumerReady reports whether the named consumer is registered and non-nil.
func (p *PulsarClient) IsConsumerReady(name string) bool {
	p.consumerMutex.RLock()
	defer p.consumerMutex.RUnlock()
	c, ok := p.consumers[name]
	return ok && c != nil
}

// Unsubscribe cancels the subscription of the named consumer and removes it.
func (p *PulsarClient) Unsubscribe(name string) error {
	p.consumerMutex.Lock()
	consumer, ok := p.consumers[name]
	if ok {
		delete(p.consumers, name)
	}
	p.consumerMutex.Unlock()

	if ok && consumer != nil {
		return consumer.Unsubscribe()
	}
	return nil
}

// dispatchMessage invokes the user handler with panic recovery. On error or
// panic the message is negatively acknowledged for redelivery.
func (p *PulsarClient) dispatchMessage(
	ctx context.Context,
	consumer pulsarlib.Consumer,
	consumerName string,
	msg pulsarlib.Message,
	handler MessageHandler,
) {
	err := p.invokeHandler(ctx, consumerName, msg, handler)
	if err != nil {
		consumer.Nack(msg)
		return
	}
	consumer.Ack(msg) //nolint:errcheck
}

// invokeHandler calls handler with panic recovery and records metrics.
// It returns an error if the handler returned an error or panicked.
func (p *PulsarClient) invokeHandler(
	ctx context.Context,
	consumerName string,
	msg pulsarlib.Message,
	handler MessageHandler,
) (handlerErr error) {
	start := time.Now()

	func() {
		defer func() {
			if rec := recover(); rec != nil {
				p.prom.consumerFailed.Inc()
				log.Errorf("pulsar: panic in message handler consumer=%s topic=%s panic=%v",
					consumerName, msg.Topic(), rec)
				handlerErr = fmt.Errorf("handler panic: %v", rec)
			}
		}()
		handlerErr = handler(ctx, msg)
	}()

	if handlerErr != nil {
		p.prom.consumerFailed.Inc()
		log.Errorf("pulsar: handler error consumer=%s topic=%s err=%v",
			consumerName, msg.Topic(), handlerErr)
		return handlerErr
	}

	p.prom.consumerReceived.Inc()
	p.prom.consumerLatency.Observe(time.Since(start).Seconds())
	return nil
}

// defaultConsumerName returns the name of the first registered consumer, or
// an empty string when none is available.
func (p *PulsarClient) defaultConsumerName() string {
	p.consumerMutex.RLock()
	defer p.consumerMutex.RUnlock()
	for name := range p.consumers {
		return name
	}
	return ""
}
