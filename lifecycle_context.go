package pulsar

import (
	"context"
	"fmt"

	"github.com/go-lynx/lynx/plugins"
)

func (p *PulsarClient) InitializeContext(ctx context.Context, plugin plugins.Plugin, rt plugins.Runtime) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pulsar initialize canceled before execution: %w", err)
	}
	return p.BasePlugin.Initialize(plugin, rt)
}

func (p *PulsarClient) StartContext(ctx context.Context, _ plugins.Plugin) error {
	return p.startupWithContext(ctx)
}

func (p *PulsarClient) StopContext(ctx context.Context, _ plugins.Plugin) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pulsar stop canceled before execution: %w", err)
	}
	return p.cleanupWithContext(ctx)
}

func (p *PulsarClient) IsContextAware() bool {
	return true
}

func (p *PulsarClient) PluginProtocol() plugins.PluginProtocol {
	protocol := p.BasePlugin.PluginProtocol()
	protocol.ContextLifecycle = true
	return protocol
}
