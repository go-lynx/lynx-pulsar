package pulsar

import (
	"fmt"
	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx/pkg/factory"
	"github.com/go-lynx/lynx/plugins"
)

// init registers the Pulsar plugin with the global factory on import so the
// plugin manager can discover and load it.
func init() {
	factory.GlobalTypedFactory().RegisterPlugin(pluginName, confPrefix, func() plugins.Plugin {
		return NewPulsarClient()
	})
}

// GetPulsarClient returns the registered Pulsar plugin from the plugin manager,
// or an error if it is absent or of the wrong type.
func GetPulsarClient() (*PulsarClient, error) {
	plugin := lynx.Lynx().GetPluginManager().GetPlugin(pluginName)
	if client, ok := plugin.(*PulsarClient); ok && client != nil {
		return client, nil
	}
	return nil, fmt.Errorf("failed to get Pulsar client: plugin not found or type assertion failed")
}

// GetPulsarClientByName retrieves a registered Pulsar plugin by its plugin name.
// The go-lynx framework supports a single Pulsar plugin instance identified by
// pluginName. Pass an empty string or pluginName to get the default client.
//
// Returns an error when no matching plugin is found or the type assertion fails.
func GetPulsarClientByName(name string) (*PulsarClient, error) {
	if name == "" {
		name = pluginName
	}
	plugin := lynx.Lynx().GetPluginManager().GetPlugin(name)
	if client, ok := plugin.(*PulsarClient); ok && client != nil {
		return client, nil
	}
	return nil, fmt.Errorf("pulsar client %q not found or type assertion failed", name)
}
