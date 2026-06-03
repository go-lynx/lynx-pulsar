package pulsar

import (
	"fmt"
	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx/pkg/factory"
	"github.com/go-lynx/lynx/plugins"
)

// init function registers the Apache Pulsar plugin to the global plugin factory.
// This function is automatically called when the package is imported.
// It creates a new PulsarClient instance and registers it to the plugin factory with the configured plugin name and configuration prefix.
func init() {
	// Call the RegisterPlugin method of the global plugin factory for plugin registration
	// Pass in the plugin name, configuration prefix, and a function that returns a plugins.Plugin interface instance
	factory.GlobalTypedFactory().RegisterPlugin(pluginName, confPrefix, func() plugins.Plugin {
		// Create and return a new PulsarClient instance
		return NewPulsarClient()
	})
}

// GetPulsarClient gets the Apache Pulsar client instance from the plugin manager.
// This function provides access to the underlying Pulsar client for other parts of the application
// that may need to use message queue functionality.
//
// Returns:
//   - *PulsarClient: Configured Apache Pulsar client instance
//   - error: Error if the plugin is not properly initialized or if the plugin manager cannot find the Pulsar plugin.
func GetPulsarClient() (*PulsarClient, error) {
	// Get the plugin with the specified name from the application's plugin manager,
	// convert it to *PulsarClient type, and return it
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
