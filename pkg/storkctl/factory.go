package storkctl

import (
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const (
	outputFormatTable = "table"
	outputFormatYaml  = "yaml"
	outputFormatJSON  = "json"
)

type factory struct {
	namespace    string
	kubeconfig   string
	context      string
	outputFormat string
}

// Factory to be used for command line
type Factory interface {
	// BindFlags Binds command flags to the command
	BindFlags(flags *pflag.FlagSet)

	// GetNamespace Gets the namespace used for the command
	GetNamespace() string
	// GetConfig Get the merged config for the server
	GetConfig() (*rest.Config, error)
	// RawConfig Gets the raw merged config for the server
	RawConfig() (clientcmdapi.Config, error)
	// GetOutputFormat Get the output format
	GetOutputFormat() (string, error)
	// SetOutputFormat Set the output format
	SetOutputFormat(string)
}

// NewFactory Return a new factory interface that can be used by commands
func NewFactory() Factory {
	return &factory{}
}

func (f *factory) BindFlags(flags *pflag.FlagSet) {
	flags.StringVarP(&f.namespace, "namespace", "n", "default", "If present, the namespace scope for this CLI request")
	flags.StringVar(&f.kubeconfig, "kubeconfig", "", "Path to the kubeconfig file to use for CLI requests")
	flags.StringVar(&f.context, "context", "", "The name of the kubeconfig context to use")
	flags.StringVarP(&f.outputFormat, "output", "o", outputFormatTable, "Output format. One of: table|json|yaml")
}

func (f *factory) GetNamespace() string {
	return f.namespace
}

func (f *factory) getKubeconfig() clientcmd.ClientConfig {
	configLoadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configLoadingRules.ExplicitPath = f.kubeconfig

	configOverrides := &clientcmd.ConfigOverrides{
		CurrentContext: f.context,
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(configLoadingRules, configOverrides)
}

func (f *factory) GetConfig() (*rest.Config, error) {
	return f.getKubeconfig().ClientConfig()
}

func (f *factory) RawConfig() (clientcmdapi.Config, error) {
	return f.getKubeconfig().RawConfig()
}

func (f *factory) GetOutputFormat() (string, error) {
	switch f.outputFormat {
	case outputFormatTable, outputFormatYaml, outputFormatJSON:
		return f.outputFormat, nil
	default:
		return "", fmt.Errorf("Unsupported output type %v", f.outputFormat)
	}
}

func (f *factory) SetOutputFormat(outputFormat string) {
	f.outputFormat = outputFormat
}
