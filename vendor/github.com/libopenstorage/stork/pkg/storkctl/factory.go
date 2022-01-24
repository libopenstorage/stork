package storkctl

import (
	"fmt"

	appsops "github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	dynamicops "github.com/portworx/sched-ops/k8s/dynamic"
	externalstorageops "github.com/portworx/sched-ops/k8s/externalstorage"
	ocpops "github.com/portworx/sched-ops/k8s/openshift"
	storkops "github.com/portworx/sched-ops/k8s/stork"
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
	allNamespaces bool
	namespace     string
	kubeconfig    string
	context       string
	outputFormat  string
	watch         bool
}

// Factory to be used for command line
type Factory interface {
	// BindFlags Binds command flags to the command
	BindFlags(flags *pflag.FlagSet)
	// BindGetFlags Binds command flags for the get subcommand
	BindGetFlags(flags *pflag.FlagSet)

	// AllNamespaces Retruns true if the all-namespaces flag was used
	AllNamespaces() bool
	// GetNamespace Gets the namespace used for the command
	GetNamespace() string
	// GetAllNamespaces Get all the namespaces that should be used for a command
	GetAllNamespaces() ([]string, error)
	// GetConfig Get the merged config for the server
	GetConfig() (*rest.Config, error)
	// RawConfig Gets the raw merged config for the server
	RawConfig() (clientcmdapi.Config, error)
	// UpdateConfig Updates the config to be used for API calls
	UpdateConfig() error
	// GetOutputFormat Get the output format
	GetOutputFormat() (string, error)
	// setOutputFormat Set the output format
	setOutputFormat(string)
	// setNamespace Set the namespace
	setNamespace(string)
	// IsWatchSet return true if -w/watch is passed
	IsWatchSet() bool
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
	flags.BoolVarP(&f.watch, "watch", "w", false, "watch stork resourrces")
}

func (f *factory) BindGetFlags(flags *pflag.FlagSet) {
	flags.BoolVarP(&f.allNamespaces, "all-namespaces", "", false, "If present, list the requested object(s) across all namespaces.\n"+
		"Namespace in current context is ignored even if specified with --namespace.")
}

func (f *factory) AllNamespaces() bool {
	return f.allNamespaces
}

func (f *factory) GetNamespace() string {
	return f.namespace
}

func (f *factory) GetAllNamespaces() ([]string, error) {
	allNamespaces := make([]string, 0)
	if f.allNamespaces {
		namespaces, err := core.Instance().ListNamespaces(nil)
		if err != nil {
			return nil, err
		}
		for _, ns := range namespaces.Items {
			allNamespaces = append(allNamespaces, ns.Name)
		}
	} else {
		allNamespaces = append(allNamespaces, f.GetNamespace())
	}
	return allNamespaces, nil
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

func (f *factory) IsWatchSet() bool {
	return f.watch
}

func (f *factory) UpdateConfig() error {
	config, err := f.GetConfig()
	if err != nil {
		return err
	}
	core.Instance().SetConfig(config)
	storkops.Instance().SetConfig(config)
	batch.Instance().SetConfig(config)
	ocpops.Instance().SetConfig(config)
	appsops.Instance().SetConfig(config)
	dynamicops.Instance().SetConfig(config)
	externalstorageops.Instance().SetConfig(config)
	return nil
}

func (f *factory) RawConfig() (clientcmdapi.Config, error) {
	config, err := f.getKubeconfig().RawConfig()
	if err != nil {
		return config, err
	}

	if f.context != "" {
		config.CurrentContext = f.context
	}
	return config, nil
}

func (f *factory) GetOutputFormat() (string, error) {
	switch f.outputFormat {
	case outputFormatTable, outputFormatYaml, outputFormatJSON:
		return f.outputFormat, nil
	default:
		return "", fmt.Errorf("unsupported output type %v", f.outputFormat)
	}
}

func (f *factory) setOutputFormat(outputFormat string) {
	f.outputFormat = outputFormat
}

func (f *factory) setNamespace(namespace string) {
	f.namespace = namespace
}
