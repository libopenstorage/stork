package kubeauth

import (
	"context"

	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"k8s.io/client-go/rest"
	clientcmd "k8s.io/client-go/tools/clientcmd/api"
)

// PluginClient returns a k8s client returned by one of the
// supported plugins
type PluginClient struct {
	// Kubeconfig is the string representation of kubeconfig
	Kubeconfig string
	//  Restclient is the rest k8s client
	Rest *rest.Config
	// Uid uniquely identifies the managed cluster by the cloud provider
	Uid string
	// k8s version
	Version string
}

// Plugin is the interface the plugins need to implement
type Plugin interface {
	UpdateClient(
		conn *grpc.ClientConn,
		ctx context.Context,
		cloudCredentialName string,
		cloudCredentialUID string,
		orgID string,
		restConfig *rest.Config,
		clientConfig *clientcmd.Config,
	) (bool, string, error)

	UpdateClientByCredObject(
		cloudCred *api.CloudCredentialObject,
		restConfig *rest.Config,
		clientConfig *clientcmd.Config,
	) (bool, string, error)

	GetClient(
		cloudCred *api.CloudCredentialObject,
		clusterName string,
		region string,
	) (*PluginClient, error)

	GetAllClients(
		cloudCred *api.CloudCredentialObject,
		maxResults int64,
		config interface{},
	) (map[string]*PluginClient, *string, error)
}

var (
	plugins = make(map[string]Plugin)
)

// Register registers the given auth plugin
func Register(name string, p Plugin) error {
	logrus.Infof("Registering auth plugin: %v", name)
	plugins[name] = p
	return nil
}

// UpdateClient Updates the k8s client config with the required info
// from the cloud credential. It will return the new kubeconfig with
// which the client was updated
func UpdateClient(
	conn *grpc.ClientConn,
	ctx context.Context,
	cloudCredentialName string,
	cloudCredentialUID string,
	orgID string,
	restConfig *rest.Config,
	clientConfig *clientcmd.Config,
) (string, error) {
	for _, plugin := range plugins {
		updated, kubeconfig, err := plugin.UpdateClient(conn, ctx, cloudCredentialName, cloudCredentialUID, orgID, restConfig, clientConfig)
		if err != nil {
			return "", err
		}
		if updated {
			return kubeconfig, nil
		}
	}
	return "", nil
}

// UpdateClientByCredObject Updates the k8s client config with the required info
// from the provided cloud credential object
func UpdateClientByCredObject(
	cloudCred *api.CloudCredentialObject,
	restConfig *rest.Config,
	clientConfig *clientcmd.Config,
) (string, error) {
	for _, plugin := range plugins {
		updated, kubeconfig, err := plugin.UpdateClientByCredObject(cloudCred, restConfig, clientConfig)
		if err != nil {
			return "", err
		}
		if updated {
			return kubeconfig, nil
		}
	}
	return "", nil
}

// GetClient gets the k8s client config from the cloud credential
// The provided cloud credentials should have sufficient
// permissions to fetch a token using the cloud's SDK APIs
func GetClient(cloudCred *api.CloudCredentialObject, clusterName string, region string) (*PluginClient, error) {
	var getErr error
	for _, plugin := range plugins {
		client, err := plugin.GetClient(cloudCred, clusterName, region)
		if err == nil {
			return client, nil
		}
		getErr = multierr.Append(getErr, err)

	}
	return nil, getErr

}

// GetAllClients gets the k8s client config for all the clusters
// which the provided cloud credential has access to.
// The provided cloud credentials should have sufficient
// permissions to fetch a token using the cloud's SDK APIs
// TODO: In future API needs to have options for accept needed params for GKE and Azure 
// cluster scan
func GetAllClients(
	cloudCred *api.CloudCredentialObject,
	maxResult int64,
	config interface{},
) (map[string]*PluginClient, *string, error) {
	var getErr, err error
	var nextToken *string
	var clients map[string]*PluginClient
	for _, plugin := range plugins {
		clients, nextToken, err = plugin.GetAllClients(cloudCred, maxResult, config)
		if err == nil {
			return clients, nextToken, nil
		}
		getErr = multierr.Append(getErr, err)

	}
	return nil, nextToken, getErr
}

// ValidateConfig validates the provided rest config
func ValidateConfig(restConfig *rest.Config) (bool, error) {
	// Check if the provided config has expired
	coreInst, err := core.NewForConfig(restConfig)
	if err != nil {
		return false, err
	}
	_, err = coreInst.GetVersion()
	if err == nil {
		return true, nil
	}

	return false, err
}
