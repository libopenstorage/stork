package gcp

import (
	"context"
	"fmt"
	"net/http"

	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/px-backup-api/pkg/kubeauth"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"k8s.io/client-go/rest"
	clientcmd "k8s.io/client-go/tools/clientcmd/api"
)

const (
	pluginName      = "gcp"
	credOrgIDConfig = "cred-org-id"
	credOwnerConfig = "cred-owner"
	credJSON        = "cred-json"
)

var (
	defaultScopes = []string{
		"https://www.googleapis.com/auth/cloud-platform",
		"https://www.googleapis.com/auth/userinfo.email",
	}
)

type gcp struct {
}

type gcpToken struct {
	tokenSource oauth2.TokenSource
}

// Init initializes the gcp auth plugin
func (g *gcp) Init() error {
	return nil
}

func (g *gcp) UpdateClient(
	conn *grpc.ClientConn,
	ctx context.Context,
	cloudCredentialName string,
	cloudCredentialUID string,
	orgID string,
	client *rest.Config,
	clientConfig *clientcmd.Config,
) (bool, string, error) {
	// GCP does not support returning kubeconfigs
	var emptyKubeconfig string
	if client.AuthProvider != nil {
		if client.AuthProvider.Name == pluginName {
			if cloudCredentialName == "" {
				return false, emptyKubeconfig, fmt.Errorf("CloudCredential not provided for GKE cluster")
			}
			cloudCredentialClient := api.NewCloudCredentialClient(conn)
			resp, err := cloudCredentialClient.Inspect(
				ctx,
				&api.CloudCredentialInspectRequest{
					Name:           cloudCredentialName,
					Uid: cloudCredentialUID,
					OrgId:          orgID,
					IncludeSecrets: true,
				},
			)
			if err != nil {
				return false, emptyKubeconfig, err
			}
			cloudCred := resp.GetCloudCredential()
			client.AuthProvider.Config = make(map[string]string)
			client.AuthProvider.Config[credOrgIDConfig] = cloudCred.GetOrgId()
			client.AuthProvider.Config[credOwnerConfig] = cloudCred.GetOwnership().GetOwner()
			client.AuthProvider.Config[credJSON] = cloudCred.GetCloudCredentialInfo().GetGoogleConfig().GetJsonKey()
			return true, emptyKubeconfig, nil
		} // else not a gcp kubeauth provider
	}
	return false, emptyKubeconfig, nil
}

func (g *gcp) newGCPAuthProvider(
	_ string,
	gcpConfig map[string]string,
	_ rest.AuthProviderConfigPersister,
) (rest.AuthProvider, error) {
	var creds *google.Credentials
	var err error
	if creds, err = google.CredentialsFromJSON(context.Background(), []byte(gcpConfig[credJSON]), defaultScopes...); err != nil {
		return nil, err
	}
	return &gcpToken{tokenSource: creds.TokenSource}, nil
}

func (g *gcpToken) Login() error { return nil }

func (g *gcpToken) WrapTransport(rt http.RoundTripper) http.RoundTripper {
	return &oauth2.Transport{Source: g.tokenSource, Base: rt}
}

func (g *gcp) UpdateClientByCredObject(
	cloudCred *api.CloudCredentialObject,
	client *rest.Config,
	clientConfig *clientcmd.Config,
) (bool, string, error) {
	// GCP does not support returning kubeconfigs
	var emptyKubeconfig string
	if client.AuthProvider != nil {
		if client.AuthProvider.Name == pluginName {
			if cloudCred == nil {
				return false, emptyKubeconfig, fmt.Errorf("CloudCredential not provided for GKE cluster")
			}
			client.AuthProvider.Config = make(map[string]string)
			client.AuthProvider.Config[credOrgIDConfig] = cloudCred.GetOrgId()
			client.AuthProvider.Config[credOwnerConfig] = cloudCred.GetOwnership().GetOwner()
			client.AuthProvider.Config[credJSON] = cloudCred.GetCloudCredentialInfo().GetGoogleConfig().GetJsonKey()
			return true, emptyKubeconfig, nil
		} // else not a gcp kubeauth provider
	}
	return false, emptyKubeconfig, nil
}

func (g *gcp) GetClient(
	cloudCredential *api.CloudCredentialObject,
	clusterName string,
	region string,
) (*kubeauth.PluginClient, error) {
	return nil, fmt.Errorf("not supported")
}

func (g *gcp) GetAllClients(
	cloudCredential *api.CloudCredentialObject,
	maxResult int64,
	config interface{},
) (map[string]*kubeauth.PluginClient, *string, error) {
	return nil, nil, fmt.Errorf("not supported")

}

func init() {
	g := &gcp{}
	if err := rest.RegisterAuthProviderPlugin(pluginName, g.newGCPAuthProvider); err != nil {
		logrus.Panicf("failed to initialize gcp auth plugin: %v", err)
	}
	if err := kubeauth.Register(pluginName, g); err != nil {
		logrus.Panicf("Error registering gcp auth plugin: %v", err)
	}
}
