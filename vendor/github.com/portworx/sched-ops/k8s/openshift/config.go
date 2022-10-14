package openshift

import (
	"context"

	openshiftv1 "github.com/openshift/api/config/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConfigOps interface for openshift config operations
type ConfigOps interface {
	// GetClusterVersion get cluster version for the given name
	GetClusterVersion(string) (*openshiftv1.ClusterVersion, error)
}

// GetClusterVersion get cluster version for the given name
func (c *Client) GetClusterVersion(name string) (*openshiftv1.ClusterVersion, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ocpConfigClient.ConfigV1().ClusterVersions().Get(context.TODO(), name, metav1.GetOptions{})
}
