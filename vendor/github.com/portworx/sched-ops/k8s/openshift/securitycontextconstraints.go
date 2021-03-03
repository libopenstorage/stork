package openshift

import (
	"context"

	ocpsecurityv1api "github.com/openshift/api/security/v1"
	ocpsecurityv1client "github.com/openshift/client-go/security/clientset/versioned/typed/security/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SecurityContextConstraintsOps is an interface to list, get and update security context constraints
type SecurityContextConstraintsOps interface {
	// ListSecurityContextConstraints returns the list of all SecurityContextConstraints, and an error if there is any.
	ListSecurityContextConstraints() (*ocpsecurityv1api.SecurityContextConstraintsList, error)
	// GetSecurityContextConstraints takes name of the securityContextConstraints and returns the corresponding securityContextConstraints object, and an error if there is any.
	GetSecurityContextConstraints(string) (*ocpsecurityv1api.SecurityContextConstraints, error)
	// UpdateSecurityContextConstraints takes the representation of a securityContextConstraints and updates it. Returns the server's representation of the securityContextConstraints, and an error, if there is any.
	UpdateSecurityContextConstraints(*ocpsecurityv1api.SecurityContextConstraints) (*ocpsecurityv1api.SecurityContextConstraints, error)
}

func (c *Client) getOcpSecurityClient() ocpsecurityv1client.SecurityV1Interface {
	return c.ocpSecurityClient.SecurityV1()
}

// ListSecurityContextConstraints returns the list of all SecurityContextConstraints, and an error if there is any.
func (c *Client) ListSecurityContextConstraints() (result *ocpsecurityv1api.SecurityContextConstraintsList, err error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.getOcpSecurityClient().SecurityContextConstraints().List(context.TODO(), metav1.ListOptions{})
}

// GetSecurityContextConstraints takes name of the securityContextConstraints and returns the corresponding securityContextConstraints object, and an error if there is any.
func (c *Client) GetSecurityContextConstraints(name string) (result *ocpsecurityv1api.SecurityContextConstraints, err error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.getOcpSecurityClient().SecurityContextConstraints().Get(context.TODO(), name, metav1.GetOptions{})
}

// UpdateSecurityContextConstraints takes the representation of a securityContextConstraints and updates it. Returns the server's representation of the securityContextConstraints, and an error, if there is any.
func (c *Client) UpdateSecurityContextConstraints(securityContextConstraints *ocpsecurityv1api.SecurityContextConstraints) (result *ocpsecurityv1api.SecurityContextConstraints, err error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.getOcpSecurityClient().SecurityContextConstraints().Update(context.TODO(), securityContextConstraints, metav1.UpdateOptions{})
}
