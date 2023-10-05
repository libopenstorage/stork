package operator

import (
	"context"

	portworxv1 "github.com/libopenstorage/operator/pkg/apis/portworx/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PortworxDiagOps is an interface to perfrom k8s PortworxDiag operations
type PortworxDiagOps interface {
	// CreatePortworxDiag creates the given PortworxDiag
	CreatePortworxDiag(*portworxv1.PortworxDiag) (*portworxv1.PortworxDiag, error)
	// UpdatePortworxDiag updates the given PortworxDiag
	UpdatePortworxDiag(*portworxv1.PortworxDiag) (*portworxv1.PortworxDiag, error)
	// GetPortworxDiag gets the PortworxDiag with given name and namespace
	GetPortworxDiag(string, string) (*portworxv1.PortworxDiag, error)
	// ListPortworxDiags lists all the PortworxDiags
	ListPortworxDiags(string) (*portworxv1.PortworxDiagList, error)
	// DeletePortworxDiag deletes the given PortworxDiag
	DeletePortworxDiag(string, string) error
	// UpdatePortworxDiagStatus update the status of given PortworxDiag
	UpdatePortworxDiagStatus(*portworxv1.PortworxDiag) (*portworxv1.PortworxDiag, error)
}

// CreatePortworxDiag creates the given PortworxDiag
func (c *Client) CreatePortworxDiag(diag *portworxv1.PortworxDiag) (*portworxv1.PortworxDiag, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := diag.Namespace
	if len(ns) == 0 {
		ns = metav1.NamespaceDefault
	}

	return c.ost.PortworxV1().PortworxDiags(ns).Create(context.TODO(), diag, metav1.CreateOptions{})
}

// UpdatePortworxDiag updates the given PortworxDiag
func (c *Client) UpdatePortworxDiag(diag *portworxv1.PortworxDiag) (*portworxv1.PortworxDiag, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.ost.PortworxV1().PortworxDiags(diag.Namespace).Update(context.TODO(), diag, metav1.UpdateOptions{})
}

// GetPortworxDiag gets the PortworxDiag with given name and namespace
func (c *Client) GetPortworxDiag(name, namespace string) (*portworxv1.PortworxDiag, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ost.PortworxV1().PortworxDiags(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListPortworxDiags lists all the PortworxDiags
func (c *Client) ListPortworxDiags(namespace string) (*portworxv1.PortworxDiagList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ost.PortworxV1().PortworxDiags(namespace).List(context.TODO(), metav1.ListOptions{})
}

// DeletePortworxDiag deletes the given PortworxDiag
func (c *Client) DeletePortworxDiag(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.ost.PortworxV1().PortworxDiags(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// UpdatePortworxDiagStatus update the status of given PortworxDiag
func (c *Client) UpdatePortworxDiagStatus(diag *portworxv1.PortworxDiag) (*portworxv1.PortworxDiag, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ost.PortworxV1().PortworxDiags(diag.Namespace).UpdateStatus(context.TODO(), diag, metav1.UpdateOptions{})
}
