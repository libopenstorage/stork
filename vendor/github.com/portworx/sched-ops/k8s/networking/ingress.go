package networking

import (
	"context"
	"fmt"
	"time"

	schederrors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	v1beta1 "k8s.io/api/networking/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Ops is an interface to perform kubernetes related operations on the crd resources.
type IngressOps interface {
	// CreateIngress creates the given ingress
	CreateIngress(ingress *v1beta1.Ingress) (*v1beta1.Ingress, error)
	// UpdateIngress creates the given ingress
	UpdateIngress(ingress *v1beta1.Ingress) (*v1beta1.Ingress, error)
	// GetIngress returns the ingress given name and namespace
	GetIngress(name, namespace string) (*v1beta1.Ingress, error)
	// DeleteIngress deletes the given ingress
	DeleteIngress(name, namespace string) error
	// ValidateIngress validates the given ingress
	ValidateIngress(ingress *v1beta1.Ingress, timeout, retryInterval time.Duration) error
}

var NamespaceDefault = "default"

// CreateIngress creates the given ingress
func (c *Client) CreateIngress(ingress *v1beta1.Ingress) (*v1beta1.Ingress, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := ingress.Namespace
	if len(ns) == 0 {
		ns = NamespaceDefault
	}

	return c.networking.Ingresses(ns).Create(context.TODO(), ingress, metav1.CreateOptions{})
}

// UpdateIngress creates the given ingress
func (c *Client) UpdateIngress(ingress *v1beta1.Ingress) (*v1beta1.Ingress, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.networking.Ingresses(ingress.Namespace).Update(context.TODO(), ingress, metav1.UpdateOptions{})
}

// GetIngress returns the ingress given name and namespace
func (c *Client) GetIngress(name, namespace string) (*v1beta1.Ingress, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.networking.Ingresses(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// DeleteIngress deletes the given ingress
func (c *Client) DeleteIngress(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.networking.Ingresses(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// ValidateIngress validates the given ingress
func (c *Client) ValidateIngress(ingress *v1beta1.Ingress, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := c.initClient(); err != nil {
			return "", true, err
		}

		result, err := c.networking.Ingresses(ingress.Namespace).Get(context.TODO(), ingress.Name, metav1.GetOptions{})
		if result == nil {
			return "", true, err
		}
		if len(result.Status.LoadBalancer.Ingress) < 1 {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    ingress.Name,
				Cause: fmt.Sprintf("Failed to set load balancer for ingress. %sErr: %v", ingress.Name, err),
			}
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil

}
