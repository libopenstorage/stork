package core

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ServiceOps is an interface to perform k8s service operations
type ServiceOps interface {
	// GetService gets the service by the name
	GetService(string, string) (*corev1.Service, error)
	// CreateService creates the given service
	CreateService(*corev1.Service) (*corev1.Service, error)
	// DeleteService deletes the given service
	DeleteService(name, namespace string) error
	// ValidateDeletedService validates if given service is deleted
	ValidateDeletedService(string, string) error
	// DescribeService gets the service status
	DescribeService(string, string) (*corev1.ServiceStatus, error)
	// PatchService patches the current service with the given json path
	PatchService(name, namespace string, jsonPatch []byte) (*corev1.Service, error)
}

// CreateService creates the given service
func (c *Client) CreateService(service *corev1.Service) (*corev1.Service, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := service.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}

	return c.core.Services(ns).Create(service)
}

// DeleteService deletes the given service
func (c *Client) DeleteService(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.core.Services(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// GetService gets the service by the name
func (c *Client) GetService(svcName string, svcNS string) (*corev1.Service, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	if svcName == "" {
		return nil, fmt.Errorf("cannot return service obj without service name")
	}

	return c.core.Services(svcNS).Get(svcName, metav1.GetOptions{})
}

// DescribeService gets the service status
func (c *Client) DescribeService(svcName string, svcNamespace string) (*corev1.ServiceStatus, error) {
	svc, err := c.GetService(svcName, svcNamespace)
	if err != nil {
		return nil, err
	}
	return &svc.Status, err
}

// ValidateDeletedService validates if given service is deleted
func (c *Client) ValidateDeletedService(svcName string, svcNS string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	if svcName == "" {
		return fmt.Errorf("cannot validate service without service name")
	}

	_, err := c.core.Services(svcNS).Get(svcName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	return nil
}

// PatchService patches the current service with the given json path
func (c *Client) PatchService(name, namespace string, jsonPatch []byte) (*corev1.Service, error) {
	current, err := c.GetService(name, namespace)
	if err != nil {
		return nil, err
	}

	return c.core.Services(current.Namespace).Patch(current.Name, types.StrategicMergePatchType, jsonPatch)
}
