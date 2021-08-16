package apiextensions

import (
	"context"
	"fmt"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// CRDOps is an interface to perfrom k8s Customer Resource operations
type CRDOps interface {
	// RegisterCRD creates the given custom resource
	RegisterCRD(crd *apiextensionsv1.CustomResourceDefinition) error
	// UpdateCRD updates the existing crd
	UpdateCRD(crd *apiextensionsv1.CustomResourceDefinition) (*apiextensionsv1.CustomResourceDefinition, error)
	// GetCRD returns a crd by complete name (plural.group)
	GetCRD(name string, options metav1.GetOptions) (*apiextensionsv1.CustomResourceDefinition, error)
	// ValidateCRD checks if the given CRD is registered. The given name should be
	// the complete name (plural.group) Eg: storageclusters.core.libopenstorage.org
	ValidateCRD(name string, timeout, retryInterval time.Duration) error
	// DeleteCRD deletes the CRD for the given complete name (plural.group)
	DeleteCRD(name string) error
	// ListCRDs list all the CRDs
	ListCRDs() (*apiextensionsv1.CustomResourceDefinitionList, error)
}

// RegisterCRD creates the given custom resource
func (c *Client) RegisterCRD(crd *apiextensionsv1.CustomResourceDefinition) error {
	if err := c.initClient(); err != nil {
		return err
	}

	_, err := c.extension.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

// UpdateCRD updates the existing crd
func (c *Client) UpdateCRD(crd *apiextensionsv1.CustomResourceDefinition) (*apiextensionsv1.CustomResourceDefinition, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.extension.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
}

// GetCRD returns a crd by complete name (plural.group)
func (c *Client) GetCRD(name string, options metav1.GetOptions) (*apiextensionsv1.CustomResourceDefinition, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.extension.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), name, options)
}

// ValidateCRD checks if the given CRD is registered. The given name should be
// the complete name (plural.group) Eg: storageclusters.core.libopenstorage.org
func (c *Client) ValidateCRD(name string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return wait.PollImmediate(retryInterval, timeout, func() (bool, error) {
		crd, err := c.extension.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1.Established:
				if cond.Status == apiextensionsv1.ConditionTrue {
					return true, nil
				}
			case apiextensionsv1.NamesAccepted:
				if cond.Status == apiextensionsv1.ConditionFalse {
					return false, fmt.Errorf("name conflict: %v", cond.Reason)
				}
			}
		}
		return false, nil
	})
}

// DeleteCRD deletes the CRD for the given complete name (plural.group)
func (c *Client) DeleteCRD(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.extension.ApiextensionsV1().
		CustomResourceDefinitions().
		Delete(context.TODO(), name, metav1.DeleteOptions{PropagationPolicy: &deleteForegroundPolicy})
}

// ListCRDs list all CRD resources
func (c *Client) ListCRDs() (*apiextensionsv1.CustomResourceDefinitionList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.extension.ApiextensionsV1().
		CustomResourceDefinitions().
		List(context.TODO(), metav1.ListOptions{})
}
