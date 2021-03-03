package apiextensions

import (
	"context"
	"fmt"
	"time"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// CRDOps is an interface to perfrom k8s Customer Resource operations
type CRDOps interface {
	// CreateCRD creates the given custom resource
	// This API will be deprecated soon. Use RegisterCRD instead
	CreateCRD(resource CustomResource) error
	// RegisterCRD creates the given custom resource
	RegisterCRD(crd *apiextensionsv1beta1.CustomResourceDefinition) error
	// UpdateCRD updates the existing crd
	UpdateCRD(crd *apiextensionsv1beta1.CustomResourceDefinition) (*apiextensionsv1beta1.CustomResourceDefinition, error)
	// GetCRD returns a crd by name
	GetCRD(name string, options metav1.GetOptions) (*apiextensionsv1beta1.CustomResourceDefinition, error)
	// ValidateCRD checks if the given CRD is registered
	ValidateCRD(resource CustomResource, timeout, retryInterval time.Duration) error
	// DeleteCRD deletes the CRD for the given complete name (plural.group)
	DeleteCRD(fullName string) error
	// ListCRDs list all the CRDs
	ListCRDs() (*apiextensionsv1beta1.CustomResourceDefinitionList, error)
}

// CustomResource is for creating a Kubernetes TPR/CRD
type CustomResource struct {
	// Name of the custom resource
	Name string
	// ShortNames are short names for the resource.  It must be all lowercase.
	ShortNames []string
	// Plural of the custom resource in plural
	Plural string
	// Group the custom resource belongs to
	Group string
	// Version which should be defined in a const above
	Version string
	// Scope of the CRD. Namespaced or cluster
	Scope apiextensionsv1beta1.ResourceScope
	// Kind is the serialized interface of the resource.
	Kind string
}

// CreateCRD creates the given custom resource
// This API will be deprecated soon. Use RegisterCRD instead
func (c *Client) CreateCRD(resource CustomResource) error {
	if err := c.initClient(); err != nil {
		return err
	}

	crdName := fmt.Sprintf("%s.%s", resource.Plural, resource.Group)
	crd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: crdName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   resource.Group,
			Version: resource.Version,
			Scope:   resource.Scope,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Singular:   resource.Name,
				Plural:     resource.Plural,
				Kind:       resource.Kind,
				ShortNames: resource.ShortNames,
			},
		},
	}

	_, err := c.extension.ApiextensionsV1beta1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
}

// RegisterCRD creates the given custom resource
func (c *Client) RegisterCRD(crd *apiextensionsv1beta1.CustomResourceDefinition) error {
	if err := c.initClient(); err != nil {
		return err
	}

	_, err := c.extension.ApiextensionsV1beta1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

// UpdateCRD updates the existing crd
func (c *Client) UpdateCRD(crd *apiextensionsv1beta1.CustomResourceDefinition) (*apiextensionsv1beta1.CustomResourceDefinition, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.extension.ApiextensionsV1beta1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
}

// GetCRD returns a crd by name
func (c *Client) GetCRD(name string, options metav1.GetOptions) (*apiextensionsv1beta1.CustomResourceDefinition, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.extension.ApiextensionsV1beta1().CustomResourceDefinitions().Get(context.TODO(), name, options)
}

// ValidateCRD checks if the given CRD is registered
func (c *Client) ValidateCRD(resource CustomResource, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}

	crdName := fmt.Sprintf("%s.%s", resource.Plural, resource.Group)
	return wait.PollImmediate(retryInterval, timeout, func() (bool, error) {
		crd, err := c.extension.ApiextensionsV1beta1().CustomResourceDefinitions().Get(context.TODO(), crdName, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1beta1.Established:
				if cond.Status == apiextensionsv1beta1.ConditionTrue {
					return true, nil
				}
			case apiextensionsv1beta1.NamesAccepted:
				if cond.Status == apiextensionsv1beta1.ConditionFalse {
					return false, fmt.Errorf("name conflict: %v", cond.Reason)
				}
			}
		}
		return false, nil
	})
}

// DeleteCRD deletes the CRD for the given complete name (plural.group)
func (c *Client) DeleteCRD(fullName string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.extension.ApiextensionsV1beta1().
		CustomResourceDefinitions().
		Delete(context.TODO(), fullName, metav1.DeleteOptions{PropagationPolicy: &deleteForegroundPolicy})
}

// ListCRDs list all CRD resources
func (c *Client) ListCRDs() (*apiextensionsv1beta1.CustomResourceDefinitionList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.extension.ApiextensionsV1beta1().
		CustomResourceDefinitions().
		List(context.TODO(), metav1.ListOptions{})
}
