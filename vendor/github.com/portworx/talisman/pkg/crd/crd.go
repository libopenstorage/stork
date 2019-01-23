package crd

import (
	"fmt"
	"time"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// CustomResource is for creating a Kubernetes TPR/CRD
type CustomResource struct {
	// Name of the custom resource
	Name string

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

// Context hold the clientsets used for creating and watching custom resources
type Context struct {
	Clientset             kubernetes.Interface
	APIExtensionClientset apiextensionsclient.Interface
	Interval              time.Duration
	Timeout               time.Duration
}

// CreateCRD creates the given custom resources definitions and waits for their init
func CreateCRD(context Context, resources []CustomResource) error {
	var lastErr error
	for _, resource := range resources {
		err := createCRD(context, resource)
		if err != nil {
			lastErr = err
		}
	}

	for _, resource := range resources {
		if err := waitForCRDInit(context, resource); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

func createCRD(context Context, resource CustomResource) error {
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
				Singular: resource.Name,
				Plural:   resource.Plural,
				Kind:     resource.Kind,
			},
		},
	}

	_, err := context.APIExtensionClientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create %s CRD: %+v", resource.Name, err)
		}
	}
	return nil
}

func waitForCRDInit(context Context, resource CustomResource) error {
	crdName := fmt.Sprintf("%s.%s", resource.Plural, resource.Group)
	return wait.Poll(context.Interval, context.Timeout, func() (bool, error) {
		crd, err := context.APIExtensionClientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(crdName, metav1.GetOptions{})
		if err != nil {
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
