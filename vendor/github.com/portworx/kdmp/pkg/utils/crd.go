package utils

import (
	"fmt"

	"github.com/portworx/sched-ops/k8s/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateCRD creates the given custom resource
func CreateCRD(resource apiextensions.CustomResource) error {
	scope := apiextensionsv1.NamespaceScoped
	if string(resource.Scope) == string(apiextensionsv1.ClusterScoped) {
		scope = apiextensionsv1.ClusterScoped
	}
	ignoreSchemaValidation := true
	crdName := fmt.Sprintf("%s.%s", resource.Plural, resource.Group)
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: crdName,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: resource.Group,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: resource.Version,
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							XPreserveUnknownFields: &ignoreSchemaValidation,
						},
					},
				},
			},
			Scope: scope,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Singular:   resource.Name,
				Plural:     resource.Plural,
				Kind:       resource.Kind,
				ShortNames: resource.ShortNames,
			},
		},
	}
	err := apiextensions.Instance().RegisterCRD(crd)
	if err != nil {
		return err
	}
	return nil
}
