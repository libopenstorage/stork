package controllers

import (
	"testing"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateSpecPath(t *testing.T) {
	// Create a new ResourceTransformation object
	transform := &v1alpha1.ResourceTransformation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-transform",
			Namespace: "default",
		},
		Spec: v1alpha1.ResourceTransformationSpec{
			Objects: []v1alpha1.TransformSpecs{
				{
					Resource: "apps/v1/Deployment",
					Paths: []v1alpha1.ResourcePaths{
						{
							Operation: v1alpha1.AddResourcePath,
							Type:      v1alpha1.StringResourceType,
							Path:      "/spec/template/spec/containers/0/image",
						},
					},
				},
			},
		},
	}

	// Create a new ResourceTransformationController
	controller := &ResourceTransformationController{
		client: nil, // Replace with your own implementation of the client
	}

	// Call the validateSpecPath function
	err := controller.validateSpecPath(transform)
	assert.NoError(t, err, "Failed to validate spec path")

	// Create a new ResourceTransformation object for a custom resource like elasticsearch
	transform = &v1alpha1.ResourceTransformation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cr-transform",
			Namespace: "default",
		},
		Spec: v1alpha1.ResourceTransformationSpec{
			Objects: []v1alpha1.TransformSpecs{
				{
					Resource: "elasticsearch/v1/Cluster",
					Paths: []v1alpha1.ResourcePaths{
						{
							Operation: v1alpha1.AddResourcePath,
							Type:      v1alpha1.StringResourceType,
							Path:      "/spec/template/spec/containers/0/image",
						},
					},
				},
			},
		},
	}

	// Call the validateSpecPath function
	err = controller.validateSpecPath(transform)
	assert.NoError(t, err, "Failed to validate spec path")

	// Update the ResourceTransformation object with json patch operation
	transform.Spec.Objects[0].Paths[0].Operation = v1alpha1.JsonResourcePatch

	// validateSpecPath function should fail for json patch operation
	err = controller.validateSpecPath(transform)
	assert.Error(t, err, "Spec path validation should fail for operation %s", v1alpha1.JsonResourcePatch)

	// Update the ResourceTransformation object with an unsupported operation
	transform.Spec.Objects[0].Paths[0].Operation = "invalid-operation"

	// Call the validateSpecPath function again
	err = controller.validateSpecPath(transform)
	assert.Error(t, err, "Spec path validation should fail for operation %s", "invalid-operation")

	// Update the ResourceTransformation object with an unsupported type
	transform.Spec.Objects[0].Paths[0].Operation = v1alpha1.AddResourcePath
	transform.Spec.Objects[0].Paths[0].Type = "invalid-type"

	// Call the validateSpecPath function again
	err = controller.validateSpecPath(transform)
	assert.Error(t, err, "Spec path validation should fail for type %s", "invalid-type")
}
