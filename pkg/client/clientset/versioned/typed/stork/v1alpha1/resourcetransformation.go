/*
Copyright 2018 Openstorage.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by client-gen. DO NOT EDIT.

package v1alpha1

import (
	"context"
	"time"

	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	scheme "github.com/libopenstorage/stork/pkg/client/clientset/versioned/scheme"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// ResourceTransformationsGetter has a method to return a ResourceTransformationInterface.
// A group's client should implement this interface.
type ResourceTransformationsGetter interface {
	ResourceTransformations(namespace string) ResourceTransformationInterface
}

// ResourceTransformationInterface has methods to work with ResourceTransformation resources.
type ResourceTransformationInterface interface {
	Create(ctx context.Context, resourceTransformation *v1alpha1.ResourceTransformation, opts v1.CreateOptions) (*v1alpha1.ResourceTransformation, error)
	Update(ctx context.Context, resourceTransformation *v1alpha1.ResourceTransformation, opts v1.UpdateOptions) (*v1alpha1.ResourceTransformation, error)
	UpdateStatus(ctx context.Context, resourceTransformation *v1alpha1.ResourceTransformation, opts v1.UpdateOptions) (*v1alpha1.ResourceTransformation, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.ResourceTransformation, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.ResourceTransformationList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.ResourceTransformation, err error)
	ResourceTransformationExpansion
}

// resourceTransformations implements ResourceTransformationInterface
type resourceTransformations struct {
	client rest.Interface
	ns     string
}

// newResourceTransformations returns a ResourceTransformations
func newResourceTransformations(c *StorkV1alpha1Client, namespace string) *resourceTransformations {
	return &resourceTransformations{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the resourceTransformation, and returns the corresponding resourceTransformation object, and an error if there is any.
func (c *resourceTransformations) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.ResourceTransformation, err error) {
	result = &v1alpha1.ResourceTransformation{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("resourcetransformations").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of ResourceTransformations that match those selectors.
func (c *resourceTransformations) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.ResourceTransformationList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1alpha1.ResourceTransformationList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("resourcetransformations").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested resourceTransformations.
func (c *resourceTransformations) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("resourcetransformations").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a resourceTransformation and creates it.  Returns the server's representation of the resourceTransformation, and an error, if there is any.
func (c *resourceTransformations) Create(ctx context.Context, resourceTransformation *v1alpha1.ResourceTransformation, opts v1.CreateOptions) (result *v1alpha1.ResourceTransformation, err error) {
	result = &v1alpha1.ResourceTransformation{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("resourcetransformations").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(resourceTransformation).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a resourceTransformation and updates it. Returns the server's representation of the resourceTransformation, and an error, if there is any.
func (c *resourceTransformations) Update(ctx context.Context, resourceTransformation *v1alpha1.ResourceTransformation, opts v1.UpdateOptions) (result *v1alpha1.ResourceTransformation, err error) {
	result = &v1alpha1.ResourceTransformation{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("resourcetransformations").
		Name(resourceTransformation.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(resourceTransformation).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *resourceTransformations) UpdateStatus(ctx context.Context, resourceTransformation *v1alpha1.ResourceTransformation, opts v1.UpdateOptions) (result *v1alpha1.ResourceTransformation, err error) {
	result = &v1alpha1.ResourceTransformation{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("resourcetransformations").
		Name(resourceTransformation.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(resourceTransformation).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the resourceTransformation and deletes it. Returns an error if one occurs.
func (c *resourceTransformations) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("resourcetransformations").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *resourceTransformations) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("resourcetransformations").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched resourceTransformation.
func (c *resourceTransformations) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.ResourceTransformation, err error) {
	result = &v1alpha1.ResourceTransformation{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("resourcetransformations").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}