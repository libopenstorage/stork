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

package v1alpha1

import (
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork.com/v1alpha1"
	scheme "github.com/libopenstorage/stork/pkg/client/clientset/versioned/scheme"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// StorkRulesGetter has a method to return a StorkRuleInterface.
// A group's client should implement this interface.
type StorkRulesGetter interface {
	StorkRules(namespace string) StorkRuleInterface
}

// StorkRuleInterface has methods to work with StorkRule resources.
type StorkRuleInterface interface {
	Create(*v1alpha1.StorkRule) (*v1alpha1.StorkRule, error)
	Update(*v1alpha1.StorkRule) (*v1alpha1.StorkRule, error)
	Delete(name string, options *v1.DeleteOptions) error
	DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error
	Get(name string, options v1.GetOptions) (*v1alpha1.StorkRule, error)
	List(opts v1.ListOptions) (*v1alpha1.StorkRuleList, error)
	Watch(opts v1.ListOptions) (watch.Interface, error)
	Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.StorkRule, err error)
	StorkRuleExpansion
}

// storkRules implements StorkRuleInterface
type storkRules struct {
	client rest.Interface
	ns     string
}

// newStorkRules returns a StorkRules
func newStorkRules(c *StorkV1alpha1Client, namespace string) *storkRules {
	return &storkRules{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the storkRule, and returns the corresponding storkRule object, and an error if there is any.
func (c *storkRules) Get(name string, options v1.GetOptions) (result *v1alpha1.StorkRule, err error) {
	result = &v1alpha1.StorkRule{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("storkrules").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of StorkRules that match those selectors.
func (c *storkRules) List(opts v1.ListOptions) (result *v1alpha1.StorkRuleList, err error) {
	result = &v1alpha1.StorkRuleList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("storkrules").
		VersionedParams(&opts, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested storkRules.
func (c *storkRules) Watch(opts v1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("storkrules").
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch()
}

// Create takes the representation of a storkRule and creates it.  Returns the server's representation of the storkRule, and an error, if there is any.
func (c *storkRules) Create(storkRule *v1alpha1.StorkRule) (result *v1alpha1.StorkRule, err error) {
	result = &v1alpha1.StorkRule{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("storkrules").
		Body(storkRule).
		Do().
		Into(result)
	return
}

// Update takes the representation of a storkRule and updates it. Returns the server's representation of the storkRule, and an error, if there is any.
func (c *storkRules) Update(storkRule *v1alpha1.StorkRule) (result *v1alpha1.StorkRule, err error) {
	result = &v1alpha1.StorkRule{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("storkrules").
		Name(storkRule.Name).
		Body(storkRule).
		Do().
		Into(result)
	return
}

// Delete takes name of the storkRule and deletes it. Returns an error if one occurs.
func (c *storkRules) Delete(name string, options *v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("storkrules").
		Name(name).
		Body(options).
		Do().
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *storkRules) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("storkrules").
		VersionedParams(&listOptions, scheme.ParameterCodec).
		Body(options).
		Do().
		Error()
}

// Patch applies the patch and returns the patched storkRule.
func (c *storkRules) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.StorkRule, err error) {
	result = &v1alpha1.StorkRule{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("storkrules").
		SubResource(subresources...).
		Name(name).
		Body(data).
		Do().
		Into(result)
	return
}
