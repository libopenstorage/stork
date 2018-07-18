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

package fake

import (
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork.com/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeStorkRules implements StorkRuleInterface
type FakeStorkRules struct {
	Fake *FakeStorkV1alpha1
	ns   string
}

var storkrulesResource = schema.GroupVersionResource{Group: "stork.com", Version: "v1alpha1", Resource: "storkrules"}

var storkrulesKind = schema.GroupVersionKind{Group: "stork.com", Version: "v1alpha1", Kind: "StorkRule"}

// Get takes name of the storkRule, and returns the corresponding storkRule object, and an error if there is any.
func (c *FakeStorkRules) Get(name string, options v1.GetOptions) (result *v1alpha1.StorkRule, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(storkrulesResource, c.ns, name), &v1alpha1.StorkRule{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.StorkRule), err
}

// List takes label and field selectors, and returns the list of StorkRules that match those selectors.
func (c *FakeStorkRules) List(opts v1.ListOptions) (result *v1alpha1.StorkRuleList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(storkrulesResource, storkrulesKind, c.ns, opts), &v1alpha1.StorkRuleList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.StorkRuleList{}
	for _, item := range obj.(*v1alpha1.StorkRuleList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested storkRules.
func (c *FakeStorkRules) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(storkrulesResource, c.ns, opts))

}

// Create takes the representation of a storkRule and creates it.  Returns the server's representation of the storkRule, and an error, if there is any.
func (c *FakeStorkRules) Create(storkRule *v1alpha1.StorkRule) (result *v1alpha1.StorkRule, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(storkrulesResource, c.ns, storkRule), &v1alpha1.StorkRule{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.StorkRule), err
}

// Update takes the representation of a storkRule and updates it. Returns the server's representation of the storkRule, and an error, if there is any.
func (c *FakeStorkRules) Update(storkRule *v1alpha1.StorkRule) (result *v1alpha1.StorkRule, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(storkrulesResource, c.ns, storkRule), &v1alpha1.StorkRule{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.StorkRule), err
}

// Delete takes name of the storkRule and deletes it. Returns an error if one occurs.
func (c *FakeStorkRules) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(storkrulesResource, c.ns, name), &v1alpha1.StorkRule{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeStorkRules) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(storkrulesResource, c.ns, listOptions)

	_, err := c.Fake.Invokes(action, &v1alpha1.StorkRuleList{})
	return err
}

// Patch applies the patch and returns the patched storkRule.
func (c *FakeStorkRules) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.StorkRule, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(storkrulesResource, c.ns, name, data, subresources...), &v1alpha1.StorkRule{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.StorkRule), err
}
