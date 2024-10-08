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

// Code generated by lister-gen. DO NOT EDIT.

package v1alpha1

import (
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// ApplicationRestoreLister helps list ApplicationRestores.
// All objects returned here must be treated as read-only.
type ApplicationRestoreLister interface {
	// List lists all ApplicationRestores in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.ApplicationRestore, err error)
	// ApplicationRestores returns an object that can list and get ApplicationRestores.
	ApplicationRestores(namespace string) ApplicationRestoreNamespaceLister
	ApplicationRestoreListerExpansion
}

// applicationRestoreLister implements the ApplicationRestoreLister interface.
type applicationRestoreLister struct {
	indexer cache.Indexer
}

// NewApplicationRestoreLister returns a new ApplicationRestoreLister.
func NewApplicationRestoreLister(indexer cache.Indexer) ApplicationRestoreLister {
	return &applicationRestoreLister{indexer: indexer}
}

// List lists all ApplicationRestores in the indexer.
func (s *applicationRestoreLister) List(selector labels.Selector) (ret []*v1alpha1.ApplicationRestore, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.ApplicationRestore))
	})
	return ret, err
}

// ApplicationRestores returns an object that can list and get ApplicationRestores.
func (s *applicationRestoreLister) ApplicationRestores(namespace string) ApplicationRestoreNamespaceLister {
	return applicationRestoreNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// ApplicationRestoreNamespaceLister helps list and get ApplicationRestores.
// All objects returned here must be treated as read-only.
type ApplicationRestoreNamespaceLister interface {
	// List lists all ApplicationRestores in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.ApplicationRestore, err error)
	// Get retrieves the ApplicationRestore from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1alpha1.ApplicationRestore, error)
	ApplicationRestoreNamespaceListerExpansion
}

// applicationRestoreNamespaceLister implements the ApplicationRestoreNamespaceLister
// interface.
type applicationRestoreNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all ApplicationRestores in the indexer for a given namespace.
func (s applicationRestoreNamespaceLister) List(selector labels.Selector) (ret []*v1alpha1.ApplicationRestore, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.ApplicationRestore))
	})
	return ret, err
}

// Get retrieves the ApplicationRestore from the indexer for a given namespace and name.
func (s applicationRestoreNamespaceLister) Get(name string) (*v1alpha1.ApplicationRestore, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1alpha1.Resource("applicationrestore"), name)
	}
	return obj.(*v1alpha1.ApplicationRestore), nil
}
