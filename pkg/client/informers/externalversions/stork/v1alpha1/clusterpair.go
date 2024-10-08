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

// Code generated by informer-gen. DO NOT EDIT.

package v1alpha1

import (
	"context"
	time "time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	versioned "github.com/libopenstorage/stork/pkg/client/clientset/versioned"
	internalinterfaces "github.com/libopenstorage/stork/pkg/client/informers/externalversions/internalinterfaces"
	v1alpha1 "github.com/libopenstorage/stork/pkg/client/listers/stork/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// ClusterPairInformer provides access to a shared informer and lister for
// ClusterPairs.
type ClusterPairInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1alpha1.ClusterPairLister
}

type clusterPairInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewClusterPairInformer constructs a new informer for ClusterPair type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewClusterPairInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredClusterPairInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredClusterPairInformer constructs a new informer for ClusterPair type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredClusterPairInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.StorkV1alpha1().ClusterPairs(namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.StorkV1alpha1().ClusterPairs(namespace).Watch(context.TODO(), options)
			},
		},
		&storkv1alpha1.ClusterPair{},
		resyncPeriod,
		indexers,
	)
}

func (f *clusterPairInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredClusterPairInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *clusterPairInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&storkv1alpha1.ClusterPair{}, f.defaultInformer)
}

func (f *clusterPairInformer) Lister() v1alpha1.ClusterPairLister {
	return v1alpha1.NewClusterPairLister(f.Informer().GetIndexer())
}
