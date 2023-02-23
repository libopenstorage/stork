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

package externalversions

import (
	"fmt"

	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	cache "k8s.io/client-go/tools/cache"
)

// GenericInformer is type of SharedIndexInformer which will locate and delegate to other
// sharedInformers based on type
type GenericInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() cache.GenericLister
}

type genericInformer struct {
	informer cache.SharedIndexInformer
	resource schema.GroupResource
}

// Informer returns the SharedIndexInformer.
func (f *genericInformer) Informer() cache.SharedIndexInformer {
	return f.informer
}

// Lister returns the GenericLister.
func (f *genericInformer) Lister() cache.GenericLister {
	return cache.NewGenericLister(f.Informer().GetIndexer(), f.resource)
}

// ForResource gives generic access to a shared informer of the matching type
// TODO extend this to unknown resources with a client pool
func (f *sharedInformerFactory) ForResource(resource schema.GroupVersionResource) (GenericInformer, error) {
	switch resource {
	// Group=stork.libopenstorage.org, Version=v1alpha1
	case v1alpha1.SchemeGroupVersion.WithResource("applicationbackups"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ApplicationBackups().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("applicationbackupschedules"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ApplicationBackupSchedules().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("applicationclones"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ApplicationClones().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("applicationregistrations"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ApplicationRegistrations().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("applicationrestores"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ApplicationRestores().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("backuplocations"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().BackupLocations().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("clusterdomainupdates"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ClusterDomainUpdates().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("clusterdomainsstatuses"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ClusterDomainsStatuses().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("clusterpairs"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ClusterPairs().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("dataexports"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().DataExports().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("groupvolumesnapshots"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().GroupVolumeSnapshots().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("migrations"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().Migrations().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("migrationschedules"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().MigrationSchedules().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("namespacedschedulepolicies"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().NamespacedSchedulePolicies().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("resourcetransformations"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().ResourceTransformations().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("rules"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().Rules().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("schedulepolicies"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().SchedulePolicies().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("volumesnapshotrestores"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().VolumeSnapshotRestores().Informer()}, nil
	case v1alpha1.SchemeGroupVersion.WithResource("volumesnapshotschedules"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Stork().V1alpha1().VolumeSnapshotSchedules().Informer()}, nil

	}

	return nil, fmt.Errorf("no informer found for %v", resource)
}