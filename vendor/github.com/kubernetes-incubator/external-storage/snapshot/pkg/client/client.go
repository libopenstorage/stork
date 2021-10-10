/*
Copyright 2017 The Kubernetes Authors.

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

package client

import (
	"context"
	"reflect"
	"time"

	"github.com/golang/glog"
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
)

const (
	// SnapshotPVCAnnotation is "snapshot.alpha.kubernetes.io/snapshot"
	SnapshotPVCAnnotation = "snapshot.alpha.kubernetes.io/snapshot"
)

var (
	// storkVolumeSnapshotShortNames are the short names for volume snapshot CR
	storkVolumeSnapshotShortNames = []string{"stork-volumesnapshot", "stork-volumesnapshots", "svs"}
	// storkVolumeSnapshotDataShortNames are the short names for volume snapshot data CR
	storkVolumeSnapshotDataShortNames = []string{"stork-volumesnapshotdata", "stork-volumesnapshotdatas", "svsd"}
)

// NewClient creates a new RESTClient
func NewClient(cfg *rest.Config) (*rest.RESTClient, *runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := crdv1.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}

	config := *cfg
	config.GroupVersion = &crdv1.SchemeGroupVersion
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.WithoutConversionCodecFactory{CodecFactory: serializer.NewCodecFactory(scheme)}

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, nil, err
	}

	return client, scheme, nil
}

// CreateCRD creates CustomResourceDefinition
func CreateCRD(clientset apiextensionsclient.Interface) error {
	crd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: crdv1.VolumeSnapshotDataResourcePlural + "." + crdv1.GroupName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   crdv1.GroupName,
			Version: crdv1.SchemeGroupVersion.Version,
			Scope:   apiextensionsv1beta1.ClusterScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural: crdv1.VolumeSnapshotDataResourcePlural,
				Kind:   reflect.TypeOf(crdv1.VolumeSnapshotData{}).Name(),
				ShortNames: storkVolumeSnapshotDataShortNames,
			},
		},
	}
	res, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})

	if err != nil && !apierrors.IsAlreadyExists(err) {
		glog.Fatalf("failed to create VolumeSnapshotDataResource: %#v, err: %#v",
			res, err)
	}

	crd = &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: crdv1.VolumeSnapshotResourcePlural + "." + crdv1.GroupName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   crdv1.GroupName,
			Version: crdv1.SchemeGroupVersion.Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural: crdv1.VolumeSnapshotResourcePlural,
				Kind:   reflect.TypeOf(crdv1.VolumeSnapshot{}).Name(),
				ShortNames: storkVolumeSnapshotShortNames,
			},
		},
	}
	res, err = clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		glog.Fatalf("failed to create VolumeSnapshotResource: %#v, err: %#v",
			res, err)
	}
	return nil
}

// CreateCRDV1 creates CustomResourceDefinition for v1 apiVersion
func CreateCRDV1(clientset apiextensionsclient.Interface) error {
	setSchema := true
	annot := make(map[string]string)
	annot["api-approved.kubernetes.io"] = "https://github.com/kubernetes-csi/external-snapshotter/pull/419"
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:        crdv1.VolumeSnapshotDataResourcePlural + "." + crdv1.GroupName,
			Annotations: annot,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: crdv1.GroupName,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: crdv1.SchemeGroupVersion.Version,
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							XPreserveUnknownFields: &setSchema,
						},
					},
				},
			},
			Scope: apiextensionsv1.ClusterScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: crdv1.VolumeSnapshotDataResourcePlural,
				Kind:   reflect.TypeOf(crdv1.VolumeSnapshotData{}).Name(),
				ShortNames: storkVolumeSnapshotDataShortNames,
			},
		},
	}
	res, err := clientset.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})

	if err != nil && !apierrors.IsAlreadyExists(err) {
		glog.Fatalf("failed to create VolumeSnapshotDataResource: %#v, err: %#v",
			res, err)
	}

	crd = &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:        crdv1.VolumeSnapshotResourcePlural + "." + crdv1.GroupName,
			Annotations: annot,
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: crdv1.GroupName,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: crdv1.SchemeGroupVersion.Version,
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							XPreserveUnknownFields: &setSchema,
						},
					},
				},
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: crdv1.VolumeSnapshotResourcePlural,
				Kind:   reflect.TypeOf(crdv1.VolumeSnapshot{}).Name(),
				ShortNames: storkVolumeSnapshotShortNames,
			},
		},
	}
	res, err = clientset.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		glog.Fatalf("failed to create VolumeSnapshotResource: %#v, err: %#v",
			res, err)
	}
	return nil
}

// WaitForSnapshotResource waits for the snapshot resource
func WaitForSnapshotResource(snapshotClient *rest.RESTClient) error {
	return wait.Poll(100*time.Millisecond, 60*time.Second, func() (bool, error) {
		_, err := snapshotClient.Get().
			Resource(crdv1.VolumeSnapshotDataResourcePlural).DoRaw(context.TODO())
		if err == nil {
			return true, nil
		}
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	})
}
