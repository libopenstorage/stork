// +build unittest

package metrics

import (
	"fmt"
	"os"
	"reflect"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/portworx/sched-ops/k8s/apiextensions"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/cli-runtime/pkg/resource"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest/fake"
	v1 "k8s.io/kubernetes/pkg/apis/core/v1"
)

var codec runtime.Codec
var fakeRestClient *fake.RESTClient
var unstructuredSerializer = resource.UnstructuredPlusDefaultContentConfig().NegotiatedSerializer

func init() {
	resetTest()
}

func resetTest() {
	scheme := runtime.NewScheme()
	if err := snapv1.AddToScheme(scheme); err != nil {
		fmt.Printf("Error updating scheme: %v", err)
		os.Exit(1)
	}
	if err := stork_api.AddToScheme(scheme); err != nil {
		fmt.Printf("Error updating scheme: %v", err)
		os.Exit(1)
	}
	if err := v1.AddToScheme(scheme); err != nil {
		fmt.Printf("Error updating scheme: %v", err)
		os.Exit(1)
	}
	codec = serializer.NewCodecFactory(scheme).LegacyCodec(scheme.PrioritizedVersionsAllGroups()...)
	fakeStorkClient := fakeclient.NewSimpleClientset()
	fakeAPIExtensionClient := apiextensionsclient.NewSimpleClientset()

	fakeRestClient = &fake.RESTClient{
		NegotiatedSerializer: unstructuredSerializer,
	}

	fakeKubeClient := kubernetes.NewSimpleClientset()
	apiextensions.SetInstance(apiextensions.New(fakeAPIExtensionClient))
	storkops.SetInstance(storkops.New(fakeKubeClient, fakeStorkClient, fakeRestClient))
	// bkp
	bkp := apiextensions.CustomResource{
		Name:    stork_api.ApplicationBackupResourceName,
		Plural:  stork_api.ApplicationBackupResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationBackup{}).Name(),
	}
	err := k8sutils.CreateCRD(bkp)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		os.Exit(1)
	}
	//restore
	restore := apiextensions.CustomResource{
		Name:    stork_api.ApplicationRestoreResourceName,
		Plural:  stork_api.ApplicationRestoreResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationRestore{}).Name(),
	}
	err = k8sutils.CreateCRD(restore)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		os.Exit(1)
	}
	//clone
	clone := apiextensions.CustomResource{
		Name:    stork_api.ApplicationCloneResourceName,
		Plural:  stork_api.ApplicationCloneResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationClone{}).Name(),
	}
	err = k8sutils.CreateCRD(clone)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		os.Exit(1)
	}
	//clusterpair
	pair := apiextensions.CustomResource{
		Name:    stork_api.ClusterPairResourceName,
		Plural:  stork_api.ClusterPairResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ClusterPair{}).Name(),
	}
	err = k8sutils.CreateCRD(pair)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		os.Exit(1)
	}
	//migration
	migr := apiextensions.CustomResource{
		Name:    stork_api.MigrationResourceName,
		Plural:  stork_api.MigrationResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.Migration{}).Name(),
	}
	err = k8sutils.CreateCRD(migr)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		os.Exit(1)
	}
	if err := StartMetrics(true, true); err != nil {
		fmt.Printf("Error starting metrics: %v", err)
		os.Exit(1)
	}
	time.Sleep(5 * time.Second)
}
