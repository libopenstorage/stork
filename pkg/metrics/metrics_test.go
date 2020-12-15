// +build unittest

package metrics

import (
	"fmt"
	"os"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
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
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		fmt.Printf("Error updating scheme: %v", err)
		os.Exit(1)
	}
	if err := v1.AddToScheme(scheme); err != nil {
		fmt.Printf("Error updating scheme: %v", err)
		os.Exit(1)
	}
	codec = serializer.NewCodecFactory(scheme).LegacyCodec(scheme.PrioritizedVersionsAllGroups()...)
	fakeStorkClient := fakeclient.NewSimpleClientset()
	fakeRestClient = &fake.RESTClient{
		NegotiatedSerializer: unstructuredSerializer,
	}

	fakeKubeClient := kubernetes.NewSimpleClientset()

	storkops.SetInstance(storkops.New(fakeKubeClient, fakeStorkClient, fakeRestClient))
	if err := StartMetrics(); err != nil {
		fmt.Printf("Error starting metrics: %v", err)
		os.Exit(1)
	}
}
