// +build unittest

package storkctl

import (
	"fmt"
	"net/http"
	"os"
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/appregistration"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	fakeocpclient "github.com/openshift/client-go/apps/clientset/versioned/fake"
	fakeocpsecurityclient "github.com/openshift/client-go/security/clientset/versioned/fake"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/dynamic"
	"github.com/portworx/sched-ops/k8s/externalstorage"
	"github.com/portworx/sched-ops/k8s/openshift"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest/fake"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	v1 "k8s.io/kubernetes/pkg/apis/core/v1"
)

var codec runtime.Codec
var fakeRestClient *fake.RESTClient
var testFactory *TestFactory

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
	fakeOCPClient := fakeocpclient.NewSimpleClientset()
	fakeOCPSecurityClient := fakeocpsecurityclient.NewSimpleClientset()
	fakeDynamicClient := fakedynamicclient.NewSimpleDynamicClientWithCustomListKinds(scheme, appregistration.GetSupportedGVR())

	if testFactory != nil {
		testFactory.TestFactory.WithNamespace("test").Cleanup()
	}

	fakeRestClient = &fake.RESTClient{
		NegotiatedSerializer: unstructuredSerializer,
	}
	testFactory = NewTestFactory()
	testFactory.setOutputFormat(outputFormatTable)
	tf := testFactory.TestFactory
	tf.Client = fakeRestClient
	fakeKubeClient := kubernetes.NewSimpleClientset()

	core.SetInstance(core.New(fakeKubeClient))
	storkops.SetInstance(storkops.New(fakeKubeClient, fakeStorkClient, fakeRestClient))
	externalstorage.SetInstance(externalstorage.New(fakeRestClient))
	openshift.SetInstance(openshift.New(fakeKubeClient, fakeOCPClient, fakeOCPSecurityClient))
	apps.SetInstance(apps.New(fakeKubeClient.AppsV1(), fakeKubeClient.CoreV1()))
	batch.SetInstance(batch.New(fakeKubeClient.BatchV1(), fakeKubeClient.BatchV1beta1()))
	dynamic.SetInstance(dynamic.New(fakeDynamicClient))
}

func testCommon(t *testing.T, cmdArgs []string, obj runtime.Object, expected string, errorExpected bool) {
	var err error

	if obj != nil {
		fakeRestClient.Resp = &http.Response{StatusCode: 200, Header: defaultHeader(), Body: objBody(codec, obj)}
		fakeRestClient.Client = fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: defaultHeader(), Body: objBody(codec, obj)}, nil
		})
	}

	streams, _, buf, _ := genericclioptions.NewTestIOStreams()
	cmd := NewCommand(testFactory, streams.In, streams.Out, streams.ErrOut)
	cmd.SetOutput(buf)
	cmd.SetArgs(cmdArgs)

	calledFatal := false
	if errorExpected {
		defer cmdutil.DefaultBehaviorOnFatal()
		cmdutil.BehaviorOnFatal(func(e string, code int) {
			if calledFatal {
				return
			}
			calledFatal = true
			require.Equal(t, 1, code, "Unexpected error code")
			require.Equal(t, expected, e)
		})
	}

	err = cmd.Execute()

	if errorExpected {
		require.True(t, calledFatal)
	} else {
		require.NoError(t, err, "Error executing command: %v", cmd)
		require.Equal(t, expected, buf.String())
	}
}
