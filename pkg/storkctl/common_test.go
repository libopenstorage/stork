// +build unittest

package storkctl

import (
	"net/http"
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest/fake"
	"k8s.io/kubernetes/pkg/apis/core/v1"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

var codec runtime.Codec
var fakeStorkClient *fakeclient.Clientset
var fakeRestClient *fake.RESTClient
var testFactory *TestFactory

func init() {
	resetTest()
}

func resetTest() {
	scheme := runtime.NewScheme()
	snapv1.AddToScheme(scheme)
	v1alpha1.AddToScheme(scheme)
	v1.AddToScheme(scheme)
	codec = serializer.NewCodecFactory(scheme).LegacyCodec(scheme.PrioritizedVersionsAllGroups()...)
	fakeStorkClient = fakeclient.NewSimpleClientset()

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

	k8s.Instance().SetClient(fakeKubeClient, fakeRestClient, fakeStorkClient, nil, nil)
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
