// +build unittest

package storkctl

import (
	"net/http"
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest/fake"
	"k8s.io/kubernetes/pkg/apis/core/v1"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

var codec runtime.Codec

func init() {
	scheme := runtime.NewScheme()
	snapv1.AddToScheme(scheme)
	v1alpha1.AddToScheme(scheme)
	v1.AddToScheme(scheme)
	codec = serializer.NewCodecFactory(scheme).LegacyCodec(scheme.PrioritizedVersionsAllGroups()...)
}

type NewTestCommand func(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command

func testCommon(t *testing.T, newCommand NewTestCommand, cmdArgs []string, obj runtime.Object, expected string, errorExpected bool) {
	var err error

	f := NewTestFactory()
	f.setOutputFormat(outputFormatTable)
	tf := f.TestFactory.WithNamespace("test")
	f.setNamespace("test")
	defer tf.Cleanup()

	fakeRestClient := &fake.RESTClient{
		NegotiatedSerializer: unstructuredSerializer,
		Resp:                 &http.Response{StatusCode: 200, Header: defaultHeader(), Body: objBody(codec, obj)},
		Client: fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: defaultHeader(), Body: objBody(codec, obj)}, nil
		}),
	}

	tf.Client = fakeRestClient
	fakeKubeClient, err := tf.KubernetesClientSet()
	require.NoError(t, err, "Error getting KubernetesClientSet")

	fakeStorkClient := fakeclient.NewSimpleClientset()

	k8s.Instance().SetClient(fakeKubeClient, fakeRestClient, fakeStorkClient, nil, nil)

	streams, _, buf, _ := genericclioptions.NewTestIOStreams()
	cmd := newCommand(f, streams)
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
