// +build unittest

package storkctl

import (
	"net/http"
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest/fake"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

type NewTestCommand func(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command

func testSnapshotsCommon(t *testing.T, cmdArgs []string, snapshots *snapv1.VolumeSnapshotList, expected string) {
	var err error

	scheme := runtime.NewScheme()
	if err := snapv1.AddToScheme(scheme); err != nil {
		require.NoError(t, err, "Error adding snapshot to scheme")
	}

	f := NewTestFactory()
	tf := f.TestFactory.WithNamespace("test")
	defer tf.Cleanup()
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(schema.GroupVersion{Version: "v1", Group: snapv1.GroupName})

	fakeRestClient := &fake.RESTClient{
		NegotiatedSerializer: unstructuredSerializer,
		Resp:                 &http.Response{StatusCode: 200, Header: defaultHeader(), Body: objBody(codec, snapshots)},
	}
	tf.Client = fakeRestClient
	fakeKubeClient, err := tf.KubernetesClientSet()
	if err != nil {
		require.NoError(t, err, "Error getting KubernetesClientSet")
	}

	k8s.Instance().SetClient(fakeKubeClient, fakeRestClient, nil, nil, nil)

	_, bufIn, bufOut, bufErrOut := genericclioptions.NewTestIOStreams()
	cmd := NewCommand(bufIn, bufOut, bufErrOut)
	cmd.SetOutput(bufOut)
	cmd.SetArgs(cmdArgs)
	if err = cmd.Execute(); err != nil {
		require.NoError(t, err, "Error executing command: %v", cmd)
	}

	if e, a := expected, bufOut.String(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestGetVolumeSnapshotsNoSnapshots(t *testing.T) {
	cmdArgs := []string{"get", "volumesnapshots"}

	var snapshots snapv1.VolumeSnapshotList

	expected := `No resources found.
`

	testSnapshotsCommon(t, cmdArgs, &snapshots, expected)
}

func TestGetVolumeSnapshotsOneSnapshot(t *testing.T) {
	cmdArgs := []string{"get", "volumesnapshots"}

	snap := &snapv1.VolumeSnapshot{
		Metadata: metav1.ObjectMeta{
			Name:      "snap1",
			Namespace: "test_namespace",
			Labels: map[string]string{
				"Label1": "labelValue1",
				"Label2": "labelValue2",
			},
		},
		Spec: snapv1.VolumeSnapshotSpec{
			SnapshotDataName:          "snapShotDataName",
			PersistentVolumeClaimName: "persistentVolumeClaimName",
		},
	}
	var snapshots snapv1.VolumeSnapshotList

	snapshots.Items = append(snapshots.Items, *snap)

	expected := `NAME      PVC                         STATUS    CREATED   COMPLETED   TYPE
snap1     persistentVolumeClaimName   Pending                         Local
`

	testSnapshotsCommon(t, cmdArgs, &snapshots, expected)
}
