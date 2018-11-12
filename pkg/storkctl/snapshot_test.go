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
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

type NewTestCommand func(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command

func testSnapshotsCommon(t *testing.T, newCommand NewTestCommand, cmdArgs []string, snapshots *snapv1.VolumeSnapshotList, expected string, errorExpected bool) {
	var err error

	scheme := runtime.NewScheme()
	err = snapv1.AddToScheme(scheme)
	require.NoError(t, err, "Error adding %v to scheme", scheme)

	f := NewTestFactory()
	f.setOutputFormat(outputFormatTable)
	tf := f.TestFactory.WithNamespace("test")
	f.setNamespace("test")
	defer tf.Cleanup()
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(schema.GroupVersion{Version: "v1", Group: snapv1.GroupName})

	fakeRestClient := &fake.RESTClient{
		NegotiatedSerializer: unstructuredSerializer,
		Resp:                 &http.Response{StatusCode: 200, Header: defaultHeader(), Body: objBody(codec, snapshots)},
	}
	tf.Client = fakeRestClient
	fakeKubeClient, err := tf.KubernetesClientSet()
	require.NoError(t, err, "Error getting KubernetesClientSet")

	k8s.Instance().SetClient(fakeKubeClient, fakeRestClient, nil, nil, nil)

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

func TestGetVolumeSnapshotsNoSnapshots(t *testing.T) {
	cmdArgs := []string{"volumesnapshots"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "No resources found.\n"
	testSnapshotsCommon(t, newGetCommand, cmdArgs, &snapshots, expected, false)
}

func TestGetVolumeSnapshotsOneSnapshot(t *testing.T) {
	cmdArgs := []string{"volumesnapshots"}

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

	testSnapshotsCommon(t, newGetCommand, cmdArgs, &snapshots, expected, false)
}

func TestCreateSnapshotsNoSnapshotName(t *testing.T) {
	cmdArgs := []string{"volumesnapshots"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "error: Exactly one argument needs to be provided for snapshot name"
	testSnapshotsCommon(t, newCreateCommand, cmdArgs, &snapshots, expected, true)
}

func TestCreateSnapshotsNoPVCName(t *testing.T) {
	cmdArgs := []string{"volumesnapshots", "-p", "", "snap1"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "error: PVC name needs to be given"
	testSnapshotsCommon(t, newCreateCommand, cmdArgs, &snapshots, expected, true)
}

func TestCreateSnapshots(t *testing.T) {
	cmdArgs := []string{"volumesnapshots", "-p", "pvc_name", "snap1"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "Snapshot snap1 created successfully\n\n"
	testSnapshotsCommon(t, newCreateCommand, cmdArgs, &snapshots, expected, false)
}

func TestDeleteSnapshotsNoSnapshotName(t *testing.T) {
	cmdArgs := []string{"volumesnapshots"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "error: At least one argument needs to be provided for snapshot name"
	testSnapshotsCommon(t, newDeleteCommand, cmdArgs, &snapshots, expected, true)
}

func TestDeleteSnapshotsNoPVCName(t *testing.T) {
	cmdArgs := []string{"volumesnapshots", "-p", "", "snap1"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "Snapshot snap1 deleted successfully\n"
	testSnapshotsCommon(t, newDeleteCommand, cmdArgs, &snapshots, expected, false)
}

func TestDeleteSnapshotsNoSnapshots(t *testing.T) {
	cmdArgs := []string{"volumesnapshots", "-p", "pvc_name", "snap1"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "No resources found.\n"
	testSnapshotsCommon(t, newDeleteCommand, cmdArgs, &snapshots, expected, false)
}
