// +build unittest

package storkctl

import (
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetVolumeSnapshotsNoSnapshots(t *testing.T) {
	cmdArgs := []string{"get", "volumesnapshots"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "No resources found.\n"
	testCommon(t, cmdArgs, &snapshots, expected, false)
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

	testCommon(t, cmdArgs, &snapshots, expected, false)
}

func TestGetVolumeSnapshotsMultipleSnapshots(t *testing.T) {
	var snapshots snapv1.VolumeSnapshotList
	_, err := k8s.Instance().CreateNamespace("test1", nil)
	require.NoError(t, err, "Error creating test1 namespace")
	_, err = k8s.Instance().CreateNamespace("test2", nil)
	require.NoError(t, err, "Error creating test2 namespace")

	cmdArgs := []string{"get", "volumesnapshots", "--all-namespaces"}

	snap1 := &snapv1.VolumeSnapshot{
		Metadata: metav1.ObjectMeta{
			Name:      "snap1",
			Namespace: "test1",
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

	snap2 := &snapv1.VolumeSnapshot{
		Metadata: metav1.ObjectMeta{
			Name:      "snap1",
			Namespace: "test2",
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
	snapshots.Items = append(snapshots.Items, *snap1, *snap2)

	// The test API for snapshot doesn't know about namespaces, so it return
	// all snapshots on List calls
	expected := "NAMESPACE   NAME      PVC                         STATUS    CREATED   COMPLETED   TYPE\n" +
		"test1       snap1     persistentVolumeClaimName   Pending                         Local\n" +
		"test2       snap1     persistentVolumeClaimName   Pending                         Local\n" +
		"test1       snap1     persistentVolumeClaimName   Pending                         Local\n" +
		"test2       snap1     persistentVolumeClaimName   Pending                         Local\n"

	testCommon(t, cmdArgs, &snapshots, expected, false)
}

func TestCreateSnapshotsNoSnapshotName(t *testing.T) {
	cmdArgs := []string{"create", "volumesnapshots"}

	expected := "error: Exactly one argument needs to be provided for snapshot name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateSnapshotsNoPVCName(t *testing.T) {
	cmdArgs := []string{"create", "volumesnapshots", "-p", "", "snap1"}

	expected := "error: PVC name needs to be given"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateSnapshots(t *testing.T) {
	cmdArgs := []string{"create", "volumesnapshots", "-p", "pvc_name", "snap1"}

	expected := "Snapshot snap1 created successfully\n\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteSnapshotsNoSnapshotName(t *testing.T) {
	cmdArgs := []string{"delete", "volumesnapshots"}

	expected := "error: At least one argument needs to be provided for snapshot name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteSnapshotsNoPVCName(t *testing.T) {
	cmdArgs := []string{"delete", "volumesnapshots", "-p", "", "snap1"}

	expected := "Snapshot snap1 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteSnapshotsNoSnapshots(t *testing.T) {
	cmdArgs := []string{"delete", "volumesnapshots", "-p", "pvc_name", "snap1"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}
