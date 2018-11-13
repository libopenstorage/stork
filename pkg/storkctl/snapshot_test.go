// +build unittest

package storkctl

import (
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetVolumeSnapshotsNoSnapshots(t *testing.T) {
	cmdArgs := []string{"volumesnapshots"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "No resources found.\n"
	testCommon(t, newGetCommand, cmdArgs, &snapshots, expected, false)
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

	testCommon(t, newGetCommand, cmdArgs, &snapshots, expected, false)
}

func TestCreateSnapshotsNoSnapshotName(t *testing.T) {
	cmdArgs := []string{"volumesnapshots"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "error: Exactly one argument needs to be provided for snapshot name"
	testCommon(t, newCreateCommand, cmdArgs, &snapshots, expected, true)
}

func TestCreateSnapshotsNoPVCName(t *testing.T) {
	cmdArgs := []string{"volumesnapshots", "-p", "", "snap1"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "error: PVC name needs to be given"
	testCommon(t, newCreateCommand, cmdArgs, &snapshots, expected, true)
}

func TestCreateSnapshots(t *testing.T) {
	cmdArgs := []string{"volumesnapshots", "-p", "pvc_name", "snap1"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "Snapshot snap1 created successfully\n\n"
	testCommon(t, newCreateCommand, cmdArgs, &snapshots, expected, false)
}

func TestDeleteSnapshotsNoSnapshotName(t *testing.T) {
	cmdArgs := []string{"volumesnapshots"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "error: At least one argument needs to be provided for snapshot name"
	testCommon(t, newDeleteCommand, cmdArgs, &snapshots, expected, true)
}

func TestDeleteSnapshotsNoPVCName(t *testing.T) {
	cmdArgs := []string{"volumesnapshots", "-p", "", "snap1"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "Snapshot snap1 deleted successfully\n"
	testCommon(t, newDeleteCommand, cmdArgs, &snapshots, expected, false)
}

func TestDeleteSnapshotsNoSnapshots(t *testing.T) {
	cmdArgs := []string{"volumesnapshots", "-p", "pvc_name", "snap1"}

	var snapshots snapv1.VolumeSnapshotList
	expected := "No resources found.\n"
	testCommon(t, newDeleteCommand, cmdArgs, &snapshots, expected, false)
}
