// +build unittest

package storkctl

import (
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
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
