// +build unittest

package storkctl

import (
	"strconv"
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
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

	expected := "error: exactly one argument needs to be provided for snapshot name"
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

	expected := "error: at least one argument needs to be provided for snapshot name"
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

func TestGetVolumeSnapshotRestoreNoRestores(t *testing.T) {
	cmdArgs := []string{"get", "volumesnapshotrestore"}

	var snapshots storkv1.VolumeSnapshotRestoreList
	expected := "No resources found.\n"
	testCommon(t, cmdArgs, &snapshots, expected, false)
}

func TestGetVolumeSnapshotRestoreAllRestores(t *testing.T) {

	_, err := k8s.Instance().CreateNamespace("ns", nil)
	require.NoError(t, err, "Error creating ns namespace")
	_, err = k8s.Instance().CreateNamespace("default", nil)
	require.NoError(t, err, "Error creating ns1 namespace")

	cmdArgs := []string{"get", "volumesnapshotrestore", "--all-namespaces"}
	createSnapshotRestoreAndVerify(t, "test1", "asdf", "sourceSnapName1", "default", false)
	createSnapshotRestoreAndVerify(t, "test2", "default", "sourceSnapName2", "default", false)
	createSnapshotRestoreAndVerify(t, "test3", "ns", "sourceSnapName3", "ns", false)

	var snapshots storkv1.VolumeSnapshotRestoreList
	expected := "NAMESPACE   NAME      SOURCE-SNAPSHOT   SOURCE-SNAPSHOT-NAMESPACE   STATUS    VOLUMES   CREATED\n" +
		"ns          test3     sourceSnapName3   ns                                    0         \n" +
		"default     test2     sourceSnapName2   default                               0         \n"
	testCommon(t, cmdArgs, &snapshots, expected, false)
}

func TestGetVolumeSnapshotsOneRestore(t *testing.T) {
	crdRestore := "crd-restore-test"
	cmdArgs := []string{"get", "volumesnapshotrestore", crdRestore}
	namespace := "default"
	createSnapshotRestoreAndVerify(t, crdRestore, namespace, "sourceSnapName", "sourceSnapnamespace", false)
	expected := "NAME               SOURCE-SNAPSHOT   SOURCE-SNAPSHOT-NAMESPACE   STATUS    VOLUMES   CREATED\n" +
		"crd-restore-test   sourceSnapName    sourceSnapnamespace                   0         \n"

	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetVolumeSnapshotMultipleRestore(t *testing.T) {
	crdRestore1 := "crd-restore1"
	crdRestore2 := "crd-restore2"
	namespace := "default"

	createSnapshotRestoreAndVerify(t, crdRestore1, namespace, "sourceName1", "sourceNamespace1", false)
	createSnapshotRestoreAndVerify(t, crdRestore2, namespace, "sourceName2", "sourceNamespace2", false)

	expected := "NAME               SOURCE-SNAPSHOT   SOURCE-SNAPSHOT-NAMESPACE   STATUS    VOLUMES   CREATED\n" +
		"test2              sourceSnapName2   default                               0         \n" +
		"crd-restore-test   sourceSnapName    sourceSnapnamespace                   0         \n" +
		"crd-restore1       sourceName1       sourceNamespace1                      0         \n" +
		"crd-restore2       sourceName2       sourceNamespace2                      0         \n"

	cmdArgs := []string{"get", "volumesnapshotrestore"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteVolumeSnapshotRestore(t *testing.T) {
	crdRestore := "crd-restore"
	namespace := "default"

	createSnapshotRestoreAndVerify(t, crdRestore, namespace, "sourceName1", "sourceNamespace1", false)

	expected := "Volume snapshot restore " + crdRestore + " deleted successfully\n"

	cmdArgs := []string{"delete", "volumesnapshotrestore", crdRestore}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestVolumeSnapshotRestoreWithNoName(t *testing.T) {
	expected := "error: exactly one argument needs to be provided for volumesnapshotrestore name"
	cmdArgs := []string{"create", "volumesnapshotrestore"}
	testCommon(t, cmdArgs, nil, expected, true)
}

func createSnapshotRestoreAndVerify(
	t *testing.T,
	name string,
	namespace string,
	sourceName string,
	sourceNamespace string,
	isGroup bool,
) {
	cmdArgs := []string{"create", "volumesnapshotrestore", "-n", namespace, "--snapname", sourceName, "--sourcenamepace", sourceNamespace, "-g=" + strconv.FormatBool(isGroup), name}
	expected := "Snapshot restore " + name + " started successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	snapRestore, err := k8s.Instance().GetVolumeSnapshotRestore(name, namespace)
	require.NoError(t, err, "Error getting volumesnapshotrestores")
	require.Equal(t, name, snapRestore.Name, "VolumeSnapshotRestore name mismatch")
	require.Equal(t, sourceName, snapRestore.Spec.SourceName, "VolumeSnapshotRestore sourceName mismatch")
	require.Equal(t, sourceNamespace, snapRestore.Spec.SourceNamespace, "VolumeSnapshotRestore sourceNamespace mismatch")
	require.Equal(t, isGroup, snapRestore.Spec.GroupSnapshot, "VolumeSnapshotRestore isGroupSnapshot mismatch")
}
