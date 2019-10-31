// +build unittest

package storkctl

import (
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetRestoresNoRestore(t *testing.T) {
	cmdArgs := []string{"get", "applicationrestores"}

	var restoreList storkv1.ApplicationRestoreList
	expected := "No resources found.\n"
	testCommon(t, cmdArgs, &restoreList, expected, false)
}

func createApplicationRestoreAndVerify(
	t *testing.T,
	name string,
	namespace string,
	namespaces []string,
	backupLocation string,
	backupName string,
) {
	cmdArgs := []string{"create", "apprestores", "-n", namespace, name, "--backupLocation", backupLocation, "--backupName", backupName}

	expected := "ApplicationRestore " + name + " started successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	restore, err := k8s.Instance().GetApplicationRestore(name, namespace)
	require.NoError(t, err, "Error getting restore")
	require.Equal(t, name, restore.Name, "ApplicationRestore name mismatch")
	require.Equal(t, namespace, restore.Namespace, "ApplicationRestore namespace mismatch")
	require.Equal(t, backupLocation, restore.Spec.BackupLocation, "ApplicationRestore backupLocation mismatch")
	require.Equal(t, backupName, restore.Spec.BackupName, "ApplicationRestore backupName mismatch")
}

func TestGetApplicationRestoresOneApplicationRestore(t *testing.T) {
	defer resetTest()
	createApplicationRestoreAndVerify(t, "getrestoretest", "test", []string{"namespace1"}, "backuplocation", "backupname")

	expected := "NAME             STAGE     STATUS    VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getrestoretest                       0/0       0                     \n"

	cmdArgs := []string{"get", "apprestores", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationRestoresMultiple(t *testing.T) {
	defer resetTest()
	_, err := k8s.Instance().CreateNamespace("default", nil)
	require.NoError(t, err, "Error creating default namespace")

	createApplicationRestoreAndVerify(t, "getrestoretest1", "default", []string{"namespace1"}, "backuplocation", "backupname")
	createApplicationRestoreAndVerify(t, "getrestoretest2", "default", []string{"namespace1"}, "backuplocation", "backupname")

	expected := "NAME              STAGE     STATUS    VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getrestoretest1                       0/0       0                     \n" +
		"getrestoretest2                       0/0       0                     \n"

	cmdArgs := []string{"get", "apprestores", "getrestoretest1", "getrestoretest2"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all restores if no name given
	cmdArgs = []string{"get", "apprestores"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "NAME              STAGE     STATUS    VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getrestoretest1                       0/0       0                     \n"
	// Should get only one restore if name given
	cmdArgs = []string{"get", "apprestores", "getrestoretest1"}
	testCommon(t, cmdArgs, nil, expected, false)

	_, err = k8s.Instance().CreateNamespace("ns1", nil)
	require.NoError(t, err, "Error creating ns1 namespace")
	createApplicationRestoreAndVerify(t, "getrestoretest21", "ns1", []string{"namespace1"}, "backuplocation", "backupname")
	cmdArgs = []string{"get", "apprestores", "--all-namespaces"}
	expected = "NAMESPACE   NAME               STAGE     STATUS    VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"default     getrestoretest1                        0/0       0                     \n" +
		"default     getrestoretest2                        0/0       0                     \n" +
		"ns1         getrestoretest21                       0/0       0                     \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationRestoresWithStatusAndProgress(t *testing.T) {
	defer resetTest()
	createApplicationRestoreAndVerify(t, "getrestorestatustest", "default", []string{"namespace1"}, "backuplocation", "backupname")
	restore, err := k8s.Instance().GetApplicationRestore("getrestorestatustest", "default")
	require.NoError(t, err, "Error getting restore")

	// Update the status of the restore
	restore.Status.FinishTimestamp = metav1.Now()
	restore.CreationTimestamp = metav1.NewTime(restore.Status.FinishTimestamp.Add(-5 * time.Minute))
	restore.Status.Stage = storkv1.ApplicationRestoreStageFinal
	restore.Status.Status = storkv1.ApplicationRestoreStatusSuccessful
	restore.Status.Volumes = []*storkv1.ApplicationRestoreVolumeInfo{}
	_, err = k8s.Instance().UpdateApplicationRestore(restore)
	require.NoError(t, err, "Error updating restore")

	expected := "NAME                   STAGE     STATUS       VOLUMES   RESOURCES   CREATED               ELAPSED\n" +
		"getrestorestatustest   Final     Successful   0/0       0           " + toTimeString(restore.CreationTimestamp.Time) + "   5m0s\n"
	cmdArgs := []string{"get", "apprestores", "getrestorestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateApplicationRestoresNoName(t *testing.T) {
	cmdArgs := []string{"create", "apprestores"}

	expected := "error: exactly one name needs to be provided for applicationrestore name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateApplicationRestores(t *testing.T) {
	defer resetTest()
	createApplicationRestoreAndVerify(t, "createrestore", "default", []string{"namespace1"}, "backuplocation", "backupname")
}

func TestCreateApplicationRestoresMissingParameters(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"create", "apprestores", "createrestore", "--backupName", "backupname"}
	expected := "error: need to provide BackupLocation to use for restore"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"create", "apprestores", "createrestore", "--backupLocation", "backuplocation"}
	expected = "error: need to provide BackupName to restore"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateDuplicateApplicationRestores(t *testing.T) {
	defer resetTest()
	createApplicationRestoreAndVerify(t, "createrestore", "default", []string{"namespace1"}, "backuplocation", "backupname")
	cmdArgs := []string{"create", "apprestores", "createrestore", "--backupLocation", "backuplocation", "--backupName", "backupname"}

	expected := "Error from server (AlreadyExists): applicationrestores.stork.libopenstorage.org \"createrestore\" already exists"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteApplicationRestoresNoApplicationRestoreName(t *testing.T) {
	cmdArgs := []string{"delete", "apprestores"}

	var restoreList storkv1.ApplicationRestoreList
	expected := "error: at least one argument needs to be provided for applicationrestore name"
	testCommon(t, cmdArgs, &restoreList, expected, true)
}

func TestDeleteApplicationRestores(t *testing.T) {
	defer resetTest()
	createApplicationRestoreAndVerify(t, "deleterestore", "default", []string{"namespace1"}, "backuplocation", "backupname")

	cmdArgs := []string{"delete", "apprestores", "deleterestore"}
	expected := "ApplicationRestore deleterestore deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"delete", "apprestores", "deleterestore"}
	expected = "Error from server (NotFound): applicationrestores.stork.libopenstorage.org \"deleterestore\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	createApplicationRestoreAndVerify(t, "deleterestore1", "default", []string{"namespace1"}, "backuplocation", "backupname1")
	createApplicationRestoreAndVerify(t, "deleterestore2", "default", []string{"namespace1"}, "backuplocation", "backupname2")

	cmdArgs = []string{"delete", "apprestores", "deleterestore1", "deleterestore2"}
	expected = "ApplicationRestore deleterestore1 deleted successfully\n"
	expected += "ApplicationRestore deleterestore2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	createApplicationRestoreAndVerify(t, "deleterestore1", "default", []string{"namespace1"}, "backuplocation", "backupname1")
	createApplicationRestoreAndVerify(t, "deleterestore2", "default", []string{"namespace1"}, "backuplocation", "backupname2")
}

func TestCreateApplicationRestoreWaitSuccess(t *testing.T) {
	restoreStatusRetryInterval = 10 * time.Second
	defer resetTest()

	namespace := "dummy-namespace"
	name := "dummy-name"
	cmdArgs := []string{"create", "apprestores", "-n", namespace, name, "--backupLocation", "backuplocation", "--backupName", "backupname", "-w"}

	expected := "ApplicationRestore dummy-name started successfully\n" +
		"STAGE\t\tSTATUS              \n" +
		"\t\t                    \n" +
		"Volumes\t\tSuccessful          \n" +
		"ApplicationRestore dummy-name completed successfully\n"
	go setApplicationRestoreStatus(name, namespace, false, t)
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateApplicationRestoreWaitFailed(t *testing.T) {
	restoreStatusRetryInterval = 10 * time.Second
	defer resetTest()

	namespace := "dummy-namespace"
	name := "dummy-name"
	cmdArgs := []string{"create", "applicationrestore", "-n", namespace, name, "--backupLocation", "backuplocation", "--backupName", "backupname", "-w"}

	expected := "ApplicationRestore dummy-name started successfully\n" +
		"STAGE\t\tSTATUS              \n" +
		"\t\t                    \n" +
		"Volumes\t\tFailed              \n" +
		"ApplicationRestore dummy-name failed\n"
	go setApplicationRestoreStatus(name, namespace, true, t)
	testCommon(t, cmdArgs, nil, expected, false)
}

func setApplicationRestoreStatus(name, namespace string, isFail bool, t *testing.T) {
	time.Sleep(10 * time.Second)
	restore, err := k8s.Instance().GetApplicationRestore(name, namespace)
	require.NoError(t, err, "Error getting ApplicationRestore details")
	require.Equal(t, restore.Status.Status, storkv1.ApplicationRestoreStatusInitial)
	require.Equal(t, restore.Status.Stage, storkv1.ApplicationRestoreStageInitial)
	restore.Status.Status = storkv1.ApplicationRestoreStatusSuccessful
	restore.Status.Stage = storkv1.ApplicationRestoreStageVolumes
	if isFail {
		restore.Status.Status = storkv1.ApplicationRestoreStatusFailed
	}

	_, err = k8s.Instance().UpdateApplicationRestore(restore)
	require.NoError(t, err, "Error updating ApplicationRestores")
}
