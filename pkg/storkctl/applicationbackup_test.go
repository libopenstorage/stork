// +build unittest

package storkctl

import (
	"strings"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetBackupsNoBackup(t *testing.T) {
	cmdArgs := []string{"get", "applicationbackups"}

	var backupList storkv1.ApplicationBackupList
	expected := "No resources found.\n"
	testCommon(t, cmdArgs, &backupList, expected, false)
}

func createApplicationBackupAndVerify(
	t *testing.T,
	name string,
	namespace string,
	namespaces []string,
	backupLocation string,
	preExecRule string,
	postExecRule string,
) {
	cmdArgs := []string{"create", "backups", "-n", namespace, "--namespaces", strings.Join(namespaces, ","), name, "--backupLocation", backupLocation}
	if preExecRule != "" {
		cmdArgs = append(cmdArgs, "--preExecRule", preExecRule)
	}
	if postExecRule != "" {
		cmdArgs = append(cmdArgs, "--postExecRule", postExecRule)
	}

	expected := "ApplicationBackup " + name + " started successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	backup, err := storkops.Instance().GetApplicationBackup(name, namespace)
	require.NoError(t, err, "Error getting backup")
	require.Equal(t, name, backup.Name, "ApplicationBackup name mismatch")
	require.Equal(t, namespace, backup.Namespace, "ApplicationBackup namespace mismatch")
	require.Equal(t, namespaces, backup.Spec.Namespaces, "ApplicationBackup namespace mismatch")
	require.Equal(t, preExecRule, backup.Spec.PreExecRule, "ApplicationBackup preExecRule mismatch")
	require.Equal(t, postExecRule, backup.Spec.PostExecRule, "ApplicationBackup postExecRule mismatch")
	require.Equal(t, backupLocation, backup.Spec.BackupLocation, "ApplicationBackup backupLocation mismatch")
}

func TestGetApplicationBackupsOneApplicationBackup(t *testing.T) {
	defer resetTest()
	createApplicationBackupAndVerify(t, "getbackuptest", "test", []string{"namespace1"}, "backuplocation", "preExec", "postExec")

	expected := "NAME            STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getbackuptest                    0/0       0                     \n"

	cmdArgs := []string{"get", "backups", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationBackupsMultiple(t *testing.T) {
	defer resetTest()
	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	require.NoError(t, err, "Error creating default namespace")

	createApplicationBackupAndVerify(t, "getbackuptest1", "default", []string{"namespace1"}, "backuplocation", "", "")
	createApplicationBackupAndVerify(t, "getbackuptest2", "default", []string{"namespace1"}, "backuplocation", "", "")

	expected := "NAME             STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getbackuptest1                    0/0       0                     \n" +
		"getbackuptest2                    0/0       0                     \n"

	cmdArgs := []string{"get", "backups", "getbackuptest1", "getbackuptest2"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all backups if no name given
	cmdArgs = []string{"get", "backups"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "NAME             STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getbackuptest1                    0/0       0                     \n"
	// Should get only one backup if name given
	cmdArgs = []string{"get", "backups", "getbackuptest1"}
	testCommon(t, cmdArgs, nil, expected, false)

	_, err = core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}})
	require.NoError(t, err, "Error creating ns1 namespace")
	createApplicationBackupAndVerify(t, "getbackuptest21", "ns1", []string{"namespace1"}, "backuplocation", "", "")
	cmdArgs = []string{"get", "backups", "--all-namespaces"}
	expected = "NAMESPACE   NAME              STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"default     getbackuptest1                     0/0       0                     \n" +
		"default     getbackuptest2                     0/0       0                     \n" +
		"ns1         getbackuptest21                    0/0       0                     \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationBackupsWithStatusAndProgress(t *testing.T) {
	defer resetTest()
	createApplicationBackupAndVerify(t, "getbackupstatustest", "default", []string{"namespace1"}, "backuplocation", "", "")
	backup, err := storkops.Instance().GetApplicationBackup("getbackupstatustest", "default")
	require.NoError(t, err, "Error getting backup")

	// Update the status of the backup
	backup.Status.FinishTimestamp = metav1.Now()
	backup.CreationTimestamp = metav1.NewTime(backup.Status.FinishTimestamp.Add(-5 * time.Minute))
	backup.Status.TriggerTimestamp = metav1.NewTime(backup.Status.FinishTimestamp.Add(-5 * time.Minute))
	backup.Status.Stage = storkv1.ApplicationBackupStageFinal
	backup.Status.Status = storkv1.ApplicationBackupStatusSuccessful
	backup.Status.Volumes = []*storkv1.ApplicationBackupVolumeInfo{}
	_, err = storkops.Instance().UpdateApplicationBackup(backup)
	require.NoError(t, err, "Error updating backup")

	expected := "NAME                  STAGE   STATUS       VOLUMES   RESOURCES   CREATED               ELAPSED\n" +
		"getbackupstatustest   Final   Successful   0/0       0           " + toTimeString(backup.Status.TriggerTimestamp.Time) + "   5m0s\n"
	cmdArgs := []string{"get", "backups", "getbackupstatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateApplicationBackupsNoNamespace(t *testing.T) {
	cmdArgs := []string{"create", "backups", "backup1"}

	expected := "error: need to provide atleast one namespace to backup"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateApplicationBackupsNoName(t *testing.T) {
	cmdArgs := []string{"create", "backups"}

	expected := "error: exactly one name needs to be provided for applicationbackup name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateApplicationBackups(t *testing.T) {
	defer resetTest()
	createApplicationBackupAndVerify(t, "createbackup", "default", []string{"namespace1"}, "backuplocation", "", "")
}

func TestCreateDuplicateApplicationBackups(t *testing.T) {
	defer resetTest()
	createApplicationBackupAndVerify(t, "createbackup", "default", []string{"namespace1"}, "backuplocation", "", "")
	cmdArgs := []string{"create", "backups", "--namespaces", "namespace1", "createbackup", "--backupLocation", "backuplocation"}

	expected := "Error from server (AlreadyExists): applicationbackups.stork.libopenstorage.org \"createbackup\" already exists"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteApplicationBackupsNoApplicationBackupName(t *testing.T) {
	cmdArgs := []string{"delete", "backups"}

	var backupList storkv1.ApplicationBackupList
	expected := "error: at least one argument needs to be provided for applicationbackup name"
	testCommon(t, cmdArgs, &backupList, expected, true)
}

func TestDeleteApplicationBackups(t *testing.T) {
	defer resetTest()
	createApplicationBackupAndVerify(t, "deletebackup", "default", []string{"namespace1"}, "backuplocation", "", "")

	cmdArgs := []string{"delete", "backups", "deletebackup"}
	expected := "ApplicationBackup deletebackup deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"delete", "backups", "deletebackup"}
	expected = "Error from server (NotFound): applicationbackups.stork.libopenstorage.org \"deletebackup\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	createApplicationBackupAndVerify(t, "deletebackup1", "default", []string{"namespace1"}, "backuplocation", "", "")
	createApplicationBackupAndVerify(t, "deletebackup2", "default", []string{"namespace1"}, "backuplocation", "", "")

	cmdArgs = []string{"delete", "backups", "deletebackup1", "deletebackup2"}
	expected = "ApplicationBackup deletebackup1 deleted successfully\n"
	expected += "ApplicationBackup deletebackup2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	createApplicationBackupAndVerify(t, "deletebackup1", "default", []string{"namespace1"}, "backuplocation", "", "")
	createApplicationBackupAndVerify(t, "deletebackup2", "default", []string{"namespace1"}, "backuplocation", "", "")
}

func TestCreateApplicationBackupWaitSuccess(t *testing.T) {
	backupStatusRetryInterval = 10 * time.Second
	defer resetTest()

	namespace := "dummy-namespace"
	name := "dummy-name"
	cmdArgs := []string{"create", "backups", "-n", namespace, "--namespaces", namespace, name, "--backupLocation", "backuplocation", "-w"}

	expected := "ApplicationBackup dummy-name started successfully\n" +
		"STAGE\t\tSTATUS              \n" +
		"\t\t                    \n" +
		"Volumes\t\tSuccessful          \n" +
		"ApplicationBackup dummy-name completed successfully\n"
	go setApplicationBackupStatus(name, namespace, false, t)
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateApplicationBackupWaitFailed(t *testing.T) {
	backupStatusRetryInterval = 10 * time.Second
	defer resetTest()

	namespace := "dummy-namespace"
	name := "dummy-name"
	cmdArgs := []string{"create", "applicationbackup", "-n", namespace, "--namespaces", namespace, name, "--backupLocation", "backuplocation", "-w"}

	expected := "ApplicationBackup dummy-name started successfully\n" +
		"STAGE\t\tSTATUS              \n" +
		"\t\t                    \n" +
		"Volumes\t\tFailed              \n" +
		"ApplicationBackup dummy-name failed\n"
	go setApplicationBackupStatus(name, namespace, true, t)
	testCommon(t, cmdArgs, nil, expected, false)
}

func setApplicationBackupStatus(name, namespace string, isFail bool, t *testing.T) {
	time.Sleep(10 * time.Second)
	backup, err := storkops.Instance().GetApplicationBackup(name, namespace)
	require.NoError(t, err, "Error getting ApplicationBackup details")
	require.Equal(t, backup.Status.Status, storkv1.ApplicationBackupStatusInitial)
	require.Equal(t, backup.Status.Stage, storkv1.ApplicationBackupStageInitial)
	backup.Status.Status = storkv1.ApplicationBackupStatusSuccessful
	backup.Status.Stage = storkv1.ApplicationBackupStageVolumes
	if isFail {
		backup.Status.Status = storkv1.ApplicationBackupStatusFailed
	}

	_, err = storkops.Instance().UpdateApplicationBackup(backup)
	require.NoError(t, err, "Error updating ApplicationBackups")
}
