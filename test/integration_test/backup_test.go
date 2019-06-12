// +build integrationtest

package integrationtest

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testKey = "mysql-1-pvc"
	appKey  = "mysql"
)

func testBackup(t *testing.T) {
	t.Run("backupTest", backupRestoreDeleteTest)
}

func triggerBackupRestoreTest(
	t *testing.T,
	appBackupKey []string,
	appRestoreKey []string,
) {
	var err error
	var ctxs []*scheduler.Context
	ctx := createApp(t, appKey)
	ctxs = append(ctxs, ctx)
	logrus.Infof("App created %v. Starting backup.", ctx.GetID())

	// Create backuplocation here programatically using config-map that contains name of secrets to be used, passed from the CLI
	err = createBackupLocation(t, appKey+"-backup-location", ctx.GetID(), storkv1.BackupLocationS3, "secret-config")
	require.NoError(t, err, "Error creating backuplocation")

	// Backup application
	err = schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: appBackupKey})
	require.NoError(t, err, "Error backing-up apps")
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for back-up to complete.")

	logrus.Infof("Backup completed. Starting Restore.")

	// Restore application
	err = schedulerDriver.AddTasks(ctxs[0],
		scheduler.ScheduleOptions{AppKeys: appRestoreKey})
	require.NoError(t, err, "Error restoring apps")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for restore to complete.")

	logrus.Infof("Restore completed.")

	// Validate applications after restore
	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for restore to complete.")

	logrus.Infof("App validation after restore completed.")

	destroyAndWait(t, ctxs)
}

func createBackupLocation(
	t *testing.T,
	name string,
	namespace string,
	locationtype storkv1.BackupLocationType,
	configMapName string,
) error {
	configMap, err := k8s.Instance().GetConfigMap(configMapName, "default")
	require.NoError(t, err, "Failed to get config map  %s", configMapName)

	secretName := configMap.Data["secret_name"]

	secretObj, err := k8s.Instance().GetSecret(secretName, "default")
	require.NoError(t, err, "Failed to get secret %s", secretName)

	// copy secret to the app namespace
	newSecretObj := secretObj.DeepCopy()
	newSecretObj.Namespace = namespace
	newSecretObj.ResourceVersion = ""
	newSecret, err := k8s.Instance().CreateSecret(newSecretObj)
	require.NoError(t, err, "Failed to copy secret %s  to namespace %s", name, namespace)

	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: meta.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{"stork.libopenstorage.ord/skipresource": "true"},
		},
		Location: storkv1.BackupLocationItem{
			Type:         locationtype,
			Path:         "test-restore-path",
			SecretConfig: newSecret.Name,
		},
	}
	_, err = k8s.Instance().CreateBackupLocation(backupLocation)
	return err
}

func backupRestoreDeleteTest(t *testing.T) {
	triggerBackupRestoreTest(
		t,
		[]string{testKey + "-backup"},
		[]string{"mysql-restore"},
	)
}
