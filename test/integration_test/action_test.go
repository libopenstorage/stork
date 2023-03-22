//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

var (
	appKeyMySQL = "mysql-enc-pvc"
)

func TestAction(t *testing.T) {

	setupOnce(t)

	// t.Run("testFailoverBasic", testFailoverBasic)
	// t.Run("testFailoverForMultipleNamespaces", testFailoverForMultipleNamespaces)
	// t.Run("testFailoverWithMultipleApplications", testFailoverWithMultipleApplications)
	t.Run("testFailoverForFailedPromoteVolume", testFailoverForFailedPromoteVolume)
}

func setupOnce(t *testing.T) {
	funcCreateSecret := func() {
		_ = createSecret(
			t,
			"volume-secrets",
			map[string]string{
				"mysql-secret": "supersecretpassphrase",
			})
	}
	funcCreateSecret()
	executeOnDestination(t, funcCreateSecret)
}

// test basic workflow:
// 1. start an app on source
// 2. migrate k8s resources to destination
// 3. scale down app on source and do failover on dest
func testFailoverBasic(t *testing.T) {

	appKey := "mysql-enc-pvc"
	instanceID := "failover"
	migrationAppKey := "failover-mysql-migration"
	actionName := "failover-action"

	namespace := fmt.Sprintf("%v-%v", appKey, instanceID)

	cleanup(t, namespace)

	// starts the app on src,
	// sets cluster pair,
	// creates a migration
	ctxs, preMigrationCtx := triggerMigration(
		t, instanceID, appKey, nil, []string{migrationAppKey}, true, true, false, false, "", nil)

	// validate the following
	// - migration is successful
	// - app doesn't start on dest
	validateAndDestroyMigration(
		t, ctxs, preMigrationCtx, true, false, true, true, true)
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// extract migrationObj from specList
	var migrationObj *v1alpha1.Migration
	var ok bool
	for _, specObj := range ctxs[0].App.SpecList {
		if migrationObj, ok = specObj.(*v1alpha1.Migration); ok {
			break
		}
	}

	expectedResources := uint64(4) // 1 sts, 1 service, 1 pvc, 1 pv
	expectedVolumes := uint64(0)   // 0 volume
	// validate the migration summary based on the application specs that were deployed by the test
	validateMigrationSummary(
		t, preMigrationCtx, expectedResources, expectedVolumes, migrationObj.Name, migrationObj.Namespace)

	scaleFactor := scaleDownApps(t, ctxs)
	logrus.Infof("scaleFactor: %v", scaleFactor)

	startAndValidateFailover := func() {
		_ = createActionCR(t, actionName, namespace, ctxs[0])

		// pass preMigrationCtx to only check if the mysql app is running on destination
		err := schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "error waiting for app to get to running state")

		// if above call to WaitForRunning is successful,
		// then Action validateActionCR will be successful
		validateActionCR(t, actionName, namespace)
	}
	executeOnDestination(t, startAndValidateFailover)
}

func testFailoverWithoutMigration(t *testing.T) {
	appKey := "mysql-enc-pvc"
	instanceID := "failover"
	actionName := "failover-action"
	namespace := fmt.Sprintf("%v-%v", appKey, instanceID)

	defer cleanup(t, namespace)

	ctx := scheduleAppAndWait(t, instanceID, appKey)

	startAndValidateFailover := func() {
		_ = createActionCR(t, actionName, namespace, ctx)

		// check mysql app does NOT start on destination
		err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		require.Error(t, err, "error waiting for app to get to running state")

		// Action CR should fail with: No migration found
		// TODO(dgoel): discuss and handle this error case
		validateActionCR(t, actionName, namespace)
	}
	executeOnDestination(t, startAndValidateFailover)
}

func testFailoverForFailedPromoteVolume(t *testing.T) {
	appKey := "mysql-enc-pvc"
	instanceID := "failover"
	migrationName := "failover-migration"
	actionName := "failover-action"

	namespace := fmt.Sprintf("%v-%v", appKey, instanceID)
	logrus.Infof("len(namespace): %v", len(namespace))
	cleanup(t, namespace)
	// wait for cleanup to complete
	time.Sleep(time.Second * 20)

	ctx := scheduleAppAndWait(t, instanceID, appKey)

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, migrationList := triggerMigrationMultiple(
		t, []*scheduler.Context{ctx}, []string{migrationName},
		[]string{namespace}, true, false, startAppsOnMigration)
	ctx = ctxs[0]

	for idx, migration := range migrationList {
		validateMigrationOnSrcAndDest(
			t, migration.Name, migration.Namespace, preMigrationCtxs[idx],
			startAppsOnMigration, uint64(4), uint64(0))
	}

	scaleFactor := scaleDownApps(t, ctxs)
	logrus.Infof("scaleFactor: %v", scaleFactor)

	funcStartAndValidateFailoverMultiple := func() {
		volumes, err := schedulerDriver.GetVolumes(ctx)
		require.NoError(t, err, "Error getting volumes")
		logrus.Infof("Volumes: %v", volumes[0].Name)

		nodeObj, err := volumeDriver.GetNodeForVolume(volumes[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error getting node")
		logrus.Infof("Node: %v", nodeObj.Name)

		nodeDriver.RebootNode(
			*nodeObj,
			node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         1 * time.Minute,
					TimeBeforeRetry: 5 * time.Second,
				},
			})
		logrus.Infof("Reboot Node: %v", nodeObj.Name)

		for _, ctx := range preMigrationCtxs {
			_ = createActionCR(t, actionName, namespace, ctx)
		}
		for _, ctx := range preMigrationCtxs {
			err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "error waiting for app to get to running state")

			validateActionCR(t, actionName, namespace)
		}
	}
	executeOnDestination(t, funcStartAndValidateFailoverMultiple)
}

// func testFailoverForNamespaceKubeSystem(t *testing.T) {
// }

func testFailoverForMultipleNamespaces(t *testing.T) {
	appKey := "mysql-enc-pvc"
	instanceIDList := []string{"failover-1", "failover-2"}
	migrationAppKey := "failover-mysql-migration"
	actionName := "failover-action"
	var namespaceList []string

	for _, instanceID := range instanceIDList {
		namespaceList = append(namespaceList, fmt.Sprintf("%v-%v", appKey, instanceID))
		logrus.Infof("len(namespaceList): %v", len(namespaceList))
		cleanup(t, namespaceList[len(namespaceList)-1])
	}
	// wait for cleanup to complete
	time.Sleep(time.Second * 20)

	ctxs := scheduleAppAndWaitMultiple(t, instanceIDList, appKey)

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, migrationList := triggerMigrationMultiple(
		t, ctxs, []string{migrationAppKey}, namespaceList, true, false, startAppsOnMigration)

	for idx, migration := range migrationList {
		validateMigrationOnSrcAndDest(
			t, migration.Name, migration.Namespace, preMigrationCtxs[idx], startAppsOnMigration, uint64(4), uint64(0))
	}

	scaleFactor := scaleDownApps(t, ctxs)
	logrus.Infof("scaleFactor: %v", scaleFactor)

	funcStartAndValidateFailoverMultiple := func() {
		for idx, ctx := range preMigrationCtxs {
			_ = createActionCR(t, actionName, namespaceList[idx], ctx)
		}
		for idx, ctx := range ctxs {
			err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "error waiting for app to get to running state")

			validateActionCR(t, actionName, namespaceList[idx])
		}
	}
	executeOnDestination(t, funcStartAndValidateFailoverMultiple)
}

func testFailoverWithMultipleApplications(t *testing.T) {
	appKeys := []string{"mysql-enc-pvc", "cassandra"}
	instanceID := "failover"
	migrationName := "failover-migration"
	actionName := "failover-action"

	namespace := fmt.Sprintf("%v-%v", appKeys[0], instanceID)
	logrus.Infof("len(namespace): %v", len(namespace))
	cleanup(t, namespace)
	// wait for cleanup to complete
	time.Sleep(time.Second * 20)

	ctx := scheduleAppAndWait(t, instanceID, appKeys[0])
	scheduleTasksAndWait(t, ctx, appKeys[1:])

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, migrationList := triggerMigrationMultiple(
		t, []*scheduler.Context{ctx}, []string{migrationName},
		[]string{namespace}, true, false, startAppsOnMigration)

	for idx, migration := range migrationList {
		validateMigrationOnSrcAndDest(
			t, migration.Name, migration.Namespace, preMigrationCtxs[idx],
			startAppsOnMigration, uint64(4), uint64(0))
	}

	scaleFactor := scaleDownApps(t, ctxs)
	logrus.Infof("scaleFactor: %v", scaleFactor)

	funcStartAndValidateFailoverMultiple := func() {
		for _, ctx := range preMigrationCtxs {
			_ = createActionCR(t, actionName, namespace, ctx)
		}
		for _, ctx := range preMigrationCtxs {
			err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "error waiting for app to get to running state")

			validateActionCR(t, actionName, namespace)
		}
	}
	executeOnDestination(t, funcStartAndValidateFailoverMultiple)
}

// func testFailoverOneActionPolicy(t *testing.T) {
// }
// func testFailoverWithFailedVolumePromote(t *testing.T) {
// }
