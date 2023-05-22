//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestAction(t *testing.T) {

	setupOnce(t)

	t.Run("testFailoverBasic", testFailoverBasic)
	t.Run("testFailoverWithoutMigration", testFailoverWithoutMigration)
	t.Run("testFailoverForMultipleNamespaces", testFailoverForMultipleNamespaces)
	t.Run("testFailoverWithMultipleApplications", testFailoverWithMultipleApplications)
	// t.Run("testFailoverForFailedPromoteVolume", testFailoverForFailedPromoteVolume)
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

// Simple failover with one namespace and one app
func testFailoverBasic(t *testing.T) {
	appKey := "mysql-nearsync"
	instanceIDs := []string{"failover"}
	storageClass := "px-sc"
	migrationName := "failover-migration"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	cleanup(t, namespaces[0], storageClass)

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)

	validateMigrationOnSrcAndDest(
		t, migrationName, namespaces[0], preMigrationCtxs[0],
		startAppsOnMigration, uint64(4), uint64(0))

	deactivateClusterDomainAndTriggerFailover(
		t, actionName, namespaces, preMigrationCtxs, true)
}

// Failover namespace without a migration on the destination
func testFailoverWithoutMigration(t *testing.T) {
	appKey := "mysql-nearsync"
	instanceIDs := []string{"failover"}
	storageClass := "px-sc"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	cleanup(t, namespaces[0], storageClass)

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)

	executeOnDestination(t, func() {
		_, err := createActionCR(t, actionName, namespaces[0])
		require.Error(t, err, "create action CR should have errored out")
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/4, defaultWaitInterval/4)
		require.Error(t, err, "did not expect app to start")
	})
}

// Failover multiple namespaces simultaneously
func testFailoverForMultipleNamespaces(t *testing.T) {
	appKey := "mysql-nearsync"
	instanceIDs := []string{"failover-1", "failover-2"}
	storageClass := "px-sc"
	migrationName := "failover-migration"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	for _, namespace := range namespaces {
		cleanup(t, namespace, storageClass)
	}

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)

	for idx, namespace := range namespaces {
		validateMigrationOnSrcAndDest(
			t, migrationName, namespace, preMigrationCtxs[idx],
			startAppsOnMigration, uint64(4), uint64(0))
	}

	deactivateClusterDomainAndTriggerFailover(
		t, actionName, namespaces, preMigrationCtxs, true)
}

// Failover a namespace with multiple running applications
func testFailoverWithMultipleApplications(t *testing.T) {
	appKey := "mysql-nearsync"
	additionalAppKeys := []string{"cassandra"}
	instanceIDs := []string{"failover"}
	storageClass := "px-sc"
	migrationName := "failover-migration"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	cleanup(t, namespaces[0], storageClass)

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)
	addTasksAndWait(t, ctxs[0], additionalAppKeys)

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)

	for idx, namespace := range namespaces {
		validateMigrationOnSrcAndDest(
			t, migrationName, namespace, preMigrationCtxs[idx],
			startAppsOnMigration, uint64(4), uint64(0))
	}

	deactivateClusterDomainAndTriggerFailover(
		t, actionName, namespaces, preMigrationCtxs, true)
}

func deactivateClusterDomainAndTriggerFailover(
	t *testing.T,
	actionName string,
	namespaces []string,
	preMigrationCtxs []*scheduler.Context,
	isFailoverSuccessful bool,
) {
	srcNode := node.GetStorageDriverNodes()[0]
	executeOnDestination(t, func() {
		destNode := node.GetStorageDriverNodes()[0]
		updateClusterDomain(t, false, srcNode, destNode, false)
		defer func() {
			setSourceKubeConfig()
			updateClusterDomain(t, true, srcNode, destNode, true)
		}()
		startFailover(t, actionName, namespaces)
		validateFailover(t, actionName, namespaces, preMigrationCtxs, isFailoverSuccessful)
	})
}

// Failover a namespace when the nearsync node for a volume is down
// TODO(horntail): this test still needs to be tested and the enabled
// It depends on a change that will prevent promote call from going
// through if PX is down on the nearsync node for a volume
func testFailoverForFailedPromoteVolume(t *testing.T) {
	appKey := "mysql-nearsync"
	instanceIDs := []string{"failover"}
	storageClass := "px-sc"
	migrationName := "failover-migration"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	cleanup(t, namespaces[0], storageClass)

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)

	for idx, namespace := range namespaces {
		validateMigrationOnSrcAndDest(
			t, migrationName, namespace, preMigrationCtxs[idx],
			startAppsOnMigration, uint64(4), uint64(0))
	}

	scaleFactor := scaleDownApps(t, ctxs)
	logrus.Infof("scaleFactor: %v", scaleFactor)

	pvcList, err := core.Instance().GetPersistentVolumeClaims(namespaces[0], map[string]string{})
	require.NoError(t, err, "Error getting pvcList")
	logrus.Infof("pvc: %v", pvcList.Items[0].Name)

	pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvcList.Items[0])
	require.NoError(t, err, "Error getting volume for pvc")
	logrus.Infof("pvName: %v", pvName)

	volume, err := volumeDriver.InspectVolume(pvName)
	require.NoError(t, err, "Error getting inspect volume for %v", pvName)

	nearSyncTargetMid, ok := volume.RuntimeState[0].RuntimeState["ReplicaSetNearSyncMid"]
	require.Equal(t, true, ok)
	logrus.Infof("nearSyncTargetMid: %v", nearSyncTargetMid)

	funcRestartNode := func() {
		mapNodeIDToNode := node.GetNodesByVoDriverNodeID()
		logrus.Infof("mapNodeIDToNode: %v", mapNodeIDToNode)
		nodeObj, _ := mapNodeIDToNode[nearSyncTargetMid]
		logrus.Infof("node: %v", nodeObj)

		_, err = nodeDriver.RunCommand(
			nodeObj,
			"touch /root/whatAboutNow.txt",
			node.ConnectionOpts{
				Timeout:         1 * time.Minute,
				TimeBeforeRetry: 5 * time.Second,
			},
		)
		logrus.Infof("run command on node: %v", nodeObj.Name)
		require.NoError(t, err)

		err = volumeDriver.StopDriver([]node.Node{nodeObj}, false, nil)
		require.NoError(t, err)

		startFailover(t, actionName, namespaces)
		validateFailover(t, actionName, namespaces, preMigrationCtxs, false)

		err = volumeDriver.StartDriver(nodeObj)
		require.NoError(t, err)
	}
	executeOnDestination(t, funcRestartNode)
}

func startFailover(
	t *testing.T,
	actionName string,
	namespaces []string,
) {
	for _, namespace := range namespaces {
		_, err := createActionCR(t, actionName, namespace)
		require.NoError(t, err, "error creating Action CR")
	}
}

func validateFailover(
	t *testing.T,
	actionName string,
	namespaces []string,
	preMigrationCtxs []*scheduler.Context,
	isSuccessful bool,
) {
	for idx, ctx := range preMigrationCtxs {
		err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		if isSuccessful {
			require.NoError(t, err, "error waiting for app to get to running state")
			validateActionCR(t, actionName, namespaces[idx], true)
		} else {
			require.Error(t, err, "did not expect app to get to running state")
			validateActionCR(t, actionName, namespaces[idx], false)
		}
	}
}

func getNamespaces(instanceIDs []string, appKey string) []string {
	var namespaces []string
	for _, instanceID := range instanceIDs {
		namespaces = append(namespaces, fmt.Sprintf("%v-%v", appKey, instanceID))
	}
	return namespaces
}

func updateClusterDomain(t *testing.T, activate bool, srcNode, destNode node.Node, wait bool) {
	var cmd string
	// TODO: query for cluster domain and use it here
	if activate {
		cmd = "cluster domains activate --name dc1"
	} else {
		cmd = "cluster domains deactivate --name dc1"
	}
	out, err := volumeDriver.GetPxctlCmdOutput(destNode, cmd)
	require.NoError(t, err)
	if wait {
		if activate {
			logrus.Infof("Not waiting for driver to come up on node %v", srcNode.GetDataIp())
			// TODO: WaitDriverUpOnNode fails as it receives 0 pods for the GetPodsByNode call
			// err = volumeDriver.WaitDriverUpOnNode(srcNode, defaultWaitTimeout)
			// require.NoError(t, err)
		} else {
			logrus.Infof("Waiting for driver to do go down on node %v", srcNode.GetDataIp())
			err = volumeDriver.WaitDriverDownOnNode(srcNode)
			require.NoError(t, err)
		}
	}
	logrus.Infof(out)
}
