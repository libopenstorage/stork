//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
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
	instanceIDs := []string{"failover-basic"}
	storageClass := "px-sc"
	migrationName := "failover-migration"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	cleanup(t, namespaces[0], storageClass)

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)

	validateMigrationOnSrc(t, migrationName, namespaces)

	deactivateClusterDomainAndTriggerFailover(
		t, actionName, namespaces, preMigrationCtxs, true)
}

// Failover namespace without a migration on the destination
func testFailoverWithoutMigration(t *testing.T) {
	appKey := "mysql-nearsync"
	instanceIDs := []string{"failover-without-migration"}
	storageClass := "px-sc"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	cleanup(t, namespaces[0], storageClass)

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)

	executeOnDestination(t, func() {
		_, err := createActionCR(t, actionName, namespaces[0])
		log.FailOnNoError(t, err, "create action CR should have errored out")
		err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout/4, defaultWaitInterval/4)
		log.FailOnNoError(t, err, "did not expect app to start")
	})
}

// Failover multiple namespaces simultaneously
func testFailoverForMultipleNamespaces(t *testing.T) {
	appKey := "mysql-nearsync"
	instanceIDs := []string{"failover-multiple-namespaces-1", "failover-multiple-namespaces-2"}
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

	validateMigrationOnSrc(t, migrationName, namespaces)

	deactivateClusterDomainAndTriggerFailover(
		t, actionName, namespaces, preMigrationCtxs, true)
}

// Failover a namespace with multiple running applications
func testFailoverWithMultipleApplications(t *testing.T) {
	appKey := "mysql-nearsync"
	additionalAppKeys := []string{"cassandra"}
	instanceIDs := []string{"failover-multiple-applications"}
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

	validateMigrationOnSrc(t, migrationName, namespaces)

	deactivateClusterDomainAndTriggerFailover(
		t, actionName, namespaces, preMigrationCtxs, true)
}

// assumes it will be executed with source config set
func deactivateClusterDomainAndTriggerFailover(
	t *testing.T,
	actionName string,
	namespaces []string,
	preMigrationCtxs []*scheduler.Context,
	isFailoverSuccessful bool,
) {
	clusterDomains, err := storkVolumeDriver.GetClusterDomains()
	log.FailOnError(t, err, "failed to get cluster domains")
	updateClusterDomain(t, clusterDomains, false, true)
	defer func() {
		updateClusterDomain(t, clusterDomains, true, true)
	}()
	executeOnDestination(t, func() {
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
	instanceIDs := []string{"failover-failed-promote-volume"}
	storageClass := "px-sc"
	migrationName := "failover-migration"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	cleanup(t, namespaces[0], storageClass)

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)

	validateMigrationOnSrc(t, migrationName, namespaces)

	scaleFactor := scaleDownApps(t, ctxs)
	log.InfoD("scaleFactor: %v", scaleFactor)

	pvcList, err := core.Instance().GetPersistentVolumeClaims(namespaces[0], map[string]string{})
	log.FailOnError(t, err, "Error getting pvcList")
	log.InfoD("pvc: %v", pvcList.Items[0].Name)

	pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvcList.Items[0])
	log.FailOnError(t, err, "Error getting volume for pvc")
	log.InfoD("pvName: %v", pvName)

	volume, err := volumeDriver.InspectVolume(pvName)
	log.FailOnError(t, err, "Error getting inspect volume for %v", pvName)

	nearSyncTargetMid, ok := volume.RuntimeState[0].RuntimeState["ReplicaSetNearSyncMid"]
	Dash.VerifyFatal(t, true, ok, "ReplicaSetNearSyncMid found in volume runtime state")
	log.InfoD("nearSyncTargetMid: %v", nearSyncTargetMid)

	funcRestartNode := func() {
		mapNodeIDToNode := node.GetNodesByVoDriverNodeID()
		log.InfoD("mapNodeIDToNode: %v", mapNodeIDToNode)
		nodeObj, _ := mapNodeIDToNode[nearSyncTargetMid]
		log.InfoD("node: %v", nodeObj)

		_, err = nodeDriver.RunCommand(
			nodeObj,
			"touch /root/whatAboutNow.txt",
			node.ConnectionOpts{
				Timeout:         1 * time.Minute,
				TimeBeforeRetry: 5 * time.Second,
			},
		)
		log.InfoD("run command on node: %v", nodeObj.Name)
		log.FailOnError(t, err, "Error running command on node: %v", nodeObj.Name)

		err = volumeDriver.StopDriver([]node.Node{nodeObj}, false, nil)
		log.FailOnError(t, err, "Error stopping volume driver on node: %v", nodeObj.Name)

		startFailover(t, actionName, namespaces)
		validateFailover(t, actionName, namespaces, preMigrationCtxs, false)

		err = volumeDriver.StartDriver(nodeObj)
		log.FailOnError(t, err, "Error starting volume driver on node: %v", nodeObj.Name)
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
		log.FailOnError(t, err, "error creating Action CR")
		log.InfoD("Created Action CR")
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
			log.FailOnError(t, err, "error waiting for app to get to running state")
			validateActionCR(t, actionName, namespaces[idx], true)
		} else {
			log.FailOnNoError(t, err, "did not expect app to get to running state")
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
