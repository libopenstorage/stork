// +build integrationtest

package integrationtest

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/pborman/uuid"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	activateName   = "activate-cdu"
	deactivateName = "deactivate-cdu"
)

var (
	cdsName    string
	domainMap  = map[string]bool{"dc1": true, "dc2": true}
	domainList = []string{"dc1", "dc2"}
)

func TestClusterDomains(t *testing.T) {
	enabled, err := strconv.ParseBool(os.Getenv(enableClusterDomainTests))
	if enabled && err == nil {
		logrus.Info("Running cluster domain tests")
		listCdsTask := func() (interface{}, bool, error) {
			// Fetch the cluster domains
			cdses, err := storkops.Instance().ListClusterDomainStatuses()
			if err != nil || len(cdses.Items) == 0 {
				logrus.Infof("Failed to list cluster domains statuses. Error: %v. List of cluster domains: %v", err, len(cdses.Items))
				return "", true, fmt.Errorf("failed to list cluster domains statuses")
			}

			cds := cdses.Items[0]
			cdsName = cds.Name
			if len(cds.Status.ClusterDomainInfos) == 0 {
				logrus.Infof("Found 0 cluster domain info objects in cluster domain status.")
				return "", true, fmt.Errorf("failed to list cluster domains statuses")
			}
			return "", false, nil

		}
		_, err := task.DoRetryWithTimeout(listCdsTask, clusterDomainWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "expected list cluster domains status to succeed")

		t.Run("failoverAndFailbackTest", failoverAndFailbackClusterDomainTest)
	} else if err != nil {
		logrus.Errorf("Failed to run cluster domain tests: %v", err)
	} else if !enabled {
		logrus.Info("Skipping cluster domain tests")
	}
}

func triggerClusterDomainUpdate(
	t *testing.T,
	name string,
	domain string,
	active bool,
) {
	// run the command on the remote cluster as currently
	// we do not stop/deactivate the remote cluster
	err := setRemoteConfig(remoteFilePath)
	require.NoError(t, err, "Error resetting remote config")

	defer func() {
		err = setRemoteConfig("")
		require.NoError(t, err, "Error resetting remote config")
	}()

	updateName := name + uuid.New()
	_, err = storkops.Instance().CreateClusterDomainUpdate(&v1alpha1.ClusterDomainUpdate{
		ObjectMeta: meta.ObjectMeta{
			Name: updateName,
		},
		Spec: v1alpha1.ClusterDomainUpdateSpec{
			ClusterDomain: domain,
			Active:        active,
		},
	})
	require.NoError(t, err, "Unexpected error on cluster domain update: %v", err)

	err = storkops.Instance().ValidateClusterDomainUpdate(updateName, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Failed to validate cluster domain update")
}

func failoverAndFailbackClusterDomainTest(t *testing.T) {
	// validate the cluster domains status
	err := storkops.Instance().ValidateClusterDomainsStatus(cdsName, domainMap, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "validation of cluster domain status for %v failed", cdsName)

	// Migrate the resources
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"cassandra-clusterdomain-migration",
		"cassandra",
		nil,
		[]string{"cassandra-clusterdomain-migration"},
		true,
		true,
		false,
		false,
	)

	// validate the following
	// - migration is successful
	// - app starts on cluster 1
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, false, true, false)

	testClusterDomainsFailover(t, preMigrationCtx, ctxs)

	testClusterDomainsFailback(t, preMigrationCtx, ctxs)

	// destroy the apps and pvcs
	destroyAndWait(t, ctxs)
}

func testClusterDomainsFailover(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	ctxs []*scheduler.Context,
) {
	// Failover the application

	// Deactivate cluster domain
	triggerClusterDomainUpdate(
		t,
		deactivateName,
		domainList[0],
		false,
	)

	// build the expected domain map
	domainMap[domainList[0]] = false
	// validate the cluster domains status
	err := storkops.Instance().ValidateClusterDomainsStatus(cdsName, domainMap, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "validation of cluster domain status for %v failed", cdsName)

	// Reduce the replicas on cluster 1
	scaleFactor, err := schedulerDriver.GetScaleFactorMap(ctxs[0])
	require.NoError(t, err, "Unexpected error on GetScaleFactorMap")

	// Copy the old scale factor map
	oldScaleFactor := make(map[string]int32)
	for k := range scaleFactor {
		oldScaleFactor[k] = scaleFactor[k]
	}

	for k := range scaleFactor {
		scaleFactor[k] = 0
	}
	err = schedulerDriver.ScaleApplication(ctxs[0], scaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	tk := func() (interface{}, bool, error) {
		// check if the app is scaled down.
		updatedScaleFactor, err := schedulerDriver.GetScaleFactorMap(ctxs[0])
		if err != nil {
			return "", true, err
		}

		for k := range updatedScaleFactor {
			if int(updatedScaleFactor[k]) != 0 {
				return "", true, fmt.Errorf("expected scale to be 0")
			}
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(tk, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Unexpected error on scaling down application.")

	// start the app on cluster 2
	err = setRemoteConfig(remoteFilePath)
	require.NoError(t, err, "Error setting remote config")

	// Set scale factor to it's orignal values on cluster 1
	err = schedulerDriver.ScaleApplication(preMigrationCtx, oldScaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")
}

func testClusterDomainsFailback(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	ctxs []*scheduler.Context,
) {
	// Failback the application

	// Get scale factor on cluster 2
	oldScaleFactor, err := schedulerDriver.GetScaleFactorMap(ctxs[0])
	require.NoError(t, err, "Unexpected error on GetScaleFactorMap")

	// destroy the app on cluster 2
	err = schedulerDriver.Destroy(preMigrationCtx, nil)
	require.NoError(t, err, "Error destroying ctx: %+v", preMigrationCtx)
	err = schedulerDriver.WaitForDestroy(preMigrationCtx, defaultWaitTimeout)
	require.NoError(t, err, "Error waiting for destroy of ctx: %+v", preMigrationCtx)

	// Activate cluster domain
	triggerClusterDomainUpdate(
		t,
		activateName,
		domainList[0],
		true,
	)

	// build the expected domain map
	domainMap[domainList[0]] = true

	// validate the cluster domains status
	err = storkops.Instance().ValidateClusterDomainsStatus(cdsName, domainMap, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "validation of cluster domain status for %v failed", cdsName)

	// start the app on cluster 1
	err = setRemoteConfig("")
	require.NoError(t, err, "Error resetting remote config")

	err = schedulerDriver.ScaleApplication(ctxs[0], oldScaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state on source cluster after failback")

}
