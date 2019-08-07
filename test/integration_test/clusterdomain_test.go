// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/scheduler"
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

func testClusterDomains(t *testing.T) {
	// Fetch the cluster domains
	cdses, err := k8s.Instance().ListClusterDomainStatuses()
	if err != nil || len(cdses.Items) == 0 {
		t.Skipf("Skipping cluster domain tests: %v", err)
	}
	cds := cdses.Items[0]
	cdsName = cds.Name

	if len(cds.Status.ClusterDomainInfos) == 0 {
		t.Skip("Skipping cluster domain tests: No cluster domains found")
	}

	t.Run("failoverAndFailbackTest", failoverAndFailbackClusterDomainTest)
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
	_, err = k8s.Instance().CreateClusterDomainUpdate(&v1alpha1.ClusterDomainUpdate{
		ObjectMeta: meta.ObjectMeta{
			Name: updateName,
		},
		Spec: v1alpha1.ClusterDomainUpdateSpec{
			ClusterDomain: domain,
			Active:        active,
		},
	})
	require.NoError(t, err, "Unexpected error on cluster domain update: %v", err)

	err = k8s.Instance().ValidateClusterDomainUpdate(updateName, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Failed to validate cluster domain update")
}

func failoverAndFailbackClusterDomainTest(t *testing.T) {
	// validate the cluster domains status
	err := k8s.Instance().ValidateClusterDomainsStatus(cdsName, domainMap, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "validation of cluster domain status for %v failed", cdsName)

	// Migrate the resources
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"mysql-clusterdomain-migration",
		"mysql-1-pvc",
		nil,
		[]string{"mysql-clusterdomain-migration"},
		true,
		true,
	)

	// validate the following
	// - migration is successful
	// - app starts on cluster 1
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, false, true)

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
	err := k8s.Instance().ValidateClusterDomainsStatus(cdsName, domainMap, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "validation of cluster domain status for %v failed", cdsName)

	// Reduce the replicas on cluster 1
	scaleFactor, err := schedulerDriver.GetScaleFactorMap(ctxs[0])
	require.NoError(t, err, "Unexpected error on GetScaleFactorMap")
	for k := range scaleFactor {
		scaleFactor[k] = 0
	}
	err = schedulerDriver.ScaleApplication(ctxs[0], scaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	// start the app on cluster 2
	err = setRemoteConfig(remoteFilePath)
	require.NoError(t, err, "Error setting remote config")

	// Increase the replicas on cluster 1
	scaleFactor, err = schedulerDriver.GetScaleFactorMap(preMigrationCtx)
	require.NoError(t, err, "Unexpected error on GetScaleFactorMap")
	for k := range scaleFactor {
		scaleFactor[k] = 1
	}
	err = schedulerDriver.ScaleApplication(preMigrationCtx, scaleFactor)
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

	// destroy the app on cluster 2
	err := schedulerDriver.Destroy(preMigrationCtx, nil)
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
	err = k8s.Instance().ValidateClusterDomainsStatus(cdsName, domainMap, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "validation of cluster domain status for %v failed", cdsName)

	// start the app on cluster 1
	err = setRemoteConfig("")
	require.NoError(t, err, "Error resetting remote config")

	// Reduce the replicas on cluster 1
	scaleFactor, err := schedulerDriver.GetScaleFactorMap(ctxs[0])
	require.NoError(t, err, "Unexpected error on GetScaleFactorMap")
	for k := range scaleFactor {
		scaleFactor[k] = 1
	}
	err = schedulerDriver.ScaleApplication(ctxs[0], scaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state on source cluster after failback")

}
