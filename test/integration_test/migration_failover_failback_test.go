// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMigrationFailoverFailback(t *testing.T) {
	// Create secrets on source and destination
	// Since the secrets need to be created on the destination before migration
	// is triggered using the API instead of spec factory in torpedo
	err := setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	secret := &v1.Secret{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      "volume-secrets",
			Namespace: "kube-system",
		},
		StringData: map[string]string{
			"mysql-secret": "supersecretpassphrase",
		},
	}
	_, err = core.Instance().CreateSecret(secret)
	require.NoError(t, err, "failed to create secret for volumes")

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	_, err = core.Instance().CreateSecret(secret)
	require.NoError(t, err, "failed to create secret for volumes")

	t.Run("failoverAndFailbackMigrationTest", failoverAndFailbackMigrationTest)
}

func failoverAndFailbackMigrationTest(t *testing.T) {

	// Migrate the resources
	ctxs, preMigrationCtx := triggerMigration(
		t,
		"mysql-migration-failover-failback",
		"mysql-enc-pvc",
		nil,
		[]string{"mysql-migration-failover-failback"},
		true,
		false,
		false,
		false,
	)

	// validate the following
	// - migration is successful
	// - app starts on cluster 1
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, true, true)

	scaleFactor := testMigrationFailover(t, preMigrationCtx, ctxs)

	testMigrationFailback(t, preMigrationCtx, ctxs, scaleFactor)
}

func testMigrationFailover(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	ctxs []*scheduler.Context,
) map[string]int32 {
	// Failover the application

	// Reduce the replicas on cluster 1
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

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
	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting remote config")

	// Set scale factor to it's orignal values on cluster 2
	err = schedulerDriver.ScaleApplication(preMigrationCtx, oldScaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")

	return oldScaleFactor
}

func testMigrationFailback(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	ctxs []*scheduler.Context,
	scaleFactor map[string]int32,
) {
	// Failback the application
	// Trigger a reverse migration

	ctxsReverse, err := schedulerDriver.Schedule("mysql-migration-failover-failback",
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-enc-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxsReverse), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	postMigrationCtx := ctxsReverse[0].DeepCopy()

	// create, apply and validate cluster pair specs
	err = scheduleClusterPair(ctxsReverse[0], false, false, "cluster-pair-reverse", true)
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxsReverse[0],
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-migration-failover-failback"}})
	require.NoError(t, err, "Error scheduling migration specs")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to complete")

	// destroy the app on cluster 2
	err = schedulerDriver.Destroy(preMigrationCtx, nil)
	require.NoError(t, err, "Error destroying ctx: %+v", preMigrationCtx)
	err = schedulerDriver.WaitForDestroy(preMigrationCtx, defaultWaitTimeout)
	require.NoError(t, err, "Error waiting for destroy of ctx: %+v", preMigrationCtx)

	// ensure app starts on cluster 1
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error resetting remote config")

	// Set scale factor to it's orignal values on cluster 2
	err = schedulerDriver.ScaleApplication(postMigrationCtx, scaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	err = schedulerDriver.WaitForRunning(postMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state on source cluster after failback")

	destroyAndWait(t, []*scheduler.Context{postMigrationCtx})
}
