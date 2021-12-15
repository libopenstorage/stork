//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"

	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
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

	var originalScaleFactor map[string]int32
	var err error

	originalScaleFactor, err = schedulerDriver.GetScaleFactorMap(ctxs[0])
	require.NoError(t, err, "Unexpected error on GetScaleFactorMap")

	for i := 1; i <= failOverFailBackScaleCount; i++ {
		logrus.Infof("Starting failover failback iteration: %d", i)
		scaleFactor := testMigrationFailover(t, preMigrationCtx, ctxs, originalScaleFactor, i)

		testMigrationFailback(t, preMigrationCtx, ctxs, scaleFactor, i)
	}
}

func testMigrationFailover(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	ctxs []*scheduler.Context,
	origScaleFactor map[string]int32,
	iteration int,
) map[string]int32 {
	// Failover the application

	// Reduce the replicas on cluster 1
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	scaleFactor, err := schedulerDriver.GetScaleFactorMap(ctxs[0])
	require.NoError(t, err, "Unexpected error on GetScaleFactorMap")

	// Copy the old scale factor map
	oldScaleFactor := make(map[string]int32)
	for k := range origScaleFactor {
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
	iteration int,
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

	if iteration == 1 {
		// create, apply and validate cluster pair specs, only for the first iteration
		err = scheduleClusterPair(ctxsReverse[0], false, false, "cluster-pair-reverse", true)
		require.NoError(t, err, "Error scheduling cluster pair")
	}

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxsReverse[0],
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-migration-failover-failback"}})
	require.NoError(t, err, "Error scheduling migration specs")

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to complete")

	// destroy the app on cluster 2 only if it's the last iteration
	if iteration == failOverFailBackScaleCount {
		err = schedulerDriver.Destroy(preMigrationCtx, nil)
		require.NoError(t, err, "Error destroying ctx: %+v", preMigrationCtx)
		err = schedulerDriver.WaitForDestroy(preMigrationCtx, defaultWaitTimeout)
		require.NoError(t, err, "Error waiting for destroy of ctx: %+v", preMigrationCtx)
	}

	// delete migration on cluster 2
	err = deleteAndWaitForMigrationDeletion("mysql-migration", "mysql-enc-pvc-mysql-migration-failover-failback")
	require.NoError(t, err, "error deleting migration on destination cluster")

	// ensure app starts on cluster 1
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error resetting remote config")

	// Set scale factor to it's orignal values on cluster 1
	err = schedulerDriver.ScaleApplication(postMigrationCtx, scaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	err = schedulerDriver.WaitForRunning(postMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state on source cluster after failback")

	err = deleteAndWaitForMigrationDeletion("mysql-migration", "mysql-enc-pvc-mysql-migration-failover-failback")
	require.NoError(t, err, "error deleting migration on source cluster")

	if iteration == failOverFailBackScaleCount {
		destroyAndWait(t, []*scheduler.Context{preMigrationCtx})
	}
}

func deleteAndWaitForMigrationDeletion(name, namespace string) error {
	err := storkops.Instance().DeleteMigration(name, namespace)
	if err != nil {
		return fmt.Errorf("error deleting migration on destination cluster post reverse migration")
	}

	listMigration := func() (interface{}, bool, error) {
		mig, err := storkops.Instance().ListMigrations(namespace)
		if err != nil || len(mig.Items) != 0 {
			logrus.Infof("Failed to delete all migrations in %s. Error: %v. Number of migrations: %v", namespace, err, len(mig.Items))
			return "", true, fmt.Errorf("All migrations not deleted yet")
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(listMigration, migrationRetryTimeout, migrationRetryInterval)
	return err

}
