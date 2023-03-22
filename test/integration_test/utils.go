//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"

	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createSecret(t *testing.T, secret_name string, secret_map map[string]string) *v1.Secret {
	secret := &v1.Secret{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      secret_name,
			Namespace: "kube-system",
		},
		StringData: secret_map,
	}
	secretObj, err := core.Instance().CreateSecret(secret)
	if !errors.IsAlreadyExists(err) {
		require.NoError(t, err, "failed to create secret for volumes")
	}
	return secretObj
}

func cleanup(t *testing.T, namespace string) {
	funcDeleteNamespace := func() {
		err := core.Instance().DeleteNamespace(namespace)
		if err != nil {
			logrus.Infof("Error deleting namespace %s: %v\n", namespace, err)
		}
	}
	funcDeleteNamespace()
	executeOnDestination(t, funcDeleteNamespace)
}

func executeOnDestination(t *testing.T, funcToExecute func()) {
	err := setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)
	logrus.Info("KubeConfig set to Destination")

	defer func() {
		err := setSourceKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
		logrus.Info("KubeConfig set to Source")
	}()

	funcToExecute()
}

func scheduleAppAndWait(t *testing.T, instanceID, appKey string) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule(
		instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{appKey},
		})
	require.NoError(t, err, "Error scheduling app")
	require.Equal(t, 1, len(ctxs), "Only one app should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	return ctxs
}

func scheduleAppAndWaitMultiple(t *testing.T, instanceIDList []string, appKey string) []*scheduler.Context {
	var ctxs []*scheduler.Context

	// creates the namespace (appKey-instanceID) and schedules the app (appKey)
	for _, instanceID := range instanceIDList {
		newCtxs, err := schedulerDriver.Schedule(
			instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
				Labels:  nil,
			})
		require.NoError(t, err, "Error scheduling task")
		require.Equal(t, 1, len(newCtxs), "Only one task should have started")
		ctxs = append(ctxs, newCtxs[0])
	}

	// wait for all apps to get to running state
	for _, ctx := range ctxs {
		err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app to get to running state")
	}
	return ctxs
}

func scheduleTaskAndWait(t *testing.T, ctx *scheduler.Context, appKey string) {
	err := schedulerDriver.AddTasks(
		ctx,
		scheduler.ScheduleOptions{
			AppKeys: []string{appKey},
		})
	require.NoError(t, err, "Error scheduling app")

	err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")
}

func triggerMigrationMultiple(
	t *testing.T,
	ctxs []*scheduler.Context,
	migrationAppKeys []string,
	namespaceList []string,
	includeResources bool,
	includeVolumes bool,
	startApplications bool,
) ([]*scheduler.Context, []*scheduler.Context, []*v1alpha1.Migration) {
	var preMigrationCtxs []*scheduler.Context
	var migrationList []*v1alpha1.Migration

	for idx, ctx := range ctxs {
		preMigrationCtxs = append(preMigrationCtxs, ctx.DeepCopy())

		// create, apply and validate cluster pair specs
		err := scheduleClusterPair(
			ctx, true, true, defaultClusterPairDir, "", false)
		require.NoError(t, err, "Error scheduling cluster pair")

		// apply migration specs
		migration, err := createMigration(
			t, migrationAppKeys[0], namespaceList[idx], "remoteclusterpair", namespaceList[idx], &includeResources, &includeVolumes, &startApplications)
		require.NoError(t, err, "Error scheduling migration")
		migrationList = append(migrationList, migration)

		// err = schedulerDriver.AddTasks(
		// 	ctx, scheduler.ScheduleOptions{AppKeys: migrationAppKeys})
		// require.NoError(t, err, "Error scheduling migration specs")
	}
	return preMigrationCtxs, ctxs, migrationList
}

func createActionCR(t *testing.T, actionAppKey, namespace string, ctx *scheduler.Context) *v1alpha1.Action {
	actionSpec := v1alpha1.Action{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      actionAppKey,
			Namespace: namespace,
		},
		Spec: v1alpha1.ActionSpec{
			ActionType: v1alpha1.ActionTypeFailover,
		},
		Status: v1alpha1.ActionStatusScheduled,
	}
	action, err := storkops.Instance().CreateAction(&actionSpec)

	require.NoError(t, err, "error creating Action CR")
	return action
}

func validateActionCR(t *testing.T, actionName, namespace string) {
	action, err := storkops.Instance().GetAction(actionName, namespace)
	require.NoError(t, err, "error fetching Action CR")
	require.Equal(t, v1alpha1.ActionStatusSuccessful, action.Status)
}

func scaleDownApps(
	t *testing.T,
	ctxs []*scheduler.Context,
) map[string]int32 {
	scaleFactor, err := schedulerDriver.GetScaleFactorMap(ctxs[0])
	require.NoError(t, err, "unexpected error on GetScaleFactorMap")

	newScaleFactor := make(map[string]int32) // scale down
	for k := range scaleFactor {
		newScaleFactor[k] = 0
	}

	err = schedulerDriver.ScaleApplication(ctxs[0], newScaleFactor)
	require.NoError(t, err, "unexpected error on ScaleApplication")

	// check if the app is scaled down
	_, err = task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			newScaleFactor, err = schedulerDriver.GetScaleFactorMap(ctxs[0])
			if err != nil {
				return "", true, err
			}
			for k := range newScaleFactor {
				if int(newScaleFactor[k]) != 0 {
					return "", true, fmt.Errorf("expected scale to be 0")
				}
			}
			return "", false, nil
		},
		defaultWaitTimeout,
		defaultWaitInterval)
	require.NoError(t, err, "unexpected error on scaling down application.")

	return scaleFactor
}

func validateMigrationOnSrcAndDest(
	t *testing.T,
	migrationName string,
	namespace string,
	preMigrationCtx *scheduler.Context,
	startAppsOnMigration bool,
	expectedResources uint64,
	expectedVolumes uint64,
) {
	err := storkops.Instance().ValidateMigration(migrationName, namespace, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error validating migration")
	logrus.Infof("Validated migration: %v", migrationName)

	// TODO(dgoel): calls GetMigration again, remove redundant call
	validateMigrationSummary(
		t, preMigrationCtx, expectedResources, expectedVolumes, migrationName, namespace)

	funcValidateMigrationOnDestination := func() {
		if startAppsOnMigration {
			err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
			require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")
		} else {
			err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout/4, defaultWaitInterval)
			require.Error(t, err, "Expected pods to NOT get to running state on remote cluster after migration")
		}
	}
	executeOnDestination(t, funcValidateMigrationOnDestination)
}
