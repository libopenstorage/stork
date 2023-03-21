//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"testing"

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

func TestAction(t *testing.T) {

	setupOnce(t)

	t.Run("actionFailoverTest", testFailoverBasic)
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

// test basic workflow:
// 1. start an app on source
// 2. migrate k8s resources to destination
// 3. scale down app on source and do failover on dest
func testFailoverBasic(t *testing.T) {

	appKey := "mysql-enc-pvc"
	instanceID := "failover"
	migrationAppId := "failover-mysql-migration"
	actionName := "failover-action"

	namespace := fmt.Sprintf("%v-%v", appKey, instanceID)

	cleanup(t, namespace)

	// starts the app on src,
	// sets cluster pair,
	// creates a migration
	ctxs, preMigrationCtx := triggerMigration(
		t, instanceID, appKey, nil, []string{migrationAppId}, true, true, false, false, "", nil)

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

func scheduleAndWait(t *testing.T, instanceID, appKey string) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule(
		instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{appKey},
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	return ctxs
}

func scheduleAndWaitMultiple(t *testing.T, instanceIDList []string, appKey string) []*scheduler.Context {
	var ctxs []*scheduler.Context

	// schedule the app (appKey) in each namespace (instanceID)
	for _, instanceID := range instanceIDList {
		newCtxs, err := schedulerDriver.Schedule(
			instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
			})
		require.NoError(t, err, "Error scheduling task")
		require.Equal(t, 1, len(ctxs), "Only one task should have started")
		ctxs = append(ctxs, newCtxs[0])
	}

	// wait for all apps to get to running state
	for _, ctx := range ctxs {
		err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app to get to running state")
	}
	return ctxs
}

func triggerMigrationMultiple(
	t *testing.T, ctxs []*scheduler.Context,
	migrationAppKeys []string,
) ([]*scheduler.Context, []*scheduler.Context) {
	var preMigrationCtxs []*scheduler.Context

	for _, ctx := range ctxs {
		preMigrationCtxs = append(preMigrationCtxs, ctx.DeepCopy())

		// create, apply and validate cluster pair specs
		err := scheduleClusterPair(
			ctx, true, true, defaultClusterPairDir, "", false)
		require.NoError(t, err, "Error scheduling cluster pair")

		// apply migration specs
		err = schedulerDriver.AddTasks(
			ctx, scheduler.ScheduleOptions{AppKeys: migrationAppKeys})
		require.NoError(t, err, "Error scheduling migration specs")
	}
	return ctxs, preMigrationCtxs
}

func testFailoverWithoutMigration(t *testing.T) {
	appKey := "mysql-enc-pvc"
	instanceID := "failover"
	actionName := "failover-action"
	namespace := fmt.Sprintf("%v-%v", appKey, instanceID)

	defer cleanup(t, namespace)

	ctxs := scheduleAndWait(t, instanceID, appKey)

	startAndValidateFailover := func() {
		_ = createActionCR(t, actionName, namespace, ctxs[0])

		// check mysql app does NOT start on destination
		err := schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.Error(t, err, "error waiting for app to get to running state")

		// Action CR should fail with: No migration found
		// TODO(dgoel): discuss and handle this error case
		validateActionCR(t, actionName, namespace)
	}
	executeOnDestination(t, startAndValidateFailover)
}

// func testFailoverDependencyOnPromoteVolume(t *testing.T) {
// }

// func testFailoverForNamespaceKubeSystem(t *testing.T) {
// }

func testFailoverForMultipleNamespaces(t *testing.T) {
	appKey := "mysql-enc-pvc"
	instanceIDList := []string{"mysql-action- 1", "mysql-action-2"}
	migrationAppKey := "mysql-action-migration"
	actionName := "mysql-action-failover"
	var namespaceList []string

	for _, instanceID := range instanceIDList {
		namespaceList := append(namespaceList, fmt.Sprintf("%v-%v", appKey, instanceID))
		defer cleanup(t, namespaceList[len(namespaceList)-1])
	}

	// TODO(dgoel): change all AppID references to AppKey
	ctxs := scheduleAndWaitMultiple(t, instanceIDList, appKey)
	preMigrationCtxs, ctxs := triggerMigrationMultiple(t, ctxs, []string{migrationAppKey})

	// TODO(dgoel): break the validateAndDestroyMigration method into smaller methods
	// namely: scheduleTaskAndWait, deleteApplication
	for idx, ctx := range ctxs {
		preMigrationCtx := preMigrationCtxs[idx]

		// validate the following
		// - migration is successful
		// - app doesn't start on dest
		validateAndDestroyMigration(
			t, []*scheduler.Context{ctx}, preMigrationCtx, true, false, true, true, true)

		// extract migrationObj from specList
		// TODO(dgoel): extract method, getMigrationSpecFromContext
		var migrationObj *v1alpha1.Migration
		var ok bool
		for _, specObj := range ctx.App.SpecList {
			if migrationObj, ok = specObj.(*v1alpha1.Migration); ok {
				break
			}
		}

		expectedResources := uint64(4) // 1 sts, 1 service, 1 pvc, 1 pv
		expectedVolumes := uint64(0)   // 0 volume
		// validate the migration summary based on the application specs that were deployed by the test
		validateMigrationSummary(
			t, preMigrationCtx, expectedResources, expectedVolumes, migrationObj.Name, migrationObj.Namespace)
	}

	startAndValidateFailoverMultiple := func() {
		for idx, ctx := range ctxs {
			_ = createActionCR(t, actionName, namespaceList[idx], ctx)
		}

		for idx, ctx := range ctxs {
			// check mysql app does NOT start on destination
			err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
			require.Error(t, err, "error waiting for app to get to running state")

			validateActionCR(t, actionName, namespaceList[idx])
		}
	}
	executeOnDestination(t, startAndValidateFailoverMultiple)
}

// func testFailoverWithMultipleApplications(t *testing.T) {
// }
// func testFailoverOneActionPolicy(t *testing.T) {
// }
// func testFailoverWithFailedVolumePromote(t *testing.T) {
// }

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
