//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"testing"
	"time"

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
	// Create secrets on source and destination
	// Since the secrets need to be created on the destination before migration
	// is triggered using the API instead of spec factory in torpedo
	secret := createSecret()
	setSecret(t, secret, "src")
	setSecret(t, secret, "dest")

	t.Run("actionFailoverTest", actionFailoverTest)
}

func createSecret() *v1.Secret {
	return &v1.Secret{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      "volume-secrets",
			Namespace: "kube-system",
		},
		StringData: map[string]string{
			"mysql-secret": "supersecretpassphrase",
		},
	}
}

func setSecret(t *testing.T, secret *v1.Secret, cluster string) {
	if cluster == "dest" {
		logrus.Infof("creating secret in destination")
		err := setDestinationKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		defer func() {
			err := setSourceKubeConfig()
			require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
		}()
	} else {
		logrus.Infof("creating secret in source")
	}

	_, err := core.Instance().CreateSecret(secret)
	if !errors.IsAlreadyExists(err) {
		require.NoError(t, err, "failed to create secret for volumes")
	}
}

func deleteNamespace(t *testing.T, namespace string, cluster string) {
	if cluster == "dest" {
		logrus.Infof("deleting namespace %v in destination", namespace)
		err := setDestinationKubeConfig()
		require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

		defer func() {
			err := setSourceKubeConfig()
			require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
		}()
	} else {
		logrus.Infof("deleting namespace %v in source", namespace)
	}

	err := core.Instance().DeleteNamespace(namespace)
	if err != nil {
		logrus.Infof("Error deleting namespace: %v\n", err)
	}
	time.Sleep(10 * time.Second)
}

func actionFailoverTest(t *testing.T) {

	appKey := "mysql-enc-pvc"
	instanceID := "mysql-action"
	migrationAppId := "mysql-action-migration"
	actionName := "mysql-action-failover"

	namespace := fmt.Sprintf("%v-%v", appKey, instanceID)
	deleteNamespace(t, namespace, "src")
	deleteNamespace(t, namespace, "dest")

	// starts the app on src,
	// sets cluster pair,
	// creates a migration
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		nil,
		[]string{migrationAppId},
		true,
		true,
		false,
		false,
		"",
		nil,
	)

	// validate the following
	// - migration is successful
	// - app doesn't start on dest
	validateAndDestroyMigration(
		t,
		ctxs,
		preMigrationCtx,
		true,
		false,
		true,
		true,
		true,
	)

	// extract migrationObj from specList
	var migrationObj *v1alpha1.Migration
	var ok bool
	for _, specObj := range ctxs[0].App.SpecList {
		if migrationObj, ok = specObj.(*v1alpha1.Migration); ok {
			break
		}
	}

	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	expectedResources := uint64(4) // 1 sts, 1 service, 1 pvc, 1 pv
	expectedVolumes := uint64(0)   // 0 volume
	// validate the migration summary based on the application specs that were deployed by the test
	validateMigrationSummary(t, preMigrationCtx, expectedResources, expectedVolumes, migrationObj.Name, migrationObj.Namespace)

	scaleFactor := scaleDownApps(t, ctxs)
	logrus.Infof("scaleFactor: %v", scaleFactor)

	// TODO(dgoel): add a wrapper method that sets and unsets destKubeConfig
	// trigger action
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	createActionCR(t, actionName, namespace, ctxs[0])

	err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "error waiting for app to get to running state")

	// if above call to WaitForRunning is successful,
	// then Action validateActionCR should be successful too
	validateActionCR(t, actionName, namespace)

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
}

func createActionCR(t *testing.T, actionAppKey, namespace string, ctx *scheduler.Context) {
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
	_, err := storkops.Instance().CreateAction(&actionSpec)

	require.NoError(t, err, "error creating Action CR")
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
