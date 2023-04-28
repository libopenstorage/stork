//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	rancherLabelKey          = "field.cattle.io/projectId"
	projectIDMappings        = "project-A=project-B,project-C=project-D"
	projectIDMappingsReverse = "project-B=project-A,project-D=project-C"
)

func testMigrationFailoverFailback(t *testing.T) {
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
	if !errors.IsAlreadyExists(err) {
		require.NoError(t, err, "failed to create secret for volumes")
	}

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	_, err = core.Instance().CreateSecret(secret)
	if !errors.IsAlreadyExists(err) {
		require.NoError(t, err, "failed to create secret for volumes")
	}

	t.Run("vanillaFailoverAndFailbackMigrationTest", vanillaFailoverAndFailbackMigrationTest)
	t.Run("rancherFailoverAndFailbackMigrationTest", rancherFailoverAndFailbackMigrationTest)
	t.Run("stickyFlagFailoverAndFailbackMigrationTest", stickyFlagFailoverAndFailbackMigrationTest)
}

func vanillaFailoverAndFailbackMigrationTest(t *testing.T) {
	failoverAndFailbackMigrationTest(t, "mysql-enc-pvc", "mysql-migration-failover-failback", true)
}

func rancherFailoverAndFailbackMigrationTest(t *testing.T) {
	// Migrate the resources
	instanceID := "mysql-migration-failover-failback-rancher"
	appKey := "mysql-enc-pvc-rancher"
	ctxs, preMigrationCtx := triggerMigration(
		t,
		instanceID,
		appKey,
		nil,
		[]string{instanceID},
		true,
		false,
		false,
		false,
		projectIDMappings,
		map[string]string{
			rancherLabelKey: "project-A",
		},
	)

	// validate the following
	// - migration is successful
	// - app starts on cluster 1
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, true, true)

	var migrationObj *v1alpha1.Migration
	var ok bool
	for _, specObj := range ctxs[0].App.SpecList {
		if migrationObj, ok = specObj.(*v1alpha1.Migration); ok {
			break
		}
	}

	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// 1 sts, 1 service, 1 pvc, 1 pv
	expectedResources := uint64(4)
	// 1 volume
	expectedVolumes := uint64(1)
	// validate the migration summary based on the application specs that were deployed by the test
	validateMigrationSummary(t, preMigrationCtx, expectedResources, expectedVolumes, migrationObj.Name, migrationObj.Namespace)

	scaleFactor := testMigrationFailover(t, preMigrationCtx, ctxs, "", appKey, instanceID)

	testMigrationFailback(t, preMigrationCtx, ctxs, scaleFactor, projectIDMappingsReverse, appKey, instanceID, true)
}

func stickyFlagFailoverAndFailbackMigrationTest(t *testing.T) {
	failoverAndFailbackMigrationTest(t, "mysql-sticky", "mysql-migration-sticky", false)
}

func failoverAndFailbackMigrationTest(t *testing.T, appKey, migrationKey string, failbackSuccessExpected bool) {

	// Migrate the resources
	ctxs, preMigrationCtx := triggerMigration(
		t,
		migrationKey,
		appKey,
		nil,
		[]string{migrationKey},
		true,
		false,
		false,
		false,
		"",
		nil,
	)

	// validate the following
	// - migration is successful
	// - app starts on cluster 1
	validateAndDestroyMigration(t, ctxs, preMigrationCtx, true, false, true, true, true)

	var migrationObj *v1alpha1.Migration
	var ok bool
	for _, specObj := range ctxs[0].App.SpecList {
		if migrationObj, ok = specObj.(*v1alpha1.Migration); ok {
			break
		}
	}

	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	// 1 sts, 1 service, 1 pvc, 1 pv
	expectedResources := uint64(4)
	// 1 volume
	expectedVolumes := uint64(1)
	// validate the migration summary based on the application specs that were deployed by the test
	validateMigrationSummary(t, preMigrationCtx, expectedResources, expectedVolumes, migrationObj.Name, migrationObj.Namespace)

	scaleFactor := testMigrationFailover(t, preMigrationCtx, ctxs, "", appKey, migrationKey)

	testMigrationFailback(t, preMigrationCtx, ctxs, scaleFactor, "", appKey, migrationKey, failbackSuccessExpected)
}

func testMigrationFailover(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	ctxs []*scheduler.Context,
	projectIDMappings string,
	appKey, instanceID string,
) map[string]int32 {
	// Failover the application

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
	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting remote config")

	// Set scale factor to it's original values on cluster 2
	err = schedulerDriver.ScaleApplication(preMigrationCtx, oldScaleFactor)
	require.NoError(t, err, "Unexpected error on ScaleApplication")

	err = schedulerDriver.WaitForRunning(preMigrationCtx, defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for pod to get to running state on remote cluster after migration")

	if len(projectIDMappings) > 0 {
		namespace := appKey + "-" + instanceID
		ns, err := core.Instance().GetNamespace(namespace)
		require.NoError(t, err, "failed to get namespace")
		projectValue, ok := ns.Labels[rancherLabelKey]
		require.True(t, ok, "expected rancher label")
		require.Equal(t, projectValue, "project-B")

		serviceList, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
		require.NoError(t, err, "failed to get services")
		require.GreaterOrEqual(t, len(serviceList.Items), 1, "unexpected number of services")
		for _, service := range serviceList.Items {
			projectValue, ok := service.Labels[rancherLabelKey]
			require.True(t, ok, "expected rancher label")
			require.Equal(t, projectValue, "project-B")

			projectValue, ok = service.Annotations[rancherLabelKey]
			require.True(t, ok, "expected rancher label")
			require.Equal(t, projectValue, "project-B")
		}
	}
	return oldScaleFactor
}

func testMigrationFailback(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
	ctxs []*scheduler.Context,
	scaleFactor map[string]int32,
	projectIDMappings string,
	appKey, instanceID string,
	failbackSuccessExpected bool,
) {
	// Failback the application
	// Trigger a reverse migration

	ctxsReverse, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{AppKeys: []string{appKey}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxsReverse), "Only one task should have started")

	appCtx := ctxsReverse[0]

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	postMigrationCtx := ctxsReverse[0].DeepCopy()

	// create, apply and validate cluster pair specs
	err = scheduleClusterPair(ctxsReverse[0], false, false, "cluster-pair-reverse", projectIDMappings, true)
	require.NoError(t, err, "Error scheduling cluster pair")

	// apply migration specs
	err = schedulerDriver.AddTasks(ctxsReverse[0],
		scheduler.ScheduleOptions{AppKeys: []string{instanceID}})
	require.NoError(t, err, "Error scheduling migration specs")

	if !failbackSuccessExpected {
		// In case of sticky volumes migration failure is expected, so will update volume and trigger migration again
		err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout/10, defaultWaitInterval)
		require.Error(t, err, "Expected failback migration to fail")

		// Get volumes for this migration on source cluster and update sticky flag
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting source config, for updating sticky volume")

		vols, err := schedulerDriver.GetVolumes(appCtx)
		require.NoError(t, err, "Error getting volumes for app")
		for _, v := range vols {
			err = volumeDriver.UpdateStickyFlag(v.ID, "off")
			require.NoError(t, err, "Error updating sticky flag for volumes %s", v.Name)
		}
		time.Sleep(3 * time.Minute)

		// Trigger migration on destination cluster again
		err = setDestinationKubeConfig()
		require.NoError(t, err, "Error setting destination config after updating sticky volume")

		var failedMigrationObj *v1alpha1.Migration
		var ok bool
		for _, specObj := range ctxsReverse[0].App.SpecList {
			if failedMigrationObj, ok = specObj.(*v1alpha1.Migration); ok {
				break
			}
		}
		failedMigrationObj, err = storkops.Instance().GetMigration(failedMigrationObj.Name, failedMigrationObj.Namespace)
		require.NoError(t, err, "Error getting the failed migration")
		logrus.Infof("Failed migration object found: %s in namespace: %s. Status: %s", failedMigrationObj.Name, failedMigrationObj.Namespace, failedMigrationObj.Status.Status)

		err = deleteMigrations([]*v1alpha1.Migration{failedMigrationObj})
		require.NoError(t, err, "error in deleting failed migrations.")

		// apply migration specs again, it should pass this time
		err = schedulerDriver.AddTasks(ctxsReverse[0],
			scheduler.ScheduleOptions{AppKeys: []string{instanceID}})
		require.NoError(t, err, "Error scheduling migration specs")

		err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for migration to complete post sticky flag update")
	}

	err = schedulerDriver.WaitForRunning(ctxsReverse[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for migration to complete")

	var migrationObj *v1alpha1.Migration
	var ok bool
	for _, specObj := range ctxsReverse[0].App.SpecList {
		if migrationObj, ok = specObj.(*v1alpha1.Migration); ok {
			break
		}
	}

	// 1 sts, 1 service, 1 pvc, 1 pv
	expectedResources := uint64(4)
	// 1 volume
	expectedVolumes := uint64(1)
	// validate the migration summary
	validateMigrationSummary(t, postMigrationCtx, expectedResources, expectedVolumes, migrationObj.Name, migrationObj.Namespace)

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

	// Check the namespace labels are transformed
	if len(projectIDMappings) > 0 {
		namespace := appKey + "-" + instanceID
		ns, err := core.Instance().GetNamespace(namespace)
		require.NoError(t, err, "failed to get namespace")
		projectValue, ok := ns.Labels[rancherLabelKey]
		require.True(t, ok, "expected rancher label")
		require.Equal(t, projectValue, "project-A")

		serviceList, err := core.Instance().ListServices(namespace, meta_v1.ListOptions{})
		require.NoError(t, err, "failed to get services")
		require.GreaterOrEqual(t, len(serviceList.Items), 1, "unexpected number of services")
		for _, service := range serviceList.Items {
			projectValue, ok := service.Labels[rancherLabelKey]
			require.True(t, ok, "expected rancher label")
			require.Equal(t, projectValue, "project-A")

			projectValue, ok = service.Annotations[rancherLabelKey]
			require.True(t, ok, "expected rancher label")
			require.Equal(t, projectValue, "project-A")
		}
	}
	destroyAndWait(t, []*scheduler.Context{postMigrationCtx})
	destroyAndWait(t, ctxs)

	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error resetting remote config")
	destroyAndWait(t, ctxsReverse)

	err = setSourceKubeConfig()
	require.NoError(t, err, "Error resetting remote config")

}

// The below two functions are currently not invoked during the tests since the namespaceSelector
// is still Alpha in kubernetes v1.21.0 . We can add these extra checks once we move our integration tests
// to k8s v1.24.0 where this field is GA

/*
func validateFailbackAffinityNamespaceSelector(
	t *testing.T,
	postMigrationCtx *scheduler.Context,
) {
	found := false
	for _, specObj := range postMigrationCtx.App.SpecList {
		if statefulSetSpec, ok := specObj.(*appsapi.StatefulSet); ok {
			found = true
			sts, err := apps.Instance().GetStatefulSet(statefulSetSpec.Name, statefulSetSpec.Namespace)
			require.NoError(t, err, "failed to get stateful set on remote cluster")
			require.NotNil(t, sts.Spec.Template.Spec.Affinity, "affinity is nil")

			// Pod Affinity
			affinity := sts.Spec.Template.Spec.Affinity.PodAffinity
			require.NotNil(t, affinity, "pod affinity is nil")

			// RequiredDuringSchedulingIgnoredDuringExecution
			require.NotEmpty(t, affinity.RequiredDuringSchedulingIgnoredDuringExecution, "affinity is empty")
			selector := affinity.RequiredDuringSchedulingIgnoredDuringExecution[0].NamespaceSelector
			require.NotNil(t, selector, "namespace selector is nil")
			require.Equal(t, len(selector.MatchLabels), 1, "incorrect match labels")
			projectValue, ok := selector.MatchLabels[rancherLabelKey]
			require.True(t, ok, "missing label key in namespace selector")
			require.Equal(t, projectValue, "projectA", "incorrect namespace selector value")

			// PreferredDuringSchedulingIgnoredDuringExecution
			require.NotEmpty(t, affinity.PreferredDuringSchedulingIgnoredDuringExecution, "affinity is empty")
			selector = affinity.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm.NamespaceSelector
			require.NotNil(t, selector, "namespace selector is nil")
			require.Equal(t, len(selector.MatchLabels), 1, "incorrect match labels")
			projectValue, ok = selector.MatchLabels[rancherLabelKey]
			require.True(t, ok, "missing label key in namespace selector")
			require.Equal(t, projectValue, "projectA", "incorrect namespace selector value")

			// Pod Anti Affinity
			antiAffinity := sts.Spec.Template.Spec.Affinity.PodAntiAffinity
			require.NotNil(t, antiAffinity, "pod antiAffinity is nil")

			// RequiredDuringSchedulingIgnoredDuringExecution
			require.NotEmpty(t, antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, "affinity is empty")
			selector = antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].NamespaceSelector
			require.NotNil(t, selector, "namespace selector is nil")
			require.Equal(t, len(selector.MatchLabels), 1, "incorrect match labels")
			projectValue, ok = selector.MatchLabels[rancherLabelKey]
			require.True(t, ok, "missing label key in namespace selector")
			require.Equal(t, projectValue, "projectC", "incorrect namespace selector value")

			// PreferredDuringSchedulingIgnoredDuringExecution
			require.NotEmpty(t, antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, "affinity is empty")
			selector = antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm.NamespaceSelector
			require.NotNil(t, selector, "namespace selector is nil")
			require.Equal(t, len(selector.MatchLabels), 1, "incorrect match labels")
			projectValue, ok = selector.MatchLabels[rancherLabelKey]
			require.True(t, ok, "missing label key in namespace selector")
			require.Equal(t, projectValue, "projectC", "incorrect namespace selector value")
		}
	}
	require.True(t, found, "Expected StatefulSet to be found on remote cluster")
}

func validateFailoverAffinityNamespaceSelector(
	t *testing.T,
	preMigrationCtx *scheduler.Context,
) {
	found := false
	for _, specObj := range preMigrationCtx.App.SpecList {
		if statefulSetSpec, ok := specObj.(*appsapi.StatefulSet); ok {
			found = true
			sts, err := apps.Instance().GetStatefulSet(statefulSetSpec.Name, statefulSetSpec.Namespace)
			require.NoError(t, err, "failed to get stateful set on remote cluster")
			require.NotNil(t, sts.Spec.Template.Spec.Affinity, "affinity is nil")

			// Pod Affinity
			affinity := sts.Spec.Template.Spec.Affinity.PodAffinity
			require.NotNil(t, affinity, "pod affinity is nil")

			// RequiredDuringSchedulingIgnoredDuringExecution
			require.NotEmpty(t, affinity.RequiredDuringSchedulingIgnoredDuringExecution, "affinity is empty")
			selector := affinity.RequiredDuringSchedulingIgnoredDuringExecution[0].NamespaceSelector
			require.NotNil(t, selector, "namespace selector is nil")
			require.Equal(t, len(selector.MatchLabels), 1, "incorrect match labels")
			projectValue, ok := selector.MatchLabels[rancherLabelKey]
			require.True(t, ok, "missing label key in namespace selector")
			require.Equal(t, projectValue, "projectB", "incorrect namespace selector value")

			// PreferredDuringSchedulingIgnoredDuringExecution
			require.NotEmpty(t, affinity.PreferredDuringSchedulingIgnoredDuringExecution, "affinity is empty")
			selector = affinity.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm.NamespaceSelector
			require.NotNil(t, selector, "namespace selector is nil")
			require.Equal(t, len(selector.MatchLabels), 1, "incorrect match labels")
			projectValue, ok = selector.MatchLabels[rancherLabelKey]
			require.True(t, ok, "missing label key in namespace selector")
			require.Equal(t, projectValue, "projectB", "incorrect namespace selector value")

			// Pod Anti Affinity
			antiAffinity := sts.Spec.Template.Spec.Affinity.PodAntiAffinity
			require.NotNil(t, antiAffinity, "pod antiAffinity is nil")

			// RequiredDuringSchedulingIgnoredDuringExecution
			require.NotEmpty(t, antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, "affinity is empty")
			selector = antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].NamespaceSelector
			require.NotNil(t, selector, "namespace selector is nil")
			require.Equal(t, len(selector.MatchLabels), 1, "incorrect match labels")
			projectValue, ok = selector.MatchLabels[rancherLabelKey]
			require.True(t, ok, "missing label key in namespace selector")
			require.Equal(t, projectValue, "projectD", "incorrect namespace selector value")

			// PreferredDuringSchedulingIgnoredDuringExecution
			require.NotEmpty(t, antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, "affinity is empty")
			selector = antiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm.NamespaceSelector
			require.NotNil(t, selector, "namespace selector is nil")
			require.Equal(t, len(selector.MatchLabels), 1, "incorrect match labels")
			projectValue, ok = selector.MatchLabels[rancherLabelKey]
			require.True(t, ok, "missing label key in namespace selector")
			require.Equal(t, projectValue, "projectD", "incorrect namespace selector value")
		}
	}
	require.True(t, found, "Expected StatefulSet to be found on remote cluster")
}
*/
