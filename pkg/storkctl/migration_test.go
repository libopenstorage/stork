//go:build unittest
// +build unittest

package storkctl

import (
	"strings"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	ocpv1 "github.com/openshift/api/apps/v1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/openshift"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	appv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetMigrationsNoMigration(t *testing.T) {
	cmdArgs := []string{"get", "migrations"}

	var migrationList storkv1.MigrationList
	expected := "No resources found.\n"
	testCommon(t, cmdArgs, &migrationList, expected, false)
}

func createMigrationAndVerify(
	t *testing.T,
	name string,
	namespace string,
	clusterpair string,
	namespaces []string,
	preExecRule string,
	postExecRule string,
) {
	cmdArgs := []string{"create", "migrations", "-n", namespace, "-c", clusterpair, "--namespaces", strings.Join(namespaces, ","), name}
	if preExecRule != "" {
		cmdArgs = append(cmdArgs, "--preExecRule", preExecRule)
	}
	if postExecRule != "" {
		cmdArgs = append(cmdArgs, "--postExecRule", postExecRule)
	}

	expected := "Migration " + name + " created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	migration, err := storkops.Instance().GetMigration(name, namespace)
	require.NoError(t, err, "Error getting migration")
	require.Equal(t, name, migration.Name, "Migration name mismatch")
	require.Equal(t, namespace, migration.Namespace, "Migration namespace mismatch")
	require.Equal(t, clusterpair, migration.Spec.ClusterPair, "Migration clusterpair mismatch")
	require.Equal(t, namespaces, migration.Spec.Namespaces, "Migration namespace mismatch")
	require.Equal(t, preExecRule, migration.Spec.PreExecRule, "Migration preExecRule mismatch")
	require.Equal(t, postExecRule, migration.Spec.PostExecRule, "Migration postExecRule mismatch")
}

func TestGetMigrationsOneMigration(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationtest", "test", "clusterpair1", []string{"namespace1"}, "preExec", "postExec")

	expected := "NAME               CLUSTERPAIR    STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED   TOTAL BYTES TRANSFERRED\n" +
		"getmigrationtest   clusterpair1                    0/0       0/0                             Available with stork v2.9.0+\n"
	cmdArgs := []string{"get", "migrations", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationsMultiple(t *testing.T) {
	defer resetTest()
	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	require.NoError(t, err, "Error creating default namespace")

	createMigrationAndVerify(t, "getmigrationtest1", "default", "clusterpair1", []string{"namespace1"}, "", "")
	createMigrationAndVerify(t, "getmigrationtest2", "default", "clusterpair2", []string{"namespace1"}, "", "")

	expected := "NAME                CLUSTERPAIR    STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED   TOTAL BYTES TRANSFERRED\n" +
		"getmigrationtest1   clusterpair1                    0/0       0/0                             Available with stork v2.9.0+\n" +
		"getmigrationtest2   clusterpair2                    0/0       0/0                             Available with stork v2.9.0+\n"
	cmdArgs := []string{"get", "migrations", "getmigrationtest1", "getmigrationtest2"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all migrations if no name given
	cmdArgs = []string{"get", "migrations"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "NAME                CLUSTERPAIR    STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED   TOTAL BYTES TRANSFERRED\n" +
		"getmigrationtest1   clusterpair1                    0/0       0/0                             Available with stork v2.9.0+\n"
	// Should get only one migration if name given
	cmdArgs = []string{"get", "migrations", "getmigrationtest1"}
	testCommon(t, cmdArgs, nil, expected, false)

	_, err = core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}})
	require.NoError(t, err, "Error creating ns1 namespace")
	createMigrationAndVerify(t, "getmigrationtest21", "ns1", "clusterpair2", []string{"namespace1"}, "", "")
	cmdArgs = []string{"get", "migrations", "--all-namespaces"}
	expected = "NAMESPACE   NAME                 CLUSTERPAIR    STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED" +
		"   TOTAL BYTES TRANSFERRED\ndefault     getmigrationtest1    clusterpair1                    0/0       0/0" +
		"                             Available with stork v2.9.0+\ndefault     getmigrationtest2    clusterpair2  " +
		"                  0/0       0/0                             Available with stork v2.9.0+\nns1         " +
		"getmigrationtest21   clusterpair2                    0/0       0/0                             Available with stork v2.9.0+\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationsWithClusterPair(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationtest1", "default", "clusterpair1", []string{"namespace1"}, "", "")
	createMigrationAndVerify(t, "getmigrationtest2", "default", "clusterpair2", []string{"namespace1"}, "", "")

	expected := "NAME                CLUSTERPAIR    STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED   " +
		"TOTAL BYTES TRANSFERRED\ngetmigrationtest1   clusterpair1                    0/0       0/0                             Available with stork v2.9.0+\n"
	cmdArgs := []string{"get", "migrations", "-c", "clusterpair1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationsWithStatusAndProgress(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationstatustest", "default", "clusterpair1", []string{"namespace1"}, "", "")
	migration, err := storkops.Instance().GetMigration("getmigrationstatustest", "default")
	require.NoError(t, err, "Error getting migration")

	// Update the status of the migration
	migration.Status.FinishTimestamp = metav1.Now()
	migration.CreationTimestamp = metav1.NewTime(migration.Status.FinishTimestamp.Add(-5 * time.Minute))
	migration.Status.Stage = storkv1.MigrationStageFinal
	migration.Status.Status = storkv1.MigrationStatusSuccessful
	migration.Status.Volumes = []*storkv1.MigrationVolumeInfo{}
	_, err = storkops.Instance().UpdateMigration(migration)
	require.NoError(t, err, "Error updating migration")

	expected := "NAME                     CLUSTERPAIR    STAGE   STATUS       VOLUMES   RESOURCES   CREATED               ELAPSED   TOTAL BYTES TRANSFERRED\n" +
		"getmigrationstatustest   clusterpair1   Final   Successful   0/0       0/0         " + toTimeString(migration.CreationTimestamp.Time) + "   5m0s      Available with stork v2.9.0+\n"
	cmdArgs := []string{"get", "migrations", "getmigrationstatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationsWithSummary(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationstatustest", "default", "clusterpair1", []string{"namespace1"}, "", "")
	migration, err := storkops.Instance().GetMigration("getmigrationstatustest", "default")
	require.NoError(t, err, "Error getting migration")

	// Update the status of the migration
	migration.Status.FinishTimestamp = metav1.Now()
	migration.CreationTimestamp = metav1.NewTime(migration.Status.FinishTimestamp.Add(-5 * time.Minute))
	migration.Status.Stage = storkv1.MigrationStageFinal
	migration.Status.Status = storkv1.MigrationStatusSuccessful
	migration.Status.Summary = &storkv1.MigrationSummary{
		TotalBytesMigrated:        uint64(12345),
		TotalNumberOfVolumes:      uint64(5),
		TotalNumberOfResources:    uint64(6),
		NumberOfMigratedVolumes:   uint64(5),
		NumberOfMigratedResources: uint64(6),
		ElapsedTime:               "5m0s",
	}
	_, err = storkops.Instance().UpdateMigration(migration)
	require.NoError(t, err, "Error updating migration")

	expected := "NAME                     CLUSTERPAIR    STAGE   STATUS       VOLUMES   RESOURCES   CREATED               ELAPSED   TOTAL BYTES TRANSFERRED\n" +
		"getmigrationstatustest   clusterpair1   Final   Successful   5/5       6/6         " + toTimeString(migration.CreationTimestamp.Time) + "   5m0s      12345\n"
	cmdArgs := []string{"get", "migrations", "getmigrationstatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateMigrationsNoNamespace(t *testing.T) {
	cmdArgs := []string{"create", "migrations", "-c", "clusterPair1", "migration1"}

	expected := "error: need to provide atleast one namespace to migrate"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationsNoClusterPair(t *testing.T) {
	cmdArgs := []string{"create", "migrations", "migration1"}

	expected := "error: ClusterPair name needs to be provided for migration"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationsNoName(t *testing.T) {
	cmdArgs := []string{"create", "migrations"}

	expected := "error: exactly one name needs to be provided for migration name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrations(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "createmigration", "default", "clusterpair1", []string{"namespace1"}, "", "")
}

func TestCreateDuplicateMigrations(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "createmigration", "default", "clusterpair1", []string{"namespace1"}, "", "")
	cmdArgs := []string{"create", "migrations", "-c", "clusterpair1", "--namespaces", "namespace1", "createmigration"}

	expected := "Error from server (AlreadyExists): migrations.stork.libopenstorage.org \"createmigration\" already exists"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteMigrationsNoMigrationName(t *testing.T) {
	cmdArgs := []string{"delete", "migrations"}

	var migrationList storkv1.MigrationList
	expected := "error: at least one argument needs to be provided for migration name"
	testCommon(t, cmdArgs, &migrationList, expected, true)
}

func TestDeleteMigrationsNoMigration(t *testing.T) {
	cmdArgs := []string{"delete", "migrations", "-c", "migration1"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteMigrations(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "deletemigration", "default", "clusterpair1", []string{"namespace1"}, "", "")

	cmdArgs := []string{"delete", "migrations", "deletemigration"}
	expected := "Migration deletemigration deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"delete", "migrations", "deletemigration"}
	expected = "Error from server (NotFound): migrations.stork.libopenstorage.org \"deletemigration\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	createMigrationAndVerify(t, "deletemigration1", "default", "clusterpair1", []string{"namespace1"}, "", "")
	createMigrationAndVerify(t, "deletemigration2", "default", "clusterpair2", []string{"namespace1"}, "", "")

	cmdArgs = []string{"delete", "migrations", "deletemigration1", "deletemigration2"}
	expected = "Migration deletemigration1 deleted successfully\n"
	expected += "Migration deletemigration2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	createMigrationAndVerify(t, "deletemigration1", "default", "clusterpair1", []string{"namespace1"}, "", "")
	createMigrationAndVerify(t, "deletemigration2", "default", "clusterpair1", []string{"namespace1"}, "", "")

	cmdArgs = []string{"delete", "migrations", "-c", "clusterpair1"}
	expected = "Migration deletemigration1 deleted successfully\n"
	expected += "Migration deletemigration2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestExclueVolumesForMigrations(t *testing.T) {
	defer resetTest()
	name := "excludevolumestest"
	namespace := "test"
	createMigrationAndVerify(t, name, namespace, "clusterpair1", []string{"namespace1"}, "", "")
	migration, err := storkops.Instance().GetMigration(name, namespace)
	require.NoError(t, err, "Error getting migration")

	include := false
	migration.Spec.IncludeVolumes = &include
	_, err = storkops.Instance().UpdateMigration(migration)
	require.NoError(t, err, "Error updating migration")

	expected := "NAME                 CLUSTERPAIR    STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED" +
		"   TOTAL BYTES TRANSFERRED\nexcludevolumestest   clusterpair1                    N/A       0/0                             Available with stork v2.9.0+\n"

	cmdArgs := []string{"get", "migrations", "-n", namespace}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestExclueResourcesForMigrations(t *testing.T) {
	defer resetTest()
	name := "excluderesourcstest"
	namespace := "test"
	createMigrationAndVerify(t, name, namespace, "clusterpair1", []string{"namespace1"}, "", "")
	migration, err := storkops.Instance().GetMigration(name, namespace)
	require.NoError(t, err, "Error getting migration")

	include := false
	migration.Spec.IncludeResources = &include
	_, err = storkops.Instance().UpdateMigration(migration)
	require.NoError(t, err, "Error updating migration")

	expected := "NAME                  CLUSTERPAIR    STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED" +
		"   TOTAL BYTES TRANSFERRED\nexcluderesourcstest   clusterpair1                    0/0       N/A                             Available with stork v2.9.0+\n"
	cmdArgs := []string{"get", "migrations", "-n", namespace}
	testCommon(t, cmdArgs, nil, expected, false)
}

func createMigratedDeployment(t *testing.T) {
	replicas := int32(0)
	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "dep"}})
	require.NoError(t, err, "Error creating dep namespace")

	deployment := &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "migratedDeployment",
			Namespace: "dep",
			Annotations: map[string]string{
				migration.StorkMigrationReplicasAnnotation: "1",
			},
		},
		Spec: appv1.DeploymentSpec{
			Replicas: &replicas,
		},
	}
	_, err = apps.Instance().CreateDeployment(deployment, metav1.CreateOptions{})
	require.NoError(t, err, "Error creating deployment")
}

func createMigratedStatefulSet(t *testing.T) {
	replicas := int32(0)
	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sts"}})
	require.NoError(t, err, "Error creating sts namespace")

	statefulSet := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "migratedStatefulSet",
			Namespace: "sts",
			Annotations: map[string]string{
				migration.StorkMigrationReplicasAnnotation: "3",
			},
		},
		Spec: appv1.StatefulSetSpec{
			Replicas: &replicas,
		},
	}
	_, err = apps.Instance().CreateStatefulSet(statefulSet, metav1.CreateOptions{})
	require.NoError(t, err, "Error creating statefulset")

}

func createMigratedDeploymentConfig(t *testing.T) {
	replicas := int32(0)
	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "depconf"}})
	require.NoError(t, err, "Error creating dep namespace")

	deploymentConfig := &ocpv1.DeploymentConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "migratedDeploymentConfig",
			Namespace: "depconf",
			Annotations: map[string]string{
				migration.StorkMigrationReplicasAnnotation: "1",
			},
		},
		Spec: ocpv1.DeploymentConfigSpec{
			Replicas: replicas,
		},
	}
	_, err = openshift.Instance().CreateDeploymentConfig(deploymentConfig)
	require.NoError(t, err, "Error creating deploymentconfig")
}

func TestActivateDeactivateMigrations(t *testing.T) {
	createMigratedDeployment(t)
	createMigratedStatefulSet(t)
	createMigratedDeploymentConfig(t)

	cmdArgs := []string{"activate", "migrations", "-n", "dep"}
	expected := "Updated replicas for deployment dep/migratedDeployment to 1\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"activate", "migrations", "-n", "depconf"}
	expected = "Updated replicas for deploymentconfig depconf/migratedDeploymentConfig to 1\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"activate", "migrations", "-n", "sts"}
	expected = "Updated replicas for statefulset sts/migratedStatefulSet to 3\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"deactivate", "migrations", "-n", "dep"}
	expected = "Updated replicas for deployment dep/migratedDeployment to 0\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"deactivate", "migrations", "-n", "depconf"}
	expected = "Updated replicas for deploymentconfig depconf/migratedDeploymentConfig to 0\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"deactivate", "migrations", "-n", "sts"}
	expected = "Updated replicas for statefulset sts/migratedStatefulSet to 0\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"activate", "migrations", "-a"}
	expected = "Updated replicas for deployment dep/migratedDeployment to 1\n"
	expected += "Updated replicas for deploymentconfig depconf/migratedDeploymentConfig to 1\n"
	expected += "Updated replicas for statefulset sts/migratedStatefulSet to 3\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"deactivate", "migrations", "-a"}
	expected = "Updated replicas for deployment dep/migratedDeployment to 0\n"
	expected += "Updated replicas for deploymentconfig depconf/migratedDeploymentConfig to 0\n"
	expected += "Updated replicas for statefulset sts/migratedStatefulSet to 0\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateMigrationWaitSuccess(t *testing.T) {
	migrRetryTimeout = 10 * time.Second
	defer resetTest()

	namespace := "dummy-namespace"
	name := "dummy-name"
	clusterpair := "dummy-clusterpair"
	cmdArgs := []string{"create", "migrations", "-n", namespace, "-c", clusterpair, "--namespaces", namespace, name, "--wait"}

	expected := "STAGE\t\tSTATUS              \n\t\t                    \nVolumes\t\tSuccessful          \nMigration dummy-name completed successfully\n"
	go setMigrationStatus(name, namespace, false, t)
	testCommon(t, cmdArgs, nil, expected, false)
}
func TestCreateMigrationWaitFailed(t *testing.T) {
	migrRetryTimeout = 10 * time.Second
	defer resetTest()

	namespace := "dummy-namespace"
	name := "dummy-name"
	clusterpair := "dummy-clusterpair"
	cmdArgs := []string{"create", "migrations", "-n", namespace, "-c", clusterpair, "--namespaces", namespace, name, "--wait"}

	expected := "STAGE\t\tSTATUS              \n\t\t                    \nVolumes\t\tFailed              \nMigration dummy-name failed\n"
	go setMigrationStatus(name, namespace, true, t)
	testCommon(t, cmdArgs, nil, expected, false)
}

func setMigrationStatus(name, namespace string, isFail bool, t *testing.T) {
	time.Sleep(10 * time.Second)
	migrResp, err := storkops.Instance().GetMigration(name, namespace)
	require.NoError(t, err, "Error getting Migration details")
	require.Equal(t, migrResp.Status.Status, storkv1.MigrationStatusInitial)
	require.Equal(t, migrResp.Status.Stage, storkv1.MigrationStageInitial)
	migrResp.Status.Status = storkv1.MigrationStatusSuccessful
	migrResp.Status.Stage = storkv1.MigrationStageVolumes
	if isFail {
		migrResp.Status.Status = storkv1.MigrationStatusFailed
	}

	_, err = storkops.Instance().UpdateMigration(migrResp)
	require.NoError(t, err, "Error updating Migrations")
}
