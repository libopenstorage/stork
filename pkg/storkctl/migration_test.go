// +build unittest

package storkctl

import (
	"strings"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetMigrationsNoMigration(t *testing.T) {
	cmdArgs := []string{"get", "migrations"}

	var migrationList storkv1.MigrationList
	expected := "No resources found.\n"
	testCommon(t, cmdArgs, &migrationList, expected, false)
}

func createMigrationAndVerify(t *testing.T, name string, namespace string, clusterpair string, namespaces []string) {
	cmdArgs := []string{"create", "migrations", "-n", namespace, "-c", clusterpair, "--namespaces", strings.Join(namespaces, ","), name}

	expected := "Migration " + name + " created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	migration, err := k8s.Instance().GetMigration(name, namespace)
	require.NoError(t, err, "Error getting migration")
	require.Equal(t, name, migration.Name, "Migration name mismatch")
	require.Equal(t, namespace, migration.Namespace, "Migration namespace mismatch")
	require.Equal(t, clusterpair, migration.Spec.ClusterPair, "Migration clusterpair mismatch")
	require.Equal(t, namespaces, migration.Spec.Namespaces, "Migration namespace mismatch")
}

func TestGetMigrationsOneMigration(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationtest", "test", "clusterpair1", []string{"namespace1"})

	expected := "NAME               CLUSTERPAIR    STAGE     STATUS    VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationtest   clusterpair1                       0/0       0/0         \n"

	cmdArgs := []string{"get", "migrations", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationsMultiple(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationtest1", "default", "clusterpair1", []string{"namespace1"})
	createMigrationAndVerify(t, "getmigrationtest2", "default", "clusterpair2", []string{"namespace1"})

	expected := "NAME                CLUSTERPAIR    STAGE     STATUS    VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationtest1   clusterpair1                       0/0       0/0         \n" +
		"getmigrationtest2   clusterpair2                       0/0       0/0         \n"

	cmdArgs := []string{"get", "migrations", "getmigrationtest1", "getmigrationtest2"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all migrations if no name given
	cmdArgs = []string{"get", "migrations"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "NAME                CLUSTERPAIR    STAGE     STATUS    VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationtest1   clusterpair1                       0/0       0/0         \n"
	// Should get only one migration if name given
	cmdArgs = []string{"get", "migrations", "getmigrationtest1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationsWithClusterPair(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationtest1", "default", "clusterpair1", []string{"namespace1"})
	createMigrationAndVerify(t, "getmigrationtest2", "default", "clusterpair2", []string{"namespace1"})

	expected := "NAME                CLUSTERPAIR    STAGE     STATUS    VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationtest1   clusterpair1                       0/0       0/0         \n"

	cmdArgs := []string{"get", "migrations", "-c", "clusterpair1"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetMigrationsWithStatusAndProgress(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationstatustest", "default", "clusterpair1", []string{"namespace1"})
	migration, err := k8s.Instance().GetMigration("getmigrationstatustest", "default")
	require.NoError(t, err, "Error getting migration")

	// Update the status of the migration
	migration.CreationTimestamp = metav1.Now()
	migration.Status.Stage = storkv1.MigrationStageFinal
	migration.Status.Status = storkv1.MigrationStatusSuccessful
	migration.Status.Volumes = []*storkv1.VolumeInfo{}
	migration, err = k8s.Instance().UpdateMigration(migration)

	expected := "NAME                     CLUSTERPAIR    STAGE     STATUS       VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationstatustest   clusterpair1   Final     Successful   0/0       0/0         " + toTimeString(migration.CreationTimestamp) + "\n"
	cmdArgs := []string{"get", "migrations", "getmigrationstatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateMigrationsNoNamespace(t *testing.T) {
	cmdArgs := []string{"create", "migrations", "-c", "clusterPair1", "migration1"}

	expected := "error: Need to provide atleast one namespace to migrate"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationsNoClusterPair(t *testing.T) {
	cmdArgs := []string{"create", "migrations", "migration1"}

	expected := "error: ClusterPair name needs to be provided for migration"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrationsNoName(t *testing.T) {
	cmdArgs := []string{"create", "migrations"}

	expected := "error: Exactly one name needs to be provided for migration name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateMigrations(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "createmigration", "default", "clusterpair1", []string{"namespace1"})
}

func TestCreateDuplicateMigrations(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "createmigration", "default", "clusterpair1", []string{"namespace1"})
	cmdArgs := []string{"create", "migrations", "-c", "clusterpair1", "--namespaces", "namespace1", "createmigration"}

	expected := "Error from server (AlreadyExists): migrations.stork.libopenstorage.org \"createmigration\" already exists"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteMigrationsNoMigrationName(t *testing.T) {
	cmdArgs := []string{"delete", "migrations"}

	var migrationList storkv1.MigrationList
	expected := "error: At least one argument needs to be provided for migration name"
	testCommon(t, cmdArgs, &migrationList, expected, true)
}

func TestDeleteMigrationsNoMigration(t *testing.T) {
	cmdArgs := []string{"delete", "migrations", "-c", "migration1"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteMigrations(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "deletemigration", "default", "clusterpair1", []string{"namespace1"})

	cmdArgs := []string{"delete", "migrations", "deletemigration"}
	expected := "Migration deletemigration deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"delete", "migrations", "deletemigration"}
	expected = "Error from server (NotFound): migrations.stork.libopenstorage.org \"deletemigration\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	createMigrationAndVerify(t, "deletemigration1", "default", "clusterpair1", []string{"namespace1"})
	createMigrationAndVerify(t, "deletemigration2", "default", "clusterpair2", []string{"namespace1"})

	cmdArgs = []string{"delete", "migrations", "deletemigration1", "deletemigration2"}
	expected = "Migration deletemigration1 deleted successfully\n"
	expected += "Migration deletemigration2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	createMigrationAndVerify(t, "deletemigration1", "default", "clusterpair1", []string{"namespace1"})
	createMigrationAndVerify(t, "deletemigration2", "default", "clusterpair1", []string{"namespace1"})

	cmdArgs = []string{"delete", "migrations", "-c", "clusterpair1"}
	expected = "Migration deletemigration1 deleted successfully\n"
	expected += "Migration deletemigration2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}
