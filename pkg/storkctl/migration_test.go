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
	cmdArgs := []string{"migrations"}

	var migrationList storkv1.MigrationList
	expected := "No resources found.\n"
	testCommon(t, newGetCommand, cmdArgs, &migrationList, expected, false)
}

func createMigrationAndVerify(t *testing.T, name string, clusterpair string, namespaces []string) {
	cmdArgs := []string{"migrations", "-c", clusterpair, "--namespaces", strings.Join(namespaces, ","), name}

	var migrationList storkv1.MigrationList
	expected := "Migration " + name + " created successfully\n"
	testCommon(t, newCreateCommand, cmdArgs, &migrationList, expected, false)

	// Make sure it was created correctly
	migration, err := k8s.Instance().GetMigration(name, "test")
	require.NoError(t, err, "Error getting migration")
	require.Equal(t, name, migration.Name, "Migration name mismatch")
	require.Equal(t, "test", migration.Namespace, "Migration namespace mismatch")
	require.Equal(t, clusterpair, migration.Spec.ClusterPair, "Migration clusterpair mismatch")
	require.Equal(t, namespaces, migration.Spec.Namespaces, "Migration namespace mismatch")
}

func TestGetMigrationsOneMigration(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationtest", "clusterpair1", []string{"namespace1"})

	expected := "NAME               CLUSTERPAIR    STAGE     STATUS    VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationtest   clusterpair1                       0/0       0/0         \n"

	cmdArgs := []string{"migrations"}
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)
}

func TestGetMigrationsMultiple(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationtest1", "clusterpair1", []string{"namespace1"})
	createMigrationAndVerify(t, "getmigrationtest2", "clusterpair2", []string{"namespace1"})

	expected := "NAME                CLUSTERPAIR    STAGE     STATUS    VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationtest1   clusterpair1                       0/0       0/0         \n" +
		"getmigrationtest2   clusterpair2                       0/0       0/0         \n"

	cmdArgs := []string{"migrations", "getmigrationtest1", "getmigrationtest2"}
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)

	// Should get all migrations if no name given
	cmdArgs = []string{"migrations"}
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)

	expected = "NAME                CLUSTERPAIR    STAGE     STATUS    VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationtest1   clusterpair1                       0/0       0/0         \n"
	// Should get only one migration if name given
	cmdArgs = []string{"migrations", "getmigrationtest1"}
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)
}

func TestGetMigrationsWithStatusAndProgress(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "getmigrationstatustest", "clusterpair1", []string{"namespace1"})
	migration, err := k8s.Instance().GetMigration("getmigrationstatustest", "test")
	require.NoError(t, err, "Error getting migration")

	// Update the status of the migration
	migration.CreationTimestamp = metav1.Now()
	migration.Status.Stage = storkv1.MigrationStageFinal
	migration.Status.Status = storkv1.MigrationStatusSuccessful
	migration.Status.Volumes = []*storkv1.VolumeInfo{}
	migration, err = k8s.Instance().UpdateMigration(migration)

	expected := "NAME                     CLUSTERPAIR    STAGE     STATUS       VOLUMES   RESOURCES   CREATED\n" +
		"getmigrationstatustest   clusterpair1   Final     Successful   0/0       0/0         " + toTimeString(migration.CreationTimestamp) + "\n"
	cmdArgs := []string{"migrations", "getmigrationstatustest"}
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)
}

func TestCreateMigrationsNoNamespace(t *testing.T) {
	cmdArgs := []string{"migrations", "-c", "clusterPair1", "migration1"}

	expected := "error: Need to provide atleast one namespace to migrate"
	testCommon(t, newCreateCommand, cmdArgs, nil, expected, true)
}

func TestCreateMigrationsNoClusterPair(t *testing.T) {
	cmdArgs := []string{"migrations", "migration1"}

	expected := "error: ClusterPair name needs to be provided for migration"
	testCommon(t, newCreateCommand, cmdArgs, nil, expected, true)
}

func TestCreateMigrationsNoName(t *testing.T) {
	cmdArgs := []string{"migrations"}

	expected := "error: Exactly one name needs to be provided for migration name"
	testCommon(t, newCreateCommand, cmdArgs, nil, expected, true)
}

func TestCreateMigrations(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "createmigration", "clusterpair1", []string{"namespace1"})
}

func TestDeleteMigrationsNoMigrationName(t *testing.T) {
	cmdArgs := []string{"migrations"}

	var migrationList storkv1.MigrationList
	expected := "error: At least one argument needs to be provided for migration name"
	testCommon(t, newDeleteCommand, cmdArgs, &migrationList, expected, true)
}

func TestDeleteMigrationsNoMigration(t *testing.T) {
	cmdArgs := []string{"migrations", "-c", "migration1"}

	expected := "No resources found.\n"
	testCommon(t, newDeleteCommand, cmdArgs, nil, expected, false)
}

func TestDeleteMigrations(t *testing.T) {
	defer resetTest()
	createMigrationAndVerify(t, "deletemigration", "clusterpair1", []string{"namespace1"})

	cmdArgs := []string{"migrations", "deletemigration"}
	expected := "Migration deletemigration deleted successfully\n"
	testCommon(t, newDeleteCommand, cmdArgs, nil, expected, false)

	cmdArgs = []string{"migrations", "deletemigration"}
	expected = "Error from server (NotFound): migrations.stork.libopenstorage.org \"deletemigration\" not found"
	testCommon(t, newDeleteCommand, cmdArgs, nil, expected, true)

	createMigrationAndVerify(t, "deletemigration1", "clusterpair1", []string{"namespace1"})
	createMigrationAndVerify(t, "deletemigration2", "clusterpair2", []string{"namespace1"})

	cmdArgs = []string{"migrations", "deletemigration1", "deletemigration2"}
	expected = "Migration deletemigration1 deleted successfully\n"
	expected += "Migration deletemigration2 deleted successfully\n"
	testCommon(t, newDeleteCommand, cmdArgs, nil, expected, false)

	createMigrationAndVerify(t, "deletemigration1", "clusterpair1", []string{"namespace1"})
	createMigrationAndVerify(t, "deletemigration2", "clusterpair1", []string{"namespace1"})

	cmdArgs = []string{"migrations", "-c", "clusterpair1"}
	expected = "Migration deletemigration1 deleted successfully\n"
	expected += "Migration deletemigration2 deleted successfully\n"
	testCommon(t, newDeleteCommand, cmdArgs, nil, expected, false)
}
