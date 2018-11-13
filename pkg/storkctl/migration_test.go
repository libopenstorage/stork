// +build unittest

package storkctl

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
)

func TestGetMigrationsNoMigration(t *testing.T) {
	cmdArgs := []string{"migrations"}

	var migrationList storkv1.MigrationList
	expected := "No resources found.\n"
	testCommon(t, newGetCommand, cmdArgs, &migrationList, expected, false)
}

// FIXME
//func TestGetMigrationsOneMigration(t *testing.T) {
//	cmdArgs := []string{"migrations"}
//
//	migration := &storkv1.Migration{
//		Spec: storkv1.MigrationSpec{
//			ClusterPair:       "clusterPair1",
//			Namespaces:        []string{"namespace1", "namespace2"},
//			IncludeResources:  true,
//			StartApplications: true,
//		},
//	}
//	migration.Name = "migration1"
//
//	var migrationList storkv1.MigrationList
//	migrationList.Items = append(migrationList.Items, *migration)
//
//	expected := "No resources found.\n"
//
//	testCommon(t, newGetCommand, cmdArgs, &migrationList, expected, false)
//}

func TestCreateMigrationsNoNamespace(t *testing.T) {
	cmdArgs := []string{"migrations", "-c", "clusterPair1", "migration1"}

	var migrationList storkv1.MigrationList
	expected := "error: Need to provide atleast one namespace to migrate"
	testCommon(t, newCreateCommand, cmdArgs, &migrationList, expected, true)
}

func TestCreateMigrations(t *testing.T) {
	cmdArgs := []string{"migrations", "-c", "clusterPair1", "--namespaces", "namespace1", "migration1"}

	var migrationList storkv1.MigrationList
	expected := "Migration migration1 created successfully\n"
	testCommon(t, newCreateCommand, cmdArgs, &migrationList, expected, false)
}

func TestDeleteMigrationsNoMigrationName(t *testing.T) {
	cmdArgs := []string{"migrations"}

	var migrationList storkv1.MigrationList
	expected := "error: At least one argument needs to be provided for migration name"
	testCommon(t, newDeleteCommand, cmdArgs, &migrationList, expected, true)
}

func TestDeleteMigrationsNoMigration(t *testing.T) {
	cmdArgs := []string{"migrations", "-c", "migration1"}

	var migrationList storkv1.MigrationList
	expected := "No resources found.\n"
	testCommon(t, newDeleteCommand, cmdArgs, &migrationList, expected, false)
}
