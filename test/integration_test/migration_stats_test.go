//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"html/template"
	"os"
	"testing"

	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/utils"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

type MigrateApps struct {
	MigrationKey      string
	MigrationAppKey   string
	IncludeVolumes    bool
	IncludeResources  bool
	StartApplications bool
}

var appList = []MigrateApps{
	{"mysql-migration", "mysql-1-pvc", true, true, false},
	{"cassandra-migration", "cassandra", true, true, false},
	{"elasticsearch-migration", "elasticsearch", true, true, false},
}

func TestMigrationExportStats(t *testing.T) {
	t.Run("migrationStatsExportTest", migrationStatsExportTest)
	t.Run("migrationStatsDisplayTest", migrationStatsDisplayTest)
}

func migrationStatsExportTest(t *testing.T) {
	var err error
	var includeVolumes = true
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		require.NoError(t, err, "Error resetting source config")
	}()

	for _, app := range appList {
		currCtxs, err := schedulerDriver.Schedule(app.MigrationKey,
			scheduler.ScheduleOptions{AppKeys: []string{app.MigrationAppKey}})
		require.NoError(t, err, "Error scheduling task")
		require.Equal(t, 1, len(currCtxs), "Only one task should have started")

		err = schedulerDriver.WaitForRunning(currCtxs[0], defaultWaitTimeout, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app to get to running state")

		// create, apply and validate cluster pair specs
		err = scheduleClusterPair(currCtxs[0], false, true, defaultClusterPairDir, "", false)
		require.NoError(t, err, "Error scheduling cluster pair")

		currMigNamespace := app.MigrationAppKey + "-" + app.MigrationKey
		currMig, err := createMigration(t, app.MigrationKey, currMigNamespace, "remoteclusterpair", currMigNamespace, &app.IncludeResources, &includeVolumes, &app.StartApplications)
		require.NoError(t, err, "failed to create migration: %s in namespace %s", app.MigrationKey, currMigNamespace)

		migrationList := []*v1alpha1.Migration{currMig}
		err = WaitForMigration(migrationList)
		require.NoError(t, err, "error in migrations for exported stats")

		finishedMig, err := storkops.Instance().GetMigration(currMig.Name, currMig.Namespace)
		require.Nil(t, err, "could not get completed migration")

		exportableStat := utils.GetExportableStatsFromMigrationObject(finishedMig)
		require.NotNil(t, exportableStat, "could not fetch stats from migration object")

		// Export stats to Aetos Mongo DB
		err = utils.WriteMigrationStatsToAetos(exportableStat)
		require.NoError(t, err, "error while writing stats to mongo DB")

	}
}

func migrationStatsDisplayTest(t *testing.T) {
	var err error
	// Get stats from Aetos Mongo DB
	stats, err := utils.GetMigrationStatsFromAetos(utils.AetosStatsURL)
	require.NoError(t, err, "error while fetching stats from mongo DB")

	// Print stats to the console for now
	utils.PrettyStruct(stats)

	paths := []string{
		"/specs/stats.tmpl",
	}

	// create a new file
	//err := os.MkdirAll("/reports", 0777)
	htmlStatsFile := "/reports/migstats.html"
	file, err := os.Create(htmlStatsFile)
	require.NoError(t, err, "error during creation of html stats file: %s", htmlStatsFile)

	templ := template.Must(template.ParseFiles(paths...))
	err = templ.Execute(file, stats)
	if err != nil {
		panic(err)
	}
	err = file.Close()
	require.NoError(t, err, "error during closure of html stats file: %s", htmlStatsFile)
}
