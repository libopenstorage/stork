//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type configTestAppWithNearSync struct {
	appKey           string
	stressAppKey     string
	repl             int
	ns_repl_strategy string
}

func TestApplicationsWithNearSync(t *testing.T) {

	// create secret on source and destination
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

	appConfigList := [][]string{
		{"Postgres", "nearsync-postgres", "nearsync-pgbench"},
		// {"nearsync-cassandra", "nearsync-cassandra-stress"},
		// {"nearsync-elasticsearch", "nearsync-elasticsearch-rally"},
	}
	for _, repl := range []int{2, 3} {
		for _, ns_repl_strategy := range []string{"aggressive"} {
			for _, appConfig := range appConfigList {
				config := configTestAppWithNearSync{
					appKey:           appConfig[1],
					stressAppKey:     appConfig[2],
					repl:             repl,
					ns_repl_strategy: ns_repl_strategy,
				}
				t.Run(
					fmt.Sprintf("testApp%vWithNearSync", appConfig[0]),
					func(t *testing.T) { helperTestAppWithNearSync(t, config) })
			}
		}
	}
}

func helperTestAppWithNearSync(t *testing.T, config configTestAppWithNearSync) {
	instanceIDs := []string{"test"}
	storageClass := "px-sc" // TODO: change to postgres-sc
	migrationName := "failover-migration"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, config.appKey)
	cleanup(t, namespaces[0], storageClass)

	// create storage class
	_, err := createStorageClass(StorageClass{
		name:                          "px-sc",
		provisioner:                   "kubernetes.io/portworx-volume",
		repl:                          config.repl,
		nearsync:                      true,
		nearsync_replication_strategy: config.ns_repl_strategy,
	})
	require.NoError(t, err, "unable to create storage class")

	// start application
	ctxs := scheduleAppAndWait(t, instanceIDs, config.appKey)

	// start and validate migration for the application
	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)
	validateMigrationOnSrcAndDest(
		t, migrationName, namespaces[0], preMigrationCtxs[0],
		startAppsOnMigration, uint64(4), uint64(0))
	time.Sleep(time.Second * 10)

	// start the stress tool after migration completes
	err = schedulerDriver.AddTasks(
		ctxs[0],
		scheduler.ScheduleOptions{
			AppKeys: []string{config.stressAppKey},
		})
	require.NoError(t, err, "error scheduling app")
	logrus.Infof("pause to load data in db")
	time.Sleep(time.Minute * 5)

	// disable cluster domain for src, then
	// start and validate failover on dest
	defer func() {
		executeOnDestination(t, func() { updateClusterDomain(t, true) })
	}()
	executeOnDestination(t, func() {
		updateClusterDomain(t, false)
		startFailover(t, actionName, namespaces)
		validateFailover(t, actionName, namespaces, preMigrationCtxs, true)
	})
}

func updateClusterDomain(t *testing.T, activate bool) {
	storageNode := node.GetStorageDriverNodes()[0]
	var cmd string
	if activate {
		cmd = "cluster domains activate --name dc1"
	} else {
		cmd = "cluster domains deactivate --name dc1"
	}
	out, err := volumeDriver.GetPxctlCmdOutput(storageNode, cmd)
	require.NoError(t, err)
	logrus.Infof("Pxctl output: %v", out)
}
