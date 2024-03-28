//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"github.com/libopenstorage/stork/pkg/log"
	"strings"
	"testing"
	"time"

	"github.com/portworx/torpedo/drivers/scheduler"
)

type configTestAppWithNearSync struct {
	appKey               string
	stressAppKey         string
	instanceId           string
	repl                 int
	nearsyncReplStrategy string
}

func TestApplicationsWithNearSync(t *testing.T) {
	var testResult = testResultFail
	currentTestSuite = t.Name()
	defer updateDashStats(t.Name(), &testResult)

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
		{"Cassandra", "nearsync-cassandra", "nearsync-cassandra-stress"},
		{"Elasticsearch", "nearsync-elasticsearch", "nearsync-elasticsearch-rally"},
	}
	for _, repl := range []int{2, 3} {
		for _, nearsyncReplStrategy := range []string{"Aggressive", "Optimized"} {
			for _, appConfig := range appConfigList {
				config := configTestAppWithNearSync{
					appKey:               appConfig[1],
					stressAppKey:         appConfig[2],
					instanceId:           fmt.Sprintf("repl%v-%v", repl, strings.ToLower(nearsyncReplStrategy)),
					repl:                 repl,
					nearsyncReplStrategy: strings.ToLower(nearsyncReplStrategy),
				}
				t.Run(
					fmt.Sprintf("testNearsync%vRepl%v%v", appConfig[0], repl, nearsyncReplStrategy),
					func(t *testing.T) { helperTestAppWithNearSync(t, config) })
			}
		}
	}
}

func helperTestAppWithNearSync(t *testing.T, config configTestAppWithNearSync) {
	instanceIDs := []string{config.instanceId}
	storageClass := "px-sc"
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
		nearsync_replication_strategy: config.nearsyncReplStrategy,
	})
	log.FailOnError(t, err, "unable to create storage class")

	// start application
	ctxs := scheduleAppAndWait(t, instanceIDs, config.appKey)

	// start and validate migration
	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)
	validateMigrationOnSrc(t, migrationName, namespaces)

	// start the stress tool
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "unable to set source kubeconfig")

	err = schedulerDriver.AddTasks(
		ctxs[0],
		scheduler.ScheduleOptions{
			AppKeys: []string{config.stressAppKey},
		})
	log.FailOnError(t, err, "error scheduling app")
	log.Info("wait for some data to load in db")
	time.Sleep(time.Minute * 5)

	deactivateClusterDomainAndTriggerFailover(
		t, actionName, namespaces, preMigrationCtxs, true)
}
