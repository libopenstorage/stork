//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	// "fmt"

	"github.com/portworx/torpedo/drivers/node"
	// "github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	// "github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
	storage_v1 "k8s.io/api/storage/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "time"
)

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

	// t.Run("testTest", testTest)
	t.Run("testAppNearSyncBasic", testAppNearSyncBasic)
}

type StorageClass struct {
	name        string
	provisioner string
	// parameters
	repl                          int
	nearsync                      bool
	nearsync_replication_strategy string
}

func createStorageClass(storageClass StorageClass) (*storage_v1.StorageClass, error) {
	parameters := make(map[string]string)
	if storageClass.repl != 0 {
		parameters["repl"] = strconv.Itoa(storageClass.repl)
	} else {
		parameters["repl"] = "1"
	}
	if storageClass.nearsync {
		parameters["nearsync"] = "true"
	}
	if storageClass.nearsync_replication_strategy != "" {
		parameters["nearsync_replication_strategy"] = storageClass.nearsync_replication_strategy
	}
	return storage.Instance().CreateStorageClass(&storage_v1.StorageClass{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: storageClass.name,
		},
		Provisioner: storageClass.provisioner,
		Parameters:  parameters,
	})
}

func testTest(t *testing.T) {
	executeOnDestination(t, func() {
		storageNode := node.GetStorageDriverNodes()[0]
		logrus.Infof("[DEBUG] storageNode: %v", storageNode)
		out, err := volumeDriver.GetPxctlCmdOutputConnectionOpts(
			storageNode,
			"cluster domains deactivate --name dc1",
			node.ConnectionOpts{
				IgnoreError:     false,
				Timeout:         1 * time.Minute,
				TimeBeforeRetry: 5 * time.Second,
			},
			false,
		)
		require.NoError(t, err)
		logrus.Infof("Pxctl output: %v", out)
	})

	logrus.Infof("Sleeping for 30 seconds")
	time.Sleep(time.Second * 30)

	executeOnDestination(t, func() {
		storageNode := node.GetStorageDriverNodes()[0]
		logrus.Infof("storageNode: %v", storageNode)
		out, err := volumeDriver.GetPxctlCmdOutputConnectionOpts(
			storageNode,
			"cluster domains activate --name dc1",
			node.ConnectionOpts{
				IgnoreError:     false,
				Timeout:         1 * time.Minute,
				TimeBeforeRetry: 5 * time.Second,
			},
			false,
		)
		require.NoError(t, err)
		logrus.Infof("Pxctl output: %v", out)
	})
}

// Simple failover with one namespace and one app
func testAppNearSyncBasic(t *testing.T) {
	// appKey := "nearsync-postgres"
	appKey := "nearsync-elasticsearch"
	instanceIDs := []string{"basic"}
	storageClass := "px-sc" // TODO: change to postgres-sc
	migrationName := "failover-migration"
	actionName := "failover-action"
	namespaces := getNamespaces(instanceIDs, appKey)
	cleanup(t, namespaces[0], storageClass)
	executeOnDestination(t, func() { updateClusterDomain(t, true) })
	time.Sleep(time.Second * 20) // to let PX nodes start

	_, err := createStorageClass(StorageClass{
		name:        "px-sc",
		provisioner: "kubernetes.io/portworx-volume",
		repl:        2,
		nearsync:    true,
	})
	require.NoError(t, err, "unable to create storage class")

	ctxs := scheduleAppAndWait(t, instanceIDs, appKey)

	// service, err := core.Instance().GetService("postgres", namespaces[0])
	// _, err = core.Instance().CreateConfigMap(&core_v1.ConfigMap{
	// 	ObjectMeta: meta_v1.ObjectMeta{
	// 		Name:      "pgbench-configmap",
	// 		Namespace: namespaces[0],
	// 	},
	// 	Data: map[string]string{
	// 		"clusterIP": service.Spec.ClusterIP,
	// 	},
	// })

	startAppsOnMigration := false
	preMigrationCtxs, ctxs, _ := triggerMigrationMultiple(
		t, ctxs, migrationName, namespaces, true, false, startAppsOnMigration)

	validateMigrationOnSrcAndDest(
		t, migrationName, namespaces[0], preMigrationCtxs[0],
		startAppsOnMigration, uint64(4), uint64(0))

	time.Sleep(time.Second * 10)
	err = schedulerDriver.AddTasks(
		ctxs[0],
		scheduler.ScheduleOptions{
			// AppKeys: []string{"nearsync-pgbench"},
			// AppKeys: []string{"nearsync-cassandra-stress"},
			AppKeys: []string{"nearsync-elasticsearch-rally"},
		})
	require.NoError(t, err, "Error scheduling app")

	logrus.Infof("pause to load data in db")
	time.Sleep(time.Minute * 5)

	// scaleFactor := scaleDownApps(t, ctxs)
	// logrus.Infof("scaleFactor: %v", scaleFactor)

	executeOnDestination(t, func() {
		updateClusterDomain(t, false)
		startFailover(t, actionName, namespaces)
		validateFailover(t, actionName, namespaces, preMigrationCtxs, true)
		updateClusterDomain(t, true)
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
