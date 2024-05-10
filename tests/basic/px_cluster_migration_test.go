package tests

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ibm"
	"github.com/portworx/torpedo/pkg/log"

	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"

	// https://github.com/kubernetes/client-go/issues/242
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

var (
	upgradeTimeoutMins = 90 * time.Minute
)

func waitForIKSMasterUpdate(schedVersion string) error {

	t := func() (interface{}, bool, error) {

		iksCluster, err := ibm.GetCluster()
		if err != nil {
			return nil, false, err
		}

		if strings.Contains(iksCluster.MasterKubeVersion, "pending") {
			return nil, true, fmt.Errorf("waiting for master update to complete.Current status : %s", iksCluster.MasterKubeVersion)
		}

		masterMajorVersion := iksCluster.MasterKubeVersion
		if strings.Contains(schedVersion, "openshift") {
			masterMajorVersion = strings.Split(iksCluster.MasterKubeVersion, "_")[0]
			masterMajorVersion = fmt.Sprintf("%s_openshift", masterMajorVersion)
		}

		if strings.Contains(masterMajorVersion, schedVersion) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("master update to %s failed", schedVersion)
	}
	_, err := task.DoRetryWithTimeout(t, 60*time.Minute, 2*time.Minute)

	return err

}

func waitForIBMNodeToDelete(nodeToKill node.Node) error {
	t := func() (interface{}, bool, error) {

		currState, err := Inst().N.GetNodeState(nodeToKill)
		if err != nil {
			return "", true, err
		}
		if currState == ibm.DELETED {
			return "", false, nil
		}

		return "", true, fmt.Errorf("node [%s] not deleted yet, current state : %s", nodeToKill.Hostname, currState)

	}

	_, err := task.DoRetryWithTimeout(t, 10*time.Minute, 1*time.Minute)
	return err
}

func upgradeIKSWorkerNodes(schedVersion, poolName string) error {

	storageDriverNodes := node.GetStorageDriverNodes()
	for _, sNode := range storageDriverNodes {

		if err := ibm.ReplaceWorkerNodeWithUpdate(sNode); err != nil {
			return err
		}

		if err := waitForIBMNodeToDelete(sNode); err != nil {
			return err
		}

		if err := waitForIBMNodeTODeploy(); err != nil {
			return err
		}

	}

	workers, err := ibm.GetWorkers()
	if err != nil {
		return err
	}

	for _, worker := range workers {
		if worker.PoolName == poolName {
			majorVersion := worker.KubeVersion.Actual
			if strings.Contains(schedVersion, "openshift") {
				majorVersion = strings.Split(worker.KubeVersion.Actual, "_")[0]
				majorVersion = fmt.Sprintf("%s_openshift", majorVersion)

			}
			if !strings.Contains(majorVersion, schedVersion) {
				return fmt.Errorf("node %s has version %s expected %s", worker.WorkerID, worker.KubeVersion.Actual, schedVersion)
			}

		}
	}

	return nil
}

var _ = Describe("{MigratePXCluster}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("MigratePXCluster", "Validate Worker pool upgrade and PX migration", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "Create worker pool, migrate PX  and ensure everything is running fine"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		initialNodeCount, err := Inst().N.GetASGClusterSize()
		log.FailOnError(err, "error getting ASG cluster size")

		initialStNodes := node.GetStorageNodes()

		log.InfoD("Validating cluster size before upgrade. Initial Node Count: [%v]", initialNodeCount)
		ValidateClusterSize(initialNodeCount)

		migrationHops := strings.Split(Inst().MigrationHops, ",")
		dash.VerifyFatal(len(migrationHops) > 0, true, "upgrade hops are provided?")

		for _, schedVersion := range migrationHops {
			schedVersion = strings.TrimSpace(schedVersion)
			stepLog = fmt.Sprintf("start the upgrade of scheduler to version [%v]", schedVersion)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().N.SetClusterVersion(schedVersion, upgradeTimeoutMins)
				log.FailOnError(err, "Failed to set cluster version")
			})

			if IsIksCluster() {
				stepLog = fmt.Sprintf("update IKS cluster master node to version %s", schedVersion)
				Step(stepLog, func() {
					err = waitForIKSMasterUpdate(schedVersion)
					dash.VerifyFatal(err, nil, fmt.Sprintf("verify IKS master update to version %s", schedVersion))
				})

			}

			err = Inst().S.RefreshNodeRegistry()
			log.FailOnError(err, "Node registry refresh failed")

			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "Refresh Driver end points failed")

		}

		initialWorkers, err := ibm.GetWorkers()
		log.FailOnError(err, "error getting IBM workers")
		migPoolName := "mig-pool"

		Step("Create a new pool for migration", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("migratepxcluster-%d", i))...)
			}
			Step("validate all apps", func() {
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})

			zones, err := Inst().N.GetZones()
			log.FailOnError(err, "Zones empty")

			sizePerZone := initialNodeCount / int64(len(zones))

			err = ibm.AddWorkerPool(migPoolName, sizePerZone)
			log.FailOnError(err, fmt.Sprintf("error creating worker pool %s", migPoolName))

			stepLog = fmt.Sprintf("wait for %s minutes for auto recovery of storage nodes",
				Inst().AutoStorageNodeRecoveryTimeout.String())
			Step(stepLog, func() {
				log.InfoD(fmt.Sprintf("wait for %s minutes for auto recovery of storage nodes",
					Inst().AutoStorageNodeRecoveryTimeout.String()))
				time.Sleep(Inst().AutoStorageNodeRecoveryTimeout)
			})

			err = Inst().S.RefreshNodeRegistry()
			log.FailOnError(err, "Node registry refresh failed")

			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "Refresh Driver end points failed")
		})

		Step("Initiate worker pool migration", func() {

			for _, worker := range initialWorkers {
				if worker.PoolName == ibm.IksPXWorkerpool {

					n, err := node.GetNodeByName(worker.NetworkInterfaces[0].IpAddress)
					log.FailOnError(err, fmt.Sprintf("error getting node with hostname %s", worker.WorkerID))
					err = ibm.RemoveWorkerNode(n)
					log.FailOnError(err, fmt.Sprintf("error removing node %s", n.Hostname))
					err = waitForIBMNodeToDelete(n)
					log.FailOnError(err, fmt.Sprintf("node %s not deleted", n.Hostname))
					log.Infof("wait for new node to stabilize")
					time.Sleep(3 * time.Minute)
					err = Inst().S.RefreshNodeRegistry()
					log.FailOnError(err, "Node registry refresh failed")

					err = Inst().V.RefreshDriverEndpoints()
					log.FailOnError(err, "Refresh Driver end points failed")

					newStNodes := node.GetStorageNodes()
					if len(newStNodes) != len(initialStNodes) {
						log.Infof("st node count not matching.")
						aJSON, _ := json.Marshal(newStNodes)
						fmt.Printf("JSON Print - \n%s\n", string(aJSON))
					}
					dash.VerifyFatal(len(newStNodes), len(initialStNodes), fmt.Sprintf("verfiy storage node count after deleteing %s", n.Name))
				}

			}

		})

		stepLog = fmt.Sprintf("validate number of storage nodes after migration")
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateClusterSize(initialNodeCount)
		})

		Step("validate all apps after migration", func() {
			for _, ctx := range contexts {
				ValidateContext(ctx)
			}
		})
		PerformSystemCheck()

		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})
