package tests

import (
	"fmt"
	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/torpedo/drivers/node/ibm"
	"github.com/portworx/torpedo/drivers/scheduler/aks"
	"github.com/portworx/torpedo/drivers/scheduler/oke"
	"github.com/portworx/torpedo/pkg/log"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
	// https://github.com/kubernetes/client-go/issues/242
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

const (
	scaleTimeout = 10 * time.Minute
)

// This test performs basic test of scaling up and down the asg cluster
var _ = Describe("{ClusterScaleUpDown}", func() {
	var testrailID = 58847
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58847
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("ClusterScaleUpDown", "Validate storage nodes scale down and scale up", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to validate that storage nodes are not lost during asg scaledown", func() {
		log.InfoD("Has to validate that storage nodes are not lost during asg scaledown")
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("asgscaleupdown-%d", i))...)
		}

		ValidateApplications(contexts)

		initialNodeCount, err := Inst().S.GetASGClusterSize()
		log.FailOnError(err, "Failed to Get ASG cluster size")

		scaleupCount := initialNodeCount + initialNodeCount/2

		scaleupCount = (scaleupCount / 3) * 3
		stepLog := fmt.Sprintf("scale up cluster from %d to %d nodes and validate",
			initialNodeCount, scaleupCount)

		Step(stepLog, func() {
			log.InfoD(stepLog)
			Scale(scaleupCount)
			stepLog = fmt.Sprintf("wait for %s minutes for auto recovery of storeage nodes",
				Inst().AutoStorageNodeRecoveryTimeout.String())

			Step(stepLog, func() {
				log.InfoD(stepLog)
				time.Sleep(Inst().AutoStorageNodeRecoveryTimeout)
			})
			// After scale up, get fresh list of nodes
			// by re-initializing scheduler and volume driver
			err = Inst().S.RefreshNodeRegistry()
			log.FailOnError(err, "Verify node registry refresh")

			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "Verify driver end points refresh")

			stepLog = "validate number of storage nodes after scale up"
			Step(fmt.Sprintf(stepLog), func() {
				log.InfoD(stepLog)
				ValidateClusterSize(scaleupCount)
			})

		})

		stepLog = fmt.Sprintf("scale down cluster back to original size of %d nodes",
			initialNodeCount)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			Scale(initialNodeCount)

			stepLog = fmt.Sprintf("wait for %s minutes for auto recovery of storeage nodes",
				Inst().AutoStorageNodeRecoveryTimeout.String())

			Step(stepLog, func() {
				log.InfoD(stepLog)
				time.Sleep(Inst().AutoStorageNodeRecoveryTimeout)
			})

			// After scale down, get fresh list of nodes
			// by re-initializing scheduler and volume driver
			err = Inst().S.RefreshNodeRegistry()
			log.FailOnError(err, "verify refresh node registry")

			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "verify refresh driver end points")

			stepLog = fmt.Sprintf("validate number of storage nodes after scale down")
			Step(stepLog, func() {
				log.InfoD(stepLog)
				ValidateClusterSize(initialNodeCount)
			})
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test randomly kills one volume driver node and ensures cluster remains
// intact by ASG
var _ = Describe("{ASGKillRandomNodes}", func() {
	var testrailID = 58848
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58848
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("ASGKillRandomNodes", "Validate PX and Apps when ASG enabled nodes are deleted", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("keeps killing worker nodes", func() {

		contexts = make([]*scheduler.Context, 0)

		// Get list of nodes where storage driver is installed
		storageDriverNodes := node.GetStorageDriverNodes()

		Step("Ensure apps are deployed", func() {
			log.InfoD("Deploy Apps")
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("asgchaos-%d", i))...)
			}
		})

		ValidateApplications(contexts)

		stepLog := "Randomly kill one storage node"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			// set frequency mins depending on the chaos level
			var frequency int
			switch Inst().ChaosLevel {
			case 5:
				frequency = 15
			case 4:
				frequency = 30
			case 3:
				frequency = 45
			case 2:
				frequency = 60
			case 1:
				frequency = 90
			default:
				frequency = 30

			}
			if Inst().MinRunTimeMins == 0 {
				// Run once
				asgKillANodeAndValidate(storageDriverNodes)

				// Validate applications and tear down
				opts := make(map[string]bool)
				opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
				ValidateAndDestroy(contexts, opts)
			} else {
				// Run once till timer gets triggered
				asgKillANodeAndValidate(storageDriverNodes)

				Step("validate applications", func() {
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})

				// Run repeatedly
				ticker := time.NewTicker(time.Duration(frequency) * time.Minute)
				stopChannel := time.After(time.Duration(Inst().MinRunTimeMins) * time.Minute)
			L:
				for {
					select {
					case <-ticker.C:
						asgKillANodeAndValidate(storageDriverNodes)

						Step("validate applications", func() {
							for _, ctx := range contexts {
								ValidateContext(ctx)
							}
						})
					case <-stopChannel:
						ticker.Stop()
						// ticker may expire/time out in between, apps may not be
						// in correct condition to be validated. Just tear them down.
						opts := make(map[string]bool)
						opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
						Step("destroy apps", func() {
							for _, ctx := range contexts {
								TearDownContext(ctx, opts)
							}
						})
						break L
					}
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func Scale(count int64) {
	perZoneCount := count
	// In multi-zone ASG cluster, node count is per zone
	if Inst().S.String() != aks.SchedName {
		zones, err := Inst().S.GetZones()
		dash.VerifyFatal(err, nil, "Verify Get zones")

		perZoneCount = count / int64(len(zones))
	}

	t := func() (interface{}, bool, error) {

		err = Inst().S.SetASGClusterSize(perZoneCount, scaleTimeout)
		if err != nil {
			return "", true, err
		}
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(t, 6*time.Minute, 2*time.Minute)
	dash.VerifyFatal(err, nil, "Verify Set ASG Cluster size")

}

func asgKillANodeAndValidate(storageDriverNodes []node.Node) {
	rand.Seed(time.Now().Unix())
	nodeToKill := storageDriverNodes[rand.Intn(len(storageDriverNodes))]

	stepLog := fmt.Sprintf("Deleting node [%v]", nodeToKill.Name)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		err := Inst().S.DeleteNode(nodeToKill)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Valdiate node %s deletion", nodeToKill.Name))
	})

	waitTime := 10
	if Inst().S.String() == oke.SchedName {
		waitTime = 15 // OKE takes more time to replace the node
	}

	stepLog = fmt.Sprintf("Wait for %d min. to node get replaced by autoscalling group", waitTime)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		time.Sleep(time.Duration(waitTime) * time.Minute)
	})

	err := Inst().S.RefreshNodeRegistry()
	log.FailOnError(err, "Verify node registry refresh")

	err = Inst().V.RefreshDriverEndpoints()
	log.FailOnError(err, "Verify driver end points refresh")

	stepLog = fmt.Sprintf("Validate number of storage nodes after killing node [%v]", nodeToKill.Name)
	Step(stepLog, func() {
		log.InfoD(stepLog)
		ValidateClusterSize(int64(len(storageDriverNodes)))
	})
}

func waitForIBMNodeTODeploy() error {

	workers, err := ibm.GetWorkers()
	if err != nil {
		return err
	}

	var newWorkerID string
	for _, w := range workers {
		workerState := w.Lifecycle.ActualState
		if workerState == ibm.DEPLOYING || workerState == ibm.PROVISIONING || workerState == ibm.PROVISION_PENDING {
			newWorkerID = w.WorkerID
			break
		}
	}

	if newWorkerID == "" {
		return fmt.Errorf("no new worker found")
	}

	n := node.Node{}
	n.StorageNode = &api.StorageNode{Hostname: newWorkerID}
	t := func() (interface{}, bool, error) {

		currState, err := Inst().N.GetNodeState(n)
		if err != nil {
			return "", true, err
		}
		if currState == ibm.DEPLOYED {
			return "", false, nil
		}
		return "", true, fmt.Errorf("node [%s] not deployed yet, current state : %s", n.Hostname, currState)
	}

	_, err = task.DoRetryWithTimeout(t, 20*time.Minute, 1*time.Minute)

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
