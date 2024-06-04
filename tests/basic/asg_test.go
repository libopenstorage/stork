package tests

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/operator"
	"github.com/portworx/torpedo/drivers/node/ibm"
	"github.com/portworx/torpedo/drivers/scheduler/aks"
	"github.com/portworx/torpedo/drivers/scheduler/anthos"
	"github.com/portworx/torpedo/drivers/scheduler/eks"
	"github.com/portworx/torpedo/drivers/scheduler/oke"
	"github.com/portworx/torpedo/drivers/scheduler/openshift"
	"github.com/portworx/torpedo/pkg/log"

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

// This test performs basic test of scaling up and down the asg cluster
var _ = Describe("{ClusterScaleUpIncreasesMaxStorageNodesPerZone}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("ClusterScaleUpIncreasesMaxStorageNodesPerZone", "Validate cluster nodes and storage nodes scale up", nil, testrailID)

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
			PrintPxctlStatus()
		})

		stc, err := Inst().V.GetDriver()
		log.FailOnError(err, "error getting volume driver")
		maxStorageNodesPerZone := *stc.Spec.CloudStorage.MaxStorageNodesPerZone
		numOfStorageNodes := len(node.GetStorageNodes())
		log.Infof("maxStorageNodesPerZone %d", int(maxStorageNodesPerZone))
		log.Infof("numOfStorageNodes %d", numOfStorageNodes)

		actualStorageNodes := numOfStorageNodes
		expectedStorageNodes := maxStorageNodesPerZone

		// In multi-zone ASG cluster, node count is per zone
		if Inst().S.String() != openshift.SchedName {
			zones, err := Inst().S.GetZones()
			dash.VerifyFatal(err, nil, "Verify Get zones")
			expectedStorageNodes = expectedStorageNodes * uint32(len(zones))
		}

		updatedMaxStorageNodesPerZone := uint32(0)
		if int(expectedStorageNodes) <= actualStorageNodes {
			//increase max per zone
			updatedMaxStorageNodesPerZone = maxStorageNodesPerZone + 1
		}

		initialStorageNodes := node.GetStorageNodes()

		stepLog = "update maxStorageNodesPerZone in storage cluster spec"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			stc.Spec.CloudStorage.MaxStorageNodesPerZone = &updatedMaxStorageNodesPerZone
			log.InfoD("updating maxStorageNodesPerZone from %d to %d", maxStorageNodesPerZone, updatedMaxStorageNodesPerZone)
			pxOperator := operator.Instance()
			_, err = pxOperator.UpdateStorageCluster(stc)
			log.FailOnError(err, "error updating storage cluster")
			log.Infof("Sleeping for %v mins for new storage nodes to created", Inst().AutoStorageNodeRecoveryTimeout)
			time.Sleep(Inst().AutoStorageNodeRecoveryTimeout)
			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "Verify driver end points refresh")
			PrintPxctlStatus()
			dash.VerifyFatal(len(node.GetStorageNodes()) > len(initialStorageNodes), true, "verify new storage node is added")
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
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
	if Inst().S.String() != aks.SchedName && Inst().S.String() != eks.SchedName {
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
	if Inst().S.String() == oke.SchedName || Inst().S.String() == anthos.SchedName {
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

var _ = Describe("{AddStorageNode}", func() {

	var contexts []*scheduler.Context

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		wantAfterSuiteSystemCheck = true
	})
	JustBeforeEach(func() {
		StartTorpedoTest("AddStorageNode", "Add a new storage node to the cloud platform", nil, 0)
	})
	stepLog := "Validating the drives and pools after adding new storage node"

	It(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = fmt.Sprintf("Adding a storage node to the platform [%s]", Inst().S.String())

		Step(stepLog, func() {

			log.InfoD(stepLog)
			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("addstnode-%d", i))...)
			}

			ValidateApplications(contexts)

			var numOfStorageNodes int
			var maxStorageNodesPerZone uint32
			var updatedMaxStorageNodesPerZone uint32 = 0
			var zones []string
			stepLog = "update maxStorageNodesPerZone in storage cluster spec"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				stc, err := Inst().V.GetDriver()

				log.FailOnError(err, "error getting volume driver")
				maxStorageNodesPerZone = *stc.Spec.CloudStorage.MaxStorageNodesPerZone
				numOfStorageNodes = len(node.GetStorageNodes())
				log.Infof("maxStorageNodesPerZone %d", int(maxStorageNodesPerZone))
				log.Infof("numOfStorageNodes %d", numOfStorageNodes)

				actualPerZoneCount := numOfStorageNodes

				// In multi-zone ASG cluster, node count is per zone
				if Inst().S.String() != openshift.SchedName && Inst().S.String() != anthos.SchedName {
					zones, err = Inst().S.GetZones()
					dash.VerifyFatal(err, nil, "Verify Get zones")

					actualPerZoneCount = numOfStorageNodes / len(zones)
				}

				if int(maxStorageNodesPerZone) <= actualPerZoneCount {
					//increase max per zone
					updatedMaxStorageNodesPerZone = uint32(actualPerZoneCount + 1)
				}

				if updatedMaxStorageNodesPerZone != 0 {

					stc.Spec.CloudStorage.MaxStorageNodesPerZone = &updatedMaxStorageNodesPerZone
					log.InfoD("updating maxStorageNodesPerZone from %d to %d", maxStorageNodesPerZone, updatedMaxStorageNodesPerZone)
					pxOperator := operator.Instance()
					_, err = pxOperator.UpdateStorageCluster(stc)
					log.FailOnError(err, "error updating storage cluster")

				}
				PrintPxctlStatus()
				//Scaling the cluster by one node
				expReplicas := len(node.GetStorageDriverNodes()) + 1
				log.InfoD("scaling up the cluster to replicas %d", expReplicas)
				Scale(int64(expReplicas))
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

			})

			stepLog = "validate PX on all nodes after adding a storage node"

			Step(stepLog, func() {
				log.InfoD(stepLog)
				nodes := node.GetStorageDriverNodes()
				for _, n := range nodes {
					log.InfoD("Check PX status on %v", n.Name)
					err := Inst().V.WaitForPxPodsToBeUp(n)
					dash.VerifyFatal(err, nil, fmt.Sprintf("verify px is up on  node %s", n.Name))
				}
			})

			err := Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")
			PrintPxctlStatus()

			expectedPerZone := maxStorageNodesPerZone
			if updatedMaxStorageNodesPerZone != 0 {
				expectedPerZone = updatedMaxStorageNodesPerZone
			}
			numOfZones := 1
			if len(zones) != 0 {
				numOfZones = len(zones)
			}
			expectedStorageNodesCount := int(expectedPerZone) * numOfZones

			if expectedStorageNodesCount >= len(node.GetStorageNodes()) {
				expectedStorageNodesCount = len(node.GetStorageNodes())
			}

			updatedStorageNodesCount := len(node.GetStorageNodes())
			dash.VerifyFatal(expectedStorageNodesCount, updatedStorageNodesCount, "verify new storage node is added")
			ValidateAndDestroy(contexts, nil)
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{AddStoragelessNode}", func() {

	var contexts []*scheduler.Context

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		wantAfterSuiteSystemCheck = true
	})
	JustBeforeEach(func() {
		StartTorpedoTest("AddStoragelessNode", "Add a new storageless node the cluster", nil, 0)
	})
	stepLog := "Validating px after adding new storageless node"

	It(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = fmt.Sprintf("Adding a storageless node to the platform [%s]", Inst().S.String())

		Step(stepLog, func() {

			log.InfoD(stepLog)
			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("addslnode-%d", i))...)
			}

			ValidateApplications(contexts)
			numOfStoragelessNodes := len(node.GetStorageLessNodes())

			Step(stepLog, func() {
				log.InfoD(stepLog)
				stc, err := Inst().V.GetDriver()

				log.FailOnError(err, "error getting volume driver")
				maxStorageNodesPerZone := *stc.Spec.CloudStorage.MaxStorageNodesPerZone
				numOfStorageNodes := len(node.GetStorageNodes())
				log.Infof("maxStorageNodesPerZone %d", int(maxStorageNodesPerZone))
				log.Infof("numOfStoragelessNodes %d", numOfStoragelessNodes)
				PrintPxctlStatus()

				var updatedMaxStorageNodesPerZone uint32 = 0

				actualPerZoneCount := numOfStorageNodes

				// In multi-zone ASG cluster, node count is per zone
				if Inst().S.String() != aks.SchedName && Inst().S.String() != openshift.SchedName && Inst().S.String() != anthos.SchedName {
					zones, err := Inst().S.GetZones()
					dash.VerifyFatal(err, nil, "Verify Get zones")

					actualPerZoneCount = numOfStorageNodes / len(zones)
				}

				if int(maxStorageNodesPerZone) > actualPerZoneCount {
					//updating max per zone
					updatedMaxStorageNodesPerZone = uint32(actualPerZoneCount)
					stc.Spec.CloudStorage.MaxStorageNodesPerZone = &updatedMaxStorageNodesPerZone
					log.InfoD("updating maxStorageNodesPerZone from %d to %d", maxStorageNodesPerZone, updatedMaxStorageNodesPerZone)
					pxOperator := operator.Instance()
					_, err = pxOperator.UpdateStorageCluster(stc)
					log.FailOnError(err, "error updating storage cluster")
				}
				//Scaling the cluster by one node
				expReplicas := len(node.GetStorageDriverNodes()) + 1
				log.InfoD("scaling up the cluster to replicas %d", expReplicas)
				Scale(int64(expReplicas))

			})

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

			stepLog = "validate PX on all nodes after adding storageless node"

			Step(stepLog, func() {
				log.InfoD(stepLog)
				nodes := node.GetStorageDriverNodes()
				for _, n := range nodes {
					log.InfoD("Check PX status on %v", n.Name)
					err := Inst().V.WaitForPxPodsToBeUp(n)
					dash.VerifyFatal(err, nil, fmt.Sprintf("verify px is up on  node %s", n.Name))
				}
			})

			err := Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")
			PrintPxctlStatus()

			updatedStoragelessNodesCount := len(node.GetStorageLessNodes())
			dash.VerifyFatal(numOfStoragelessNodes+1, updatedStoragelessNodesCount, "verify new storageless node is added")

			ValidateAndDestroy(contexts, nil)
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{RecycleStorageDriverNode}", func() {

	var contexts []*scheduler.Context

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		wantAfterSuiteSystemCheck = true
	})
	JustBeforeEach(func() {
		StartTorpedoTest("RecycleStorageDriverNode", "Test drives and pools after recycling a storage driver node", nil, 0)
	})

	It("Validating the drives and pools after recycling a node", func() {
		Step("Get the storage and storageless nodes and delete them", func() {
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("recyclenode-%d", i))...)
			}
			ValidateApplications(contexts)
			storagelessNodes, err := Inst().V.GetStoragelessNodes()
			log.FailOnError(err, fmt.Sprintf("Failed to get storageless nodes. Error: [%v]", err))
			if storagelessNodes != nil {
				delNode, err := node.GetNodeByName(storagelessNodes[0].Hostname)
				log.FailOnError(err, fmt.Sprintf("Failed to get node object using Name. Error: [%v]", err))
				Step(
					fmt.Sprintf("Listing all nodes before recycling a storageless node %s", delNode.Name),
					func() {
						workerNodes := node.GetWorkerNodes()
						for x, wNode := range workerNodes {
							log.Infof("WorkerNode[%d] is: [%s] and volDriverID is [%s]", x, wNode.Name, wNode.VolDriverNodeID)
						}
					})
				stepLog = fmt.Sprintf("Recycle a storageless node and validating the drives: %s", delNode.Name)
				Step(
					stepLog,
					func() {
						log.InfoD(stepLog)
						err := Inst().S.DeleteNode(delNode)
						log.FailOnError(err, fmt.Sprintf("Failed to recycle a node [%s]. Error: [%v]", delNode.Name, err))
						waitTime := 10
						if Inst().S.String() == oke.SchedName || Inst().S.String() == anthos.SchedName {
							waitTime = 15 // OKE takes more time to replace the node
						}

						stepLog = fmt.Sprintf("Wait for %d min. to node get replaced by autoscalling group", waitTime)
						Step(stepLog, func() {
							log.InfoD(stepLog)
							time.Sleep(time.Duration(waitTime) * time.Minute)
						})

						err = Inst().S.RefreshNodeRegistry()
						log.FailOnError(err, "Verify node registry refresh")

						err = Inst().V.RefreshDriverEndpoints()
						log.FailOnError(err, "Verify driver end points refresh")
						stepLog = fmt.Sprintf("Validate number of storage nodes after recycling node [%v]", delNode.Name)
						Step(stepLog, func() {
							log.InfoD(stepLog)
							ValidateClusterSize(int64(len(node.GetStorageDriverNodes())))
							PrintPxctlStatus()
						})
					})
				Step(
					fmt.Sprintf("Listing all nodes after recycle a storageless node %s", delNode.Name),
					func() {
						workerNodes := node.GetWorkerNodes()
						for x, wNode := range workerNodes {
							log.Infof("WorkerNode[%d] is: [%s] and volDriverID is [%s]", x, wNode.Name, wNode.VolDriverNodeID)
						}
					})
			}
			// Validating the apps after recycling the StorageLess node
			ValidateApplications(contexts)
			workerNodes := node.GetStorageNodes()
			delNode := workerNodes[0]
			stepLog := fmt.Sprintf("Recycle a storage node: [%s] and validating the drives", delNode.Name)
			Step(
				stepLog,
				func() {
					log.InfoD(stepLog)
					err := Inst().S.DeleteNode(delNode)
					log.FailOnError(err, fmt.Sprintf("Failed to recycle a node [%s]. Error: [%v]", delNode.Name, err))
					waitTime := 10
					if Inst().S.String() == oke.SchedName || Inst().S.String() == anthos.SchedName {
						waitTime = 15 // OKE takes more time to replace the node
					}

					stepLog = fmt.Sprintf("Wait for %d min. to node get replaced by autoscalling group", waitTime)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						time.Sleep(time.Duration(waitTime) * time.Minute)
					})

					err = Inst().S.RefreshNodeRegistry()
					log.FailOnError(err, "Verify node registry refresh")

					err = Inst().V.RefreshDriverEndpoints()
					log.FailOnError(err, "Verify driver end points refresh")
					stepLog = fmt.Sprintf("Validate number of storage nodes after recycling node [%v]", delNode.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						ValidateClusterSize(int64(len(node.GetStorageDriverNodes())))
					})
				})
			Step(fmt.Sprintf("Listing all nodes after recycling a storage node %s", delNode.Name), func() {
				workerNodes := node.GetWorkerNodes()
				for x, wNode := range workerNodes {
					log.Infof("WorkerNode[%d] is: [%s] and volDriverID is [%s]", x, wNode.Name, wNode.VolDriverNodeID)
				}
				PrintPxctlStatus()
			})
			// Validating the apps after recycling the Storage node
			ValidateApplications(contexts)
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{RecycleAllStorageDriverNodes}", func() {

	var contexts []*scheduler.Context

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		wantAfterSuiteSystemCheck = true
	})
	JustBeforeEach(func() {
		StartTorpedoTest("RecycleAllStorageDriverNodes", "Test drives and pools after recycling a all storage driver nodes one by one", nil, 0)
	})

	It("Validating the drives and pools after recycling a node", func() {
		Step("Get the storage and storageless nodes and delete them", func() {
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("recyclenode-%d", i))...)
			}
			ValidateApplications(contexts)

			existingStorageDriverNodes := node.GetStorageDriverNodes()

			for _, existingStorageDriverNode := range existingStorageDriverNodes {
				stepLog := fmt.Sprintf("Recycling node: [%s]", existingStorageDriverNode.Name)
				Step(
					stepLog,
					func() {
						log.InfoD(stepLog)
						err := Inst().S.DeleteNode(existingStorageDriverNode)
						log.FailOnError(err, fmt.Sprintf("Failed to recycle a node [%s]. Error: [%v]", existingStorageDriverNode.Name, err))
						waitTime := 10
						if Inst().S.String() == oke.SchedName || Inst().S.String() == anthos.SchedName {
							waitTime = 15 // OKE takes more time to replace the node
						}

						stepLog = fmt.Sprintf("Wait for %d min. to node get replaced by autoscalling group", waitTime)
						Step(stepLog, func() {
							log.InfoD(stepLog)
							time.Sleep(time.Duration(waitTime) * time.Minute)
						})
						err = Inst().S.RefreshNodeRegistry()
						log.FailOnError(err, "Verify node registry refresh")

						err = Inst().V.RefreshDriverEndpoints()
						log.FailOnError(err, "Verify driver end points refresh")
						stepLog = fmt.Sprintf("Validate number of storage nodes after recycling node [%v]", existingStorageDriverNode.Name)
						Step(stepLog, func() {
							log.InfoD(stepLog)
							ValidateClusterSize(int64(len(node.GetStorageDriverNodes())))
							PrintPxctlStatus()
						})
						nodeIdExists := false
						if len(existingStorageDriverNode.StorageNode.Pools) > 0 {
							newStorageDriverNodes := node.GetStorageDriverNodes()
							for _, newStorageDriverNode := range newStorageDriverNodes {
								if newStorageDriverNode.VolDriverNodeID == existingStorageDriverNode.VolDriverNodeID {
									nodeIdExists = true
									break
								}
							}
							dash.VerifyFatal(nodeIdExists, true, fmt.Sprintf("Node ID [%s] exists after deleting storage node [%v]", existingStorageDriverNode.VolDriverNodeID, existingStorageDriverNode.Name))
						}

					})
			}
			// Validating the apps after recycling the Storage driver node
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			ValidateAndDestroy(contexts, opts)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)

	})
})