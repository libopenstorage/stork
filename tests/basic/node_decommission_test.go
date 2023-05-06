package tests

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"math/rand"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{DecommissionNode}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("DecommissionNode", "Validate node decommission", nil, 0)
	})
	var contexts []*scheduler.Context

	testName := "decommissionnode"
	stepLog := "has to decommission a node and check if node was decommissioned successfully"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		var storageDriverNodes []node.Node
		Step(fmt.Sprintf("get storage driver nodes"), func() {
			storageDriverNodes = node.GetStorageDriverNodes()
			dash.VerifyFatal(len(storageDriverNodes) > 0, true, "Verify worker nodes")
		})

		nodeIndexMap := make(map[int]int)
		lenWorkerNodes := len(storageDriverNodes)
		chaosLevel := Inst().ChaosLevel
		// chaosLevel in this case is the number of worker nodes to be decommissioned
		// in case of being greater than that, it will assume the total no of worker nodes
		if chaosLevel > lenWorkerNodes {
			chaosLevel = lenWorkerNodes
		}

		Step(fmt.Sprintf("sort nodes randomly according to chaos level %d", chaosLevel), func() {
			for len(nodeIndexMap) != chaosLevel {
				index := rand.Intn(lenWorkerNodes)
				nodeIndexMap[index] = index
			}
		})

		// decommission nodes one at a time according to chaosLevel
		for nodeIndex := range nodeIndexMap {
			nodeToDecommission := storageDriverNodes[nodeIndex]
			nodeToDecommission, err := node.GetNodeByName(nodeToDecommission.Name) //This is required when multiple nodes are decommissioned sequentially
			log.FailOnError(err, fmt.Sprintf("node [%s] not found with name", nodeToDecommission.Name))
			stepLog = fmt.Sprintf("decommission node %s", nodeToDecommission.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().S.PrepareNodeToDecommission(nodeToDecommission, Inst().Provisioner)
				dash.VerifyFatal(err, nil, "Validate node decommission preparation")
				err = Inst().V.DecommissionNode(&nodeToDecommission)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate node [%s] decommission init", nodeToDecommission.Name))
				stepLog = fmt.Sprintf("check if node %s was decommissioned", nodeToDecommission.Name)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					t := func() (interface{}, bool, error) {
						status, err := Inst().V.GetNodeStatus(nodeToDecommission)
						if err != nil {
							return false, true, err
						}
						if *status == api.Status_STATUS_NONE {
							return true, false, nil
						}
						return false, true, fmt.Errorf("node %s not decomissioned yet", nodeToDecommission.Name)
					}
					decommissioned, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
					log.FailOnError(err, "Failed to get decommissioned node status")
					dash.VerifyFatal(decommissioned.(bool), true, fmt.Sprintf("Validate node [%s] is decommissioned", nodeToDecommission.Name))
				})
			})
			stepLog = fmt.Sprintf("Rejoin node %s", nodeToDecommission.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().V.RejoinNode(&nodeToDecommission)
				dash.VerifyFatal(err, nil, "Validate node rejoin init")
				var rejoinedNode *api.StorageNode
				t := func() (interface{}, bool, error) {
					drvNodes, err := Inst().V.GetDriverNodes()
					if err != nil {
						return false, true, err
					}

					for _, n := range drvNodes {
						if n.Hostname == nodeToDecommission.Hostname {
							rejoinedNode = n
							return true, false, nil
						}
					}

					return false, true, fmt.Errorf("node %s not joined yet", nodeToDecommission.Name)
				}
				_, err = task.DoRetryWithTimeout(t, appReadinessTimeout, defaultRetryInterval)
				log.FailOnError(err, fmt.Sprintf("error joining the node [%s]", nodeToDecommission.Name))
				dash.VerifyFatal(rejoinedNode != nil, true, fmt.Sprintf("verify node [%s] rejoined PX cluster", nodeToDecommission.Name))
				err = Inst().S.RefreshNodeRegistry()
				log.FailOnError(err, "error refreshing node registry")
				err = Inst().V.RefreshDriverEndpoints()
				log.FailOnError(err, "error refreshing storage drive endpoints")
				nodeToDecommission = node.Node{}
				for _, n := range node.GetStorageDriverNodes() {
					if n.Name == rejoinedNode.Hostname {
						nodeToDecommission = n
						break
					}
				}
				if nodeToDecommission.Name == "" {
					log.FailOnError(fmt.Errorf("rejoined node not found"), fmt.Sprintf("node [%s] not found in the node registry", rejoinedNode.Hostname))
				}
				err = Inst().V.WaitDriverUpOnNode(nodeToDecommission, Inst().DriverStartTimeout)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate driver up on rejoined node [%s] after rejoining", nodeToDecommission.Name))
			})

		}

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})
