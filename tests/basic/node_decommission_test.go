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

		var workerNodes []node.Node
		Step(fmt.Sprintf("get worker nodes"), func() {
			workerNodes = node.GetWorkerNodes()
			dash.VerifyFatal(len(workerNodes) > 0, true, "Verify worker nodes")
		})

		nodeIndexMap := make(map[int]int)
		lenWorkerNodes := len(workerNodes)
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
			nodeToDecommission := workerNodes[nodeIndex]
			stepLog = fmt.Sprintf("decommission node %s", nodeToDecommission.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().S.PrepareNodeToDecommission(nodeToDecommission, Inst().Provisioner)
				dash.VerifyFatal(err, nil, "Validate node decommission preparation")
				err = Inst().V.DecommissionNode(&nodeToDecommission)
				dash.VerifyFatal(err, nil, "Validate node decommission init")
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
					dash.VerifyFatal(decommissioned.(bool), true, "Validate node is decommissioned")
				})
			})
			stepLog = fmt.Sprintf("Rejoin node %s", nodeToDecommission.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err := Inst().V.RejoinNode(&nodeToDecommission)
				dash.VerifyFatal(err, nil, "Validate node rejoin init")
				err = Inst().V.WaitDriverUpOnNode(nodeToDecommission, Inst().DriverStartTimeout)
				dash.VerifyFatal(err, nil, "Validate driver up on reoined node")
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
