package tests

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

const (
	defaultTimeout       = 6 * time.Minute
	defaultRetryInterval = 10 * time.Second
)

func TestDecommissionNode(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_DecommissionNode.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo: DecommissionNode", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{DecommissionNode}", func() {
	testName := "decommissionnode"
	It("has to decommission a node and check if node was decommissioned successfully", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		var workerNodes []node.Node
		Step(fmt.Sprintf("get worker nodes"), func() {
			workerNodes = node.GetWorkerNodes()
			Expect(workerNodes).NotTo(BeEmpty())
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
			Step(fmt.Sprintf("decommission node %s", nodeToDecommission.Name), func() {
				err := Inst().S.PrepareNodeToDecommission(nodeToDecommission, Inst().Provisioner)
				Expect(err).NotTo(HaveOccurred())
				err = Inst().V.DecommissionNode(nodeToDecommission)
				Expect(err).NotTo(HaveOccurred())
				Step(fmt.Sprintf("check if node %s was decommissioned", nodeToDecommission.Name), func() {
					t := func() (interface{}, bool, error) {
						status, err := Inst().V.GetNodeStatus(nodeToDecommission)
						if err != nil && status != nil && *status == api.Status_STATUS_NONE {
							return true, false, nil
						}
						if err != nil {
							return false, true, err
						}
						return false, true, fmt.Errorf("node %s not decomissioned yet", nodeToDecommission.Name)
					}
					decommissioned, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
					Expect(err).NotTo(HaveOccurred())
					Expect(decommissioned.(bool)).To(BeTrue())
				})
			})
			Step(fmt.Sprintf("Rejoin node %s", nodeToDecommission.Name), func() {
				err := Inst().V.RejoinNode(nodeToDecommission)
				Expect(err).NotTo(HaveOccurred())
				err = Inst().V.WaitDriverUpOnNode(nodeToDecommission, Inst().DriverStartTimeout)
				Expect(err).NotTo(HaveOccurred())
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
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
