package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/scheduler/openshift"
	"github.com/portworx/torpedo/pkg/log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

// Sanity test for OCP Recycle method
var _ = Describe("{RecycleOCPNode}", func() {

	var contexts []*scheduler.Context

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		wantAfterSuiteSystemCheck = true
	})
	JustBeforeEach(func() {
		StartTorpedoTest("RecycleOCPNode", "Test drives and pools after recyling a node", nil, 0)
	})

	It("Validing the drives and pools after recyling a node", func() {
		Step("Get the storage and storageless nodes and delete them", func() {
			if Inst().S.String() != openshift.SchedName {
				log.Warnf("Failed: This test is not supported for scheduler: [%s]", Inst().S.String())
				return
			}
			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("crashonenode-%d", i))...)
			}

			ValidateApplications(contexts)
			storagelessNodes, err := Inst().V.GetStoragelessNodes()
			Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to get storageless nodes. Error: [%v]", err))
			if storagelessNodes != nil {
				delNode, err := node.GetNodeByName(storagelessNodes[0].Hostname)
				Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("Failed to get node object using Name. Error: [%v]", err))
				Step(
					fmt.Sprintf("Listing all nodes before recycling a storageless node %s", delNode.Name),
					func() {
						workerNodes := node.GetWorkerNodes()
						for x, wNode := range workerNodes {
							log.Infof("WorkerNode[%d] is: [%s] and volDriverID is [%s]", x, wNode.Name, wNode.VolDriverNodeID)
						}
					})
				Step(
					fmt.Sprintf("Recycle a storageless node and validating the drives: %s", delNode.Name),
					func() {
						err := Inst().S.RecycleNode(delNode)
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to recycle a node [%s]. Error: [%v]", delNode.Name, err))

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
			workerNodes := node.GetStorageDriverNodes()
			delNode := workerNodes[0]
			Step(
				fmt.Sprintf("Recycle a storage node: [%s] and validating the drives", delNode.Name),
				func() {
					err := Inst().S.RecycleNode(delNode)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to recycle a node [%s]. Error: [%v]", delNode.Name, err))
				})
			Step(fmt.Sprintf("Listing all nodes after recycling a storage node %s", delNode.Name), func() {
				workerNodes := node.GetWorkerNodes()
				for x, wNode := range workerNodes {
					log.Infof("WorkerNode[%d] is: [%s] and volDriverID is [%s]", x, wNode.Name, wNode.VolDriverNodeID)
				}
			})
			// Validating the apps after recycling the Storage node
			ValidateApplications(contexts)
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})
