package tests

import (
	"fmt"
	"github.com/portworx/sched-ops/k8s/operator"
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
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("recyclenode-%d", i))...)
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

var _ = Describe("{AddOCPStorageNode}", func() {

	var contexts []*scheduler.Context

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		wantAfterSuiteSystemCheck = true
	})
	JustBeforeEach(func() {
		StartTorpedoTest("AddOCPStorageNode", "Add a new storage node the OCP cluster", nil, 0)
	})
	stepLog := "Validating the drives and pools after adding new storage node"

	It(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "Add storage node"

		Step(stepLog, func() {
			if Inst().S.String() != openshift.SchedName {
				log.Warnf("Failed: This test is not supported for scheduler: [%s]", Inst().S.String())
				return
			}
			log.InfoD(stepLog)
			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("addstnode-%d", i))...)
			}

			ValidateApplications(contexts)

			var numOfStorageNodes int
			Step(stepLog, func() {
				log.InfoD(stepLog)
				stc, err := Inst().V.GetDriver()

				log.FailOnError(err, "error getting volume driver")
				maxStorageNodesPerZone := *stc.Spec.CloudStorage.MaxStorageNodesPerZone
				numOfStorageNodes = len(node.GetStorageNodes())
				log.Infof("maxStorageNodesPerZone %d", int(maxStorageNodesPerZone))
				log.Infof("numOfStorageNodes %d", numOfStorageNodes)

				var updatedMaxStorageNodesPerZone uint32 = 0
				if int(maxStorageNodesPerZone) == numOfStorageNodes {
					//increase max per zone
					updatedMaxStorageNodesPerZone = maxStorageNodesPerZone + 1
				}

				if int(maxStorageNodesPerZone) < numOfStorageNodes {
					//updating max per zone
					updatedMaxStorageNodesPerZone = uint32(numOfStorageNodes)
				}
				if updatedMaxStorageNodesPerZone != 0 {

					stc.Spec.CloudStorage.MaxStorageNodesPerZone = &updatedMaxStorageNodesPerZone
					log.InfoD("updating maxStorageNodesPerZone from %d to %d", maxStorageNodesPerZone, updatedMaxStorageNodesPerZone)
					pxOperator := operator.Instance()
					_, err = pxOperator.UpdateStorageCluster(stc)
					log.FailOnError(err, "error updating storage cluster")

				}
				//Scaling the cluster by one node
				expReplicas := len(node.GetWorkerNodes()) + 1
				log.InfoD("scaling up the cluster to replicas %d", expReplicas)
				err = Inst().S.ScaleCluster(expReplicas)

				dash.VerifyFatal(err, nil, fmt.Sprintf("verify cluster successfully scaled to %d", expReplicas))

			})

			stepLog = "validate PX on all nodes after cluster scale up"

			Step(stepLog, func() {
				log.InfoD(stepLog)
				nodes := node.GetWorkerNodes()
				for _, n := range nodes {
					log.InfoD("Check PX status on %v", n.Name)
					err := Inst().V.WaitForPxPodsToBeUp(n)
					dash.VerifyFatal(err, nil, fmt.Sprintf("verify px is up on  node %s", n.Name))
				}
			})

			err := Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")

			updatedStorageNodesCount := len(node.GetStorageNodes())
			dash.VerifySafely(numOfStorageNodes+1, updatedStorageNodesCount, "verify new storage node is added")
			// Validating the apps after recycling the Storage node
			ValidateApplications(contexts)
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{AddOCPStoragelessNode}", func() {

	var contexts []*scheduler.Context

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		wantAfterSuiteSystemCheck = true
	})
	JustBeforeEach(func() {
		StartTorpedoTest("AddOCPStoragelessNode", "Add a new storageless node the OCP cluster", nil, 0)
	})
	stepLog := "Validating px after adding new storageless node"

	It(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "Add storageless node"

		Step(stepLog, func() {
			if Inst().S.String() != openshift.SchedName {
				log.Warnf("Failed: This test is not supported for scheduler: [%s]", Inst().S.String())
				return
			}
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

				if int(maxStorageNodesPerZone) > numOfStorageNodes {
					//updating max per zone
					updatedMaxStorageNodesPerZone := uint32(numOfStorageNodes)
					stc.Spec.CloudStorage.MaxStorageNodesPerZone = &updatedMaxStorageNodesPerZone
					log.InfoD("updating maxStorageNodesPerZone from %d to %d", maxStorageNodesPerZone, updatedMaxStorageNodesPerZone)
					pxOperator := operator.Instance()
					_, err = pxOperator.UpdateStorageCluster(stc)
					log.FailOnError(err, "error updating storage cluster")

				}
				//Scaling the cluster by one node
				expReplicas := len(node.GetWorkerNodes()) + 1
				log.InfoD("scaling up the cluster to replicas %d", expReplicas)
				err = Inst().S.ScaleCluster(expReplicas)

				dash.VerifyFatal(err, nil, fmt.Sprintf("verify cluster successfully scaled to %d", expReplicas))

			})

			stepLog = "validate PX on all nodes after cluster scale up"

			Step(stepLog, func() {
				log.InfoD(stepLog)
				nodes := node.GetWorkerNodes()
				for _, n := range nodes {
					log.InfoD("Check PX status on %v", n.Name)
					err := Inst().V.WaitForPxPodsToBeUp(n)
					dash.VerifyFatal(err, nil, fmt.Sprintf("verify px is up on  node %s", n.Name))
				}
			})

			err := Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")

			updatedStoragelessNodesCount := len(node.GetStorageLessNodes())
			dash.VerifySafely(numOfStoragelessNodes+1, updatedStoragelessNodesCount, "verify new storageless node is added")
			// Validating the apps after recycling the Storage node
			ValidateApplications(contexts)
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})
