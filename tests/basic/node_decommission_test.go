package tests

import (
	"fmt"
	"github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/pkg/log"
	"math/rand"
	"time"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo/v2"
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

		if Contains(Inst().AppList, "nginx-proxy-deployment") {
			var masterNode node.Node
			stepLog = "setup proxy server necessary for proxy volume"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				masterNodes := node.GetMasterNodes()
				if len(masterNodes) == 0 {
					log.FailOnError(fmt.Errorf("no master nodes found"), "Identifying master node of proxy server failed")
				}

				masterNode = masterNodes[0]
				err = SetupProxyServer(masterNode)
				log.FailOnError(err, fmt.Sprintf("error setting up proxy server on master node %s", masterNode.Name))

			})
			stepLog = "create storage class for proxy volumes"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				addresses := masterNode.Addresses
				if len(addresses) == 0 {
					log.FailOnError(fmt.Errorf("no addresses found for node [%s]", masterNode.Name), "error getting ip addresses ")
				}
				err = CreateNFSProxyStorageClass("portworx-proxy-volume-volume", addresses[0], "/exports/testnfsexportdir")
				log.FailOnError(err, "error creating storage class for proxy volume")
			})
		}
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

			//checking node status before decommission
			status, err := Inst().V.GetNodeStatus(nodeToDecommission)
			log.FailOnError(err, "error checking node [%s] status", nodeToDecommission.Name)
			if *status != api.Status_STATUS_OK {
				continue
			}

			nodeToDecommission, err = node.GetNodeByName(nodeToDecommission.Name) //This is required when multiple nodes are decommissioned sequentially
			log.FailOnError(err, fmt.Sprintf("node [%s] not found with name", nodeToDecommission.Name))
			stepLog = fmt.Sprintf("decommission node %s", nodeToDecommission.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				var suspendedScheds []*v1alpha1.VolumeSnapshotSchedule
				defer func() {
					if len(suspendedScheds) > 0 {
						for _, sched := range suspendedScheds {
							makeSuspend := false
							sched.Spec.Suspend = &makeSuspend
							_, err := storkops.Instance().UpdateSnapshotSchedule(sched)
							log.FailOnError(err, "error resuming volumes snapshot schedule for volume [%s] ", sched.Name)
						}
					}
				}()
				err = PrereqForNodeDecomm(nodeToDecommission, suspendedScheds)
				log.FailOnError(err, "error performing prerequisites for node decommission")

				err := Inst().S.PrepareNodeToDecommission(nodeToDecommission, Inst().Provisioner)
				dash.VerifyFatal(err, nil, "Validate node decommission preparation")
				err = Inst().V.DecommissionNode(&nodeToDecommission)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate node [%s] decommission init", nodeToDecommission.Name))
				stepLog = fmt.Sprintf("check if node %s was decommissioned", nodeToDecommission.Name)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					result := false
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
					result = decommissioned.(bool)

					dash.VerifyFatal(result, true, fmt.Sprintf("Validate node [%s] is decommissioned", nodeToDecommission.Name))

				})
			})
			stepLog = fmt.Sprintf("Rejoin node %s", nodeToDecommission.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				//reboot required to remove encrypted dm devices if any
				err := Inst().N.RebootNode(nodeToDecommission, node.RebootNodeOpts{
					Force: true,
					ConnectionOpts: node.ConnectionOpts{
						Timeout:         defaultCommandTimeout,
						TimeBeforeRetry: defaultRetryInterval,
					},
				})
				log.FailOnError(err, fmt.Sprintf("error rebooting node %s", nodeToDecommission.Name))
				err = Inst().V.RejoinNode(&nodeToDecommission)
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
		PerformSystemCheck()

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{KvdbDecommissionNode}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("KvdbDecommissionNode", "Validate decommission of kvdb nodes", nil, 0)
	})
	var contexts []*scheduler.Context

	testName := "kvdbdecommissionnode"
	stepLog := "has to decommission a kvdb node and check if node was decommissioned successfully"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		var kvdbNodes []KvdbNode
		var newKVDBNodes []KvdbNode
		var err error
		Step(fmt.Sprintf("get kvdb nodes"), func() {
			kvdbNodes, err = GetAllKvdbNodes()
			log.FailOnError(err, "Failed to get list of KVDB nodes from the cluster")
			dash.VerifyFatal(len(kvdbNodes) == 3, true, "Verify kvdb nodes")
		})

		// decommission nodes one at a time according to chaosLevel
		for _, kvdbNode := range kvdbNodes {
			nodeToDecommission, err := node.GetNodeDetailsByNodeID(kvdbNode.ID)
			log.FailOnError(err, fmt.Sprintf("error getting node with id: %s", kvdbNode.ID))
			stepLog = fmt.Sprintf("decommission node %s", nodeToDecommission.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				var suspendedScheds []*v1alpha1.VolumeSnapshotSchedule
				defer func() {
					if len(suspendedScheds) > 0 {
						for _, sched := range suspendedScheds {
							makeSuspend := false
							sched.Spec.Suspend = &makeSuspend
							_, err := storkops.Instance().UpdateSnapshotSchedule(sched)
							log.FailOnError(err, "error resuming volumes snapshot schedule for volume [%s]", sched.Name)
						}
					}
				}()
				err = PrereqForNodeDecomm(nodeToDecommission, suspendedScheds)
				log.FailOnError(err, "error performing prerequisites for node decommission")
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
			err = Inst().S.RefreshNodeRegistry()
			log.FailOnError(err, "error refreshing node registry")
			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing storage drive endpoints")

			t := func() (interface{}, bool, error) {

				newKVDBNodes, err = GetAllKvdbNodes()
				log.FailOnError(err, "Failed to get list of KVDB nodes from the cluster")
				if err != nil {
					return false, true, err
				}

				if len(newKVDBNodes) == 3 {
					return true, false, nil
				}

				return false, true, fmt.Errorf("current  number of KVDB nodes : %d", len(newKVDBNodes))
			}
			_, err = task.DoRetryWithTimeout(t, 4*time.Minute, defaultRetryInterval)
			dash.VerifyFatal(len(newKVDBNodes) == 3, true, "Verify kvdb nodes are updated")

			isLeaderHealthy := false
			for _, nKVDBNode := range newKVDBNodes {
				dash.VerifyFatal(nKVDBNode.IsHealthy, true, fmt.Sprintf("verify kvdb node %s is healthy", nKVDBNode.ID))
				if nKVDBNode.Leader && nKVDBNode.IsHealthy {
					isLeaderHealthy = true
				}
			}
			dash.VerifyFatal(isLeaderHealthy, true, "verify kvdb leader node exists and healthy")

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
						log.Infof("checking for node %s", n.Hostname)
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
