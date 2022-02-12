package tests

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/portworx/torpedo/tests"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	cmdRetry             = 5 * time.Second
	cmdTimeout           = 1 * time.Minute
	rebootTimeout        = 5 * time.Minute
	rebootRetry          = 10 * time.Second
	testConnTimeout      = 15 * time.Minute
	offlineClientTimeout = 15 * time.Minute // as defined in ref.go
	exportPathPrefix     = "/var/lib/osd/pxns/"
)

var _ = Describe("{NFSServerFailover}", func() {
	var contexts []*scheduler.Context
	It("has to setup, validate, failover, make sure pods on old and new server got restarted, and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nfsserverfailover-%d", i))...)
		}

		ValidateApplications(contexts)

		for _, ctx := range contexts {
			var nodeReplicaMap map[string]bool
			var volume *volume.Volume
			Step("disable scheduling on non replica nodes", func() {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())
				volume = vols[0]
				nodeReplicaMap = getReplicaNodeIDs(volume)
				// make sure there are 2 replicas
				Expect(len(nodeReplicaMap)).To(Equal(2))
				allNodes := node.GetWorkerNodes()
				for _, node := range allNodes {
					if !nodeReplicaMap[node.VolDriverNodeID] {
						Inst().S.DisableSchedulingOnNode(node)
					}
				}
			})

			// scale down and then scale up the app, so that pods are only scheduled on replica nodes
			Step(fmt.Sprintf("scale down app: %s to 0 ", ctx.App.Key), func() {
				applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
				Expect(err).NotTo(HaveOccurred())
				for name := range applicationScaleUpMap {
					applicationScaleUpMap[name] = int32(0)
				}
				err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
				Expect(err).NotTo(HaveOccurred())
			})

			Step(fmt.Sprintf("scale up app: %s to 2, and re-enable scheduling on all nodes", ctx.App.Key), func() {
				applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
				Expect(err).NotTo(HaveOccurred())
				for name := range applicationScaleUpMap {
					applicationScaleUpMap[name] = int32(2)
				}
				err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
				Expect(err).NotTo(HaveOccurred())
				ValidateApplications(contexts)
			})

			Step("fail over nfs server, and make sure the pod on server gets restarted", func() {
				oldServer, err := Inst().V.GetNodeForVolume(volume, cmdTimeout, cmdRetry)
				Expect(err).NotTo(HaveOccurred())
				logrus.Infof("old nfs server %v [%v]", oldServer.SchedulerNodeName, oldServer.Addresses[0])
				pods, err := core.Instance().GetPodsUsingPV(volume.ID)
				Expect(err).NotTo(HaveOccurred())
				var oldPodOnOldServer corev1.Pod
				for _, pod := range pods {
					if pod.Spec.NodeName == oldServer.Name {
						oldPodOnOldServer = pod
					}
				}
				// make sure there is a pod running on the old nfs server
				Expect(oldPodOnOldServer.Name).NotTo(Equal(""))
				logrus.Infof("pod on old server %v, creation time %v", oldPodOnOldServer.Name, oldPodOnOldServer.CreationTimestamp)

				timestampBeforeFailOver := time.Now()
				err = Inst().V.StopDriver([]node.Node{*oldServer}, false, nil)
				Expect(err).NotTo(HaveOccurred())
				err = Inst().V.WaitDriverDownOnNode(*oldServer)
				Expect(err).NotTo(HaveOccurred())
				logrus.Infof("stopped px on nfs server node %v [%v]", oldServer.SchedulerNodeName, oldServer.Addresses[0])

				var newServer *node.Node

				for i := 0; i < 60; i++ {
					err := Inst().V.RefreshDriverEndpoints()
					Expect(err).NotTo(HaveOccurred())
					server, err := Inst().V.GetNodeForVolume(volume, cmdTimeout, cmdRetry)
					// there could be intermittent error here
					if err != nil {
						logrus.Infof("Failed to get node for volume. Error: %v", err)
					} else {
						if server.Id != oldServer.Id {
							logrus.Infof("nfs server failed over, new nfs server is %s [%s]", server.SchedulerNodeName, server.Addresses[0])
							newServer = server
							break
						}
					}
					time.Sleep(10 * time.Second)
				}
				// make sure nfs server failed over
				Expect(newServer).NotTo(BeNil())
				logrus.Infof("new nfs server is %v [%v]", newServer.SchedulerNodeName, newServer.Addresses[0])

				logrus.Infof("start px on old nfs server Id %v, Name %v", oldServer.Id, oldServer.Name)
				Inst().V.StartDriver(*oldServer)
				err = Inst().V.WaitDriverUpOnNode(*oldServer, Inst().DriverStartTimeout)
				Expect(err).NotTo(HaveOccurred())
				logrus.Infof("px is up on old nfs server Id %v, Name %v", oldServer.Id, oldServer.Name)

				ValidateApplications(contexts)

				// make sure the pods on both old and new server are restarted
				pods, err = core.Instance().GetPodsUsingPV(volume.ID)
				Expect(err).NotTo(HaveOccurred())
				podRestartedOnOldServer := false
				podRestartedOnNewServer := false
				for _, pod := range pods {
					if pod.Spec.NodeName == oldServer.Name {
						logrus.Infof("pod on old server %v, creation time %v", oldPodOnOldServer.Name, oldPodOnOldServer.CreationTimestamp)
						logrus.Infof("After failover, pod on old server %v, creation time %v", pod.Name, pod.CreationTimestamp)
						Expect(pod.CreationTimestamp.After(timestampBeforeFailOver)).To(BeTrue())
						podRestartedOnOldServer = true
					}
					if pod.Spec.NodeName == newServer.Name {
						logrus.Infof("After failover, pod on new server %v, creation time %v", pod.Name, pod.CreationTimestamp)
						Expect(pod.CreationTimestamp.After(timestampBeforeFailOver)).To(BeTrue())
						podRestartedOnNewServer = true
					}
				}

				Expect(podRestartedOnOldServer).To(BeTrue())
				Expect(podRestartedOnNewServer).To(BeTrue())

				// re-enable scheduling on non replica nodes
				for _, node := range node.GetWorkerNodes() {
					if !nodeReplicaMap[node.VolDriverNodeID] {
						Inst().S.EnableSchedulingOnNode(node)
					}
				}
			})
		}

		for _, ctx := range contexts {
			TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
		}
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

// Below tests uses test-sharedv4 app https://github.com/portworx/test-sharedv4 .
//
// The test-sharedv4 app has the following properties:
// - uses node anti-affinity to ensure that each pod is on a different node
// - one sharedv4 svc volume is exposed to all the pods
// - each pod creates its own file named same as the pod name and keeps incrementing a counter in that file
//
// Struct appCounter below captures the state of a pod that is part of that app.
type appCounter struct {
	// podName is same as the name of the file that this pod is storing its counter into.
	// Since this info is gathered from the volume, the pod may or may not exist currently.
	podName string

	// counter value in the file
	counter int

	// active, if true, indicates that the pod is actively incrementing the counter in the file
	active bool
}

type failoverMethod interface {
	doFailover(attachedNode *node.Node)
	getExpectedPodDeletions() []int
	String() string
}

type failoverMethodRestartVolDriver struct {
}

func (fm *failoverMethodRestartVolDriver) doFailover(attachedNode *node.Node) {
	restartVolumeDriverOnNode(attachedNode)
}

func (fm *failoverMethodRestartVolDriver) String() string {
	return "restart volume driver"
}

func (fm *failoverMethodRestartVolDriver) getExpectedPodDeletions() []int {
	// 2 pods, one on the old NFS server and one on the new NFS server should be deleted.
	return []int{2}
}

type failoverMethodReboot struct {
}

func (fm *failoverMethodReboot) doFailover(attachedNode *node.Node) {
	rebootNodeAndWaitForReady(attachedNode)
}

func (fm *failoverMethodReboot) String() string {
	return "reboot"
}

func (fm *failoverMethodReboot) getExpectedPodDeletions() []int {
	// 1 or 2 pods may get deleted depending on what kubelet does on the rebooted node.
	// The kubelet could set up the mount for the same pod or it could create a new pod.
	return []int{1, 2}
}

var _ = Describe("{Shared4 service apps}", func() {
	var testrailID, runID int
	var contexts, testSv4Contexts []*scheduler.Context
	var workers []node.Node
	var numPods int
	var namespacePrefix string

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)

		// Set up all apps
		contexts = nil
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s%d", namespacePrefix, i))...)
		}

		// Skip the test if there are no test-sharedv4 apps
		testSv4Contexts = getTestSv4Contexts(contexts)
		if len(testSv4Contexts) == 0 {
			Skip("No test-sv4-svc apps were found")
		}
		workers = node.GetWorkerNodes()
		numPods = len(workers)

		Step("scale the test-sv4-svc apps so that one pod runs on each worker node", func() {
			scaleApps(testSv4Contexts, numPods)
		})
		ValidateApplications(contexts)
	})

	Context("{Shared4SvcFailoverFailback}", func() {
		var numFailovers int
		var fm failoverMethod

		JustBeforeEach(func() {
			var err error

			//set HA level to 2 to verify failover and then failback to the same node
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("set HA level to 2 for app %s's volume", ctx.App.Key), func() {
					vols, err := Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					setHALevel(vols[0], 2)
					// ValidateContext() will fail with error "volume has invalid repl value. Expected:3 Actual:2"
					// without the line below.
					ctx.SkipVolumeValidation = true
				})
			}
			numFailovers = 2
			numFailoversStr := os.Getenv("SHAREDV4_SVC_NUM_FAILOVERS")
			if numFailoversStr != "" {
				numFailovers, err = strconv.Atoi(numFailoversStr)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		// Use the "shared behaviors" pattern described here: https://onsi.github.io/ginkgo/#shared-behaviors
		// to run the same spec (failover/failback) in 2 different contexts (stop vol driver, reboot).
		testFailoverFailback := func() {
			Specify("no I/O disruption after failover and failback", func() {
				for _, ctx := range testSv4Contexts {
					Step(fmt.Sprintf("test failover and failback for app %s", ctx.App.Key), func() {
						vols, err := Inst().S.GetVolumes(ctx)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(vols)).To(Equal(1))
						vol := vols[0]

						apiVol, err := Inst().V.InspectVolume(vol.ID)
						Expect(err).NotTo(HaveOccurred())

						// Since the HA level is 2, we can verify failover and failback by repeating the steps below.
						for i := 0; i < numFailovers; i++ {
							var countersBefore, countersAfter map[string]appCounter
							var attachedNodeBefore, attachedNodeAfter *node.Node

							counterCollectionInterval := 3 * time.Duration(numPods) * time.Second
							failoverLog := fmt.Sprintf("failover #%d by %s for app %s", i, fm, ctx.App.Key)

							Step(fmt.Sprintf("get the attached node for volume %s before %s", vol.ID, failoverLog),
								func() {
									attachedNodeBefore, err = Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
									Expect(err).NotTo(HaveOccurred())
									logrus.Infof("volume %v (%v) is attached to node %v before %s",
										vol.ID, apiVol.Id, attachedNodeBefore.Name, failoverLog)
								})

							Step(fmt.Sprintf("get counters from node %v before %s", attachedNodeBefore.Name, failoverLog),
								func() {
									countersBefore = getAppCounters(apiVol, attachedNodeBefore, counterCollectionInterval)
								})

							Step(fmt.Sprintf("failover #%d by %s", i, fm),
								func() {
									fm.doFailover(attachedNodeBefore)
								})

							Step(fmt.Sprintf("validate app after %s", failoverLog),
								func() {
									ValidateContext(ctx)
								})

							Step(fmt.Sprintf("get counter values after %s", failoverLog),
								func() {
									attachedNodeAfter, err = Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
									Expect(err).NotTo(HaveOccurred())
									Expect(attachedNodeAfter.Name).NotTo(Equal(attachedNodeBefore.Name))
									logrus.Infof("volume %v (%v) is attached to node %v after %s",
										vol.ID, apiVol.Id, attachedNodeAfter.Name, failoverLog)
									countersAfter = getAppCounters(apiVol, attachedNodeAfter, counterCollectionInterval)
								})

							Step(fmt.Sprintf("validate no I/O disruption after %s", failoverLog),
								func() {
									numDeletions := fm.getExpectedPodDeletions()
									validateAppCounters(ctx, countersBefore, countersAfter, numPods, numDeletions)
									validateAppLogs(ctx, numPods)
									validateExports(apiVol, attachedNodeBefore, attachedNodeAfter)
								})
						}
					})
				}
			})
		}

		// test failover/failback by restarting the volume driver
		Context("{Shared4SvcRestartVolDriver}", func() {
			BeforeEach(func() {
				namespacePrefix = "restartvoldriver-"
				fm = &failoverMethodRestartVolDriver{}
				testrailID = 54374
			})
			testFailoverFailback()
		})

		// test failover/failback by rebooting the node
		Context("{Shared4SvcRebootNode}", func() {
			BeforeEach(func() {
				namespacePrefix = "rebootnode-"
				fm = &failoverMethodReboot{}
				testrailID = 54385
			})
			testFailoverFailback()
		})
	})

	// Bring PX down on the node where the volume is attached. Verify that pods on client nodes unmount the volume
	// and teardown successfully
	Context("{Sharedv4ClientTeardownWhenServerOffline}", func() {
		BeforeEach(func() {
			testrailID = 54780
			namespacePrefix = "clientteardown-"

		})

		JustBeforeEach(func() {
			Step("change the sharedv4 failover strategy to normal", func() {
				updateFailoverStrategyForApps(testSv4Contexts, api.Sharedv4FailoverStrategy_NORMAL)
			})
		})

		It("has to schedule apps, stop volume driver on node where volume is attached, teardown the application", func() {
			var attachedNode *node.Node
			for _, ctx := range testSv4Contexts {
				Step(
					fmt.Sprintf("stop the volume driver on attached node and verify app %s teardown succeeds", ctx.App.Key),
					func() {
						vols, err := Inst().S.GetVolumes(ctx)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(vols)).To(Equal(1))
						vol := vols[0]

						attachedNode, err = Inst().V.GetNodeForVolume(
							vol,
							cmdTimeout,
							cmdRetry,
						)
						Expect(err).NotTo(HaveOccurred())

						Step(fmt.Sprintf("stopping volume driver on node %s", attachedNode.Name),
							func() {
								StopVolDriverAndWait([]node.Node{*attachedNode})
							},
						)

						Step(fmt.Sprintf("scale down app %s to 0", ctx.App.Key), func() {
							scaleApp(ctx, 0)
						})

						Step(fmt.Sprintf("ensure client pods have terminated for app %s", ctx.App.Key),
							func() {
								err = Inst().S.SelectiveWaitForTermination(
									ctx,
									Inst().DestroyAppTimeout,
									[]node.Node{*attachedNode},
								)
								Expect(err).NotTo(HaveOccurred())
							},
						)

						Step(fmt.Sprintf("ensure volume is detached for app %s", ctx.App.Key),
							func() {
								apiVol, err := Inst().V.InspectVolume(vol.ID)
								Expect(err).NotTo(HaveOccurred(), "failed in inspect volume: %v", err)
								Expect(len(apiVol.AttachedOn) == 0).To(BeTrue(), "expected volume %v to be detached", vol.Name)
							},
						)

						Step(fmt.Sprintf("starting volume driver on node %s", attachedNode.Name),
							func() {
								StartVolDriverAndWait([]node.Node{*attachedNode})
							},
						)

						Step(fmt.Sprintf("scale up app %s to %d", ctx.App.Key, numPods), func() {
							scaleApp(ctx, numPods)
						})
						ValidateApplications([]*scheduler.Context{ctx})
					})
			}
		})
	})

	// Bring PX down on the client nodes one by one. Verify that there is no I/O disruption.
	Context("{Shared4SvcClientRestart}", func() {
		BeforeEach(func() {
			testrailID = 54383
			namespacePrefix = "clientrestart-"
		})

		It("has to verify no I/O disruption for test-sv4-svc apps after PX restart on NFS client node", func() {
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("restart client nodes one by one and verify I/O for app %s", ctx.App.Key), func() {
					var attachedNode *node.Node

					vols, err := Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(vols)).To(Equal(1))
					vol := vols[0]

					apiVol, err := Inst().V.InspectVolume(vol.ID)
					Expect(err).NotTo(HaveOccurred())

					attachedNode, err = Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
					Expect(err).NotTo(HaveOccurred())
					logrus.Infof("volume %v (%v) is attached to node %v", vol.ID, apiVol.Id, attachedNode.Name)

					// restart PX on the client nodes one at a time
					for _, worker := range workers {
						var countersBefore, countersAfter map[string]appCounter

						// we don't expect a failover
						validateAttachedNode(vol, attachedNode)

						if worker.Name == attachedNode.Name {
							continue
						}
						restartLog := fmt.Sprintf(
							"restarting the volume driver on node %s where app %s's client pod is running",
							worker.Name, ctx.App.Key)

						Step(fmt.Sprintf("get counters from node %s before %s", attachedNode.Name, restartLog),
							func() {
								countersBefore = getAppCounters(apiVol, attachedNode, 3*time.Duration(numPods)*time.Second)
							})

						Step(restartLog,
							func() {
								restartVolumeDriverOnNode(&worker)
							})

						Step(fmt.Sprintf("validate app after %s", restartLog),
							func() {
								ValidateContext(ctx)
								validateAttachedNode(vol, attachedNode)
							})

						Step(fmt.Sprintf("get counters from node %s after %s", attachedNode.Name, restartLog),
							func() {
								countersAfter = getAppCounters(apiVol, attachedNode, 3*time.Duration(numPods)*time.Second)
							})

						Step(fmt.Sprintf("validate no I/O disruption after %s", restartLog),
							func() {
								validateAppCounters(ctx, countersBefore, countersAfter, numPods, nil /* no pod deletions */)
								validateAppLogs(ctx, numPods)
							})
					}
				})
			}
		})
	})

	// Scale app down and verify that the server removed the export for client node. Then,
	// scale the app up again and verify that the server added the export back.
	Context("{Shared4SvcUnexportExport}", func() {
		BeforeEach(func() {
			testrailID = 54776
			namespacePrefix = "unexport-"
		})

		It("has to verify that the server unexports and re-exports volume to the client node "+
			"after pod goes away and comes back", func() {
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("scale the deployment down and then up for app %s", ctx.App.Key), func() {
					var attachedNode *node.Node
					var nodeWithNoPod *node.Node

					vols, err := Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(vols)).To(Equal(1))
					vol := vols[0]

					apiVol, err := Inst().V.InspectVolume(vol.ID)
					Expect(err).NotTo(HaveOccurred())

					attachedNode, err = Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
					Expect(err).NotTo(HaveOccurred())
					logrus.Infof("volume %v (%v) is attached to node %v", vol.ID, apiVol.Id, attachedNode.Name)

					exportsBeforeScaleDown := getExportsOnNode(apiVol, attachedNode)

					// Scale down the app by 1 and check if the pod on NFS client node got removed.
					// If not, scale down by 1 more. It should not take more than 2 attempts since
					// there is no more than 1 pod running on each node.
				Outer:
					for i := 1; i < 3; i++ {
						scaleApp(ctx, len(workers)-i)
						ValidateContext(ctx)

						pods, err := core.Instance().GetPodsUsingPV(vol.ID)
						Expect(err).NotTo(HaveOccurred())

						podsByWorker := map[string]corev1.Pod{}
						for _, pod := range pods {
							podsByWorker[pod.Spec.NodeName] = pod
						}
						for _, worker := range workers {
							if _, ok := podsByWorker[worker.Name]; !ok && worker.Name != attachedNode.Name {
								nodeWithNoPod = &worker
								break Outer
							}
						}
					}
					Expect(nodeWithNoPod).NotTo(BeNil(), "did not find NFS client node whose pod was removed")

					// IPs that were exported before scaling down the app that we expect to remain exported
					var otherClients []string
					for _, export := range exportsBeforeScaleDown {
						if export != nodeWithNoPod.DataIp {
							otherClients = append(otherClients, export)
						}
					}
					Expect(otherClients).ToNot(BeEmpty())

					// We don't expect a failover
					validateAttachedNode(vol, attachedNode)
					exportsAfterScaleDown := getExportsOnNode(apiVol, attachedNode)

					Expect(exportsBeforeScaleDown).To(ContainElement(nodeWithNoPod.DataIp),
						"client IP not found in the exports before scaling down the app")

					Expect(exportsAfterScaleDown).ToNot(ContainElement(nodeWithNoPod.DataIp),
						"client IP still present in the exports after scaling down the app")

					Expect(exportsAfterScaleDown).To(ContainElements(otherClients),
						"different client IP unexpectedly disappeared from the exports after scaling down the app")

					// scale the app up again to have one pod on each worker node
					scaleApp(ctx, numPods)
					ValidateContext(ctx)

					// We don't expect a failover
					validateAttachedNode(vol, attachedNode)
					exportsAfterScaleUp := getExportsOnNode(apiVol, attachedNode)
					Expect(exportsAfterScaleUp).Should(ContainElement(nodeWithNoPod.DataIp),
						"client IP did not re-appear in the exports after scaling up the app")
				})
			}
		})
	})

	// Stop PX on the client node and wait for the server to remove the export for client.
	// Then, do a failover and restart PX on the client node.
	Context("{Shared4SvcClientOfflineTooLong}", func() {
		BeforeEach(func() {
			testrailID = 54778
			namespacePrefix = "clientoffline-"
		})

		JustBeforeEach(func() {
			//set HA level to 2 to predict which node we fail over to (needed to avoid kvdb loss)
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("set HA level to 2 for app %s's volume", ctx.App.Key), func() {
					vols, err := Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					setHALevel(vols[0], 2)
					// ValidateContext() will fail with error "volume has invalid repl value. Expected:3 Actual:2"
					// without the line below.
					ctx.SkipVolumeValidation = true
				})
			}
		})

		It("has to stop PX on the client node long enough for server to remove the export then bring the node back", func() {
			for _, ctx := range testSv4Contexts {
				var vol *volume.Volume
				var apiVol *api.Volume
				var err error
				var replicaNodeIDs map[string]bool
				var attachedNode, clientNode *node.Node
				var failover bool

				Step(fmt.Sprintf("get replica nodes for app %s's volume", ctx.App.Key), func() {
					vols, err := Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(vols)).To(Equal(1))
					vol = vols[0]
					replicaNodeIDs = getReplicaNodeIDs(vol)
					Expect(len(replicaNodeIDs)).To(Equal(2))
				})

				Step(fmt.Sprintf("stop PX on a client node for app %s and wait for export gone", ctx.App.Key), func() {
					// We need at least 5 nodes to do a failover after stopping PX on the client node.
					// Otherwise, PX will lose quorum. (This assumes that there are 3 internal kvdb nodes.)
					failover = len(workers) >= 5

					apiVol, err = Inst().V.InspectVolume(vol.ID)
					Expect(err).NotTo(HaveOccurred())

					attachedNode, err = Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
					Expect(err).NotTo(HaveOccurred())
					logrus.Infof("volume %v (%v) is attached to node %v", vol.ID, apiVol.Id, attachedNode.Name)

					// When failover=true, we will be stopping PX on 2 nodes: the node where volume is attached and
					// a node where a pod with NFS mount (client node) is running.
					//
					// Choose a client node such that:
					//
					// - the pod running on the chosen node survives the failover that we are going to induce. The
					//   pod on new NFS server gets killed during failover. Therefore, we avoid node on which the volume
					//   has replica since that node could become a new NFS server.
					//
					// - KVDB does not lose quorum. There are 3 internal kvdb nodes. We don't want to stop
					//   2 of these at the same time. If volume is attached to a node which is also a kvdb node,
					//   choose a non-kvdb node as the client node.
					for _, worker := range workers {
						// skip replica node since the pod on that node will get terminated.
						if _, ok := replicaNodeIDs[worker.VolDriverNodeID]; ok {
							continue
						}
						// can't stop 2 metadata nodes
						if failover && attachedNode.IsMetadataNode && worker.IsMetadataNode {
							continue
						}
						clientNode = &worker
						break
					}
					Expect(clientNode).ToNot(BeNil())
					logrus.Infof("chose client node %v to stop PX on", clientNode.Name)

					exports := getExportsOnNode(apiVol, attachedNode)
					Expect(exports).Should(ContainElement(clientNode.DataIp),
						"client IP not found in the exports before stopping PX on the client node")

					logrus.Infof("stopping volume driver on node %s", clientNode.Name)
					StopVolDriverAndWait([]node.Node{*clientNode})

					// TODO: offlineClientTimeout = 15 * time.Minute in ref.go
					// need to make it configurable so that we don't have to sleep for that long in the test
					logrus.Infof("sleep to allow the server to remove client %v's export", clientNode.Name)
					time.Sleep(offlineClientTimeout + 1*time.Minute)

					// We don't expect a failover
					validateAttachedNode(vol, attachedNode)

					// Verify server removed the export
					exports = getExportsOnNode(apiVol, attachedNode)
					Expect(exports).ShouldNot(ContainElement(clientNode.DataIp),
						"client IP still present in the exports after stopping PX on client node for a long time")
					// TODO: verify that the annotations on the k8s service are gone
				})

				Step(fmt.Sprintf("do a failover if possible for app %s", ctx.App.Key), func() {
					if failover {
						restartVolumeDriverOnNode(attachedNode)
						attachedNodeAfter, err := Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
						Expect(err).NotTo(HaveOccurred())
						Expect(attachedNodeAfter.Name).NotTo(Equal(attachedNode.Name))
						logrus.Infof("volume %v (%v) is attached to node %v after failover",
							vol.ID, apiVol.Id, attachedNodeAfter.Name)
						attachedNode = attachedNodeAfter
					} else {
						logrus.Infof("skipping the failover since there are not enough nodes")
					}
				})

				Step(fmt.Sprintf("start PX on a client node %s for app %s", clientNode.Name, ctx.App.Key), func() {
					logrus.Infof("Starting volume driver on node %s", clientNode.Name)
					StartVolDriverAndWait([]node.Node{*clientNode})

					logrus.Infof("Giving some time for app and PX to settle down on node %s", clientNode.Name)
					time.Sleep(60 * time.Second)
				})

				Step(fmt.Sprintf("validate app %s after all nodes are up", ctx.App.Key), func() {
					ValidateContext(ctx)
				})

				Step(fmt.Sprintf("verify export to the client node %s for app %s", clientNode.Name, ctx.App.Key), func() {
					validateAttachedNode(vol, attachedNode)
					exports := getExportsOnNode(apiVol, attachedNode)
					Expect(exports).Should(ContainElement(clientNode.DataIp),
						"client IP not found in the exports after starting PX on the client node")
				})

				Step(fmt.Sprintf("verify that app %s pods are active", ctx.App.Key), func() {
					numPods := len(workers)
					counters := getAppCounters(apiVol, attachedNode, 3*time.Duration(numPods)*time.Second)
					activePods := getActivePods(counters)
					Expect(len(activePods)).To(Equal(numPods))
				})
			}
		})
	})

	// Delete k8s service
	Context("{Sharedv4SvcDeleteK8sService}", func() {
		BeforeEach(func() {
			testrailID = 55050
			namespacePrefix = "deletek8s-"
		})

		It("has to delete k8s service", func() {
			for _, ctx := range testSv4Contexts {
				Step("Delete service and verify deletion", func() {
					ns := ctx.GetID()

					services, err := core.Instance().ListServices(ns, metav1.ListOptions{})
					Expect(err).NotTo(HaveOccurred())
					Expect(len(services.Items)).To(Equal(1))

					serviceName := services.Items[0].Name
					core.Instance().DeleteService(serviceName, ns)

					// verify the deletetionTimestamp is there
					deletionTimestamp := getDeletionTimestampFromContext(ctx)
					Expect(deletionTimestamp).NotTo(BeNil())
				})

				Step("rescale apps", func() {
					scaleApp(ctx, 0)
					ValidateContext(ctx)

					scaleApp(ctx, numPods)
					ValidateContext(ctx)
				})

				Step("get service and verify deletion is gone", func() {
					// verify the deletetionTimestamp is empty
					deletionTimestamp := getDeletionTimestampFromContext(ctx)
					Expect(deletionTimestamp).To(BeNil())
				})
			}
		})
	})

	// Template for additional tests
	// Context("{}", func() {
	// 	BeforeEach(func() {
	// 		testrailID = 0
	//      namespace_prefix = ""
	// 	})
	//
	// 	JustBeforeEach(func() {
	//		// since the apps are deployed by JustBeforeEach in the outer block,
	// 		// any changes to the deployed apps go here in JustBeforeEach() as well.
	// 	})
	//
	// 	It("", func() {
	// 		for _, ctx := range testSv4Contexts {
	// 		}
	// 	})

	// 	AfterEach(func() {
	// 	})
	// })

	AfterEach(func() {
		Step("destroy apps", func() {
			if CurrentGinkgoTestDescription().Failed {
				logrus.Info("not destroying apps because the test failed\n")
				return
			}
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
			}
		})
	})

	JustAfterEach(func() {
		AfterEachTest(contexts, testrailID, runID)
	})
})

// returns the contexts that are running test-sv4-svc* apps
func getTestSv4Contexts(contexts []*scheduler.Context) []*scheduler.Context {
	var testSv4Contexts []*scheduler.Context
	for _, ctx := range contexts {
		if !strings.HasPrefix(ctx.App.Key, "test-sv4-svc") {
			continue
		}
		testSv4Contexts = append(testSv4Contexts, ctx)
	}
	return testSv4Contexts
}

// returns the appCounter structs for the app pods by scanning the export path on the NFS server
func getAppCounters(vol *api.Volume, attachedNode *node.Node, sleepInterval time.Duration) map[string]appCounter {
	snap1 := getAppCountersSnapshot(vol, attachedNode)
	time.Sleep(sleepInterval)
	snap2 := getAppCountersSnapshot(vol, attachedNode)
	Expect(len(snap1)).To(Equal(len(snap2)), "unexpected change in the number of pods when collecting counters")

	ret := map[string]appCounter{}
	for podName, counter1 := range snap1 {
		counter2, ok := snap2[podName]
		Expect(ok).To(BeTrue(), "pod %v counter not found in the second snap", podName)
		Expect(counter2).To(BeNumerically(">=", counter1), "counter for pod %v decreased unexpectedly", podName)
		ret[podName] = appCounter{podName: podName, counter: counter2, active: counter2 > counter1}
	}
	return ret
}

func getAppCountersSnapshot(vol *api.Volume, attachedNode *node.Node) map[string]int {
	counterByPodName := map[string]int{}
	// We find all the files in the root dir where the pods are writing their counters.
	// Example:
	// # find /var/lib/osd/pxns/1088228603475411556 -maxdepth 1 -type f -exec tail -1 {} \; -exec echo -n ':' \; -print
	// 1500:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-vmdn5
	// 2458:/var/lib/osd/pxns/1088228603475411556/common
	// 1404:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-m9kzc
	// 1436:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-g79kh
	// 71:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-mwxn2
	// 1395:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-4ck8x
	// 77:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-d6598
	// 91:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-68nwv
	// 1406:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-h6hgx
	//
	cmd := fmt.Sprintf("find %s%s -maxdepth 1 -type f -exec tail -1 {} \\; -exec echo -n ':' \\; -print",
		exportPathPrefix, vol.Id)
	output, err := runCmd(cmd, *attachedNode)
	Expect(err).NotTo(HaveOccurred())
	for _, line := range strings.Split(output, "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, ":")
		Expect(len(parts)).To(Equal(2))

		podName := path.Base(parts[1])
		if podName == "common" {
			// ignore the common file that all pods write to
			continue
		}
		val, err := strconv.Atoi(parts[0])
		Expect(err).NotTo(HaveOccurred())

		counterByPodName[podName] = val
	}
	return counterByPodName
}

// Validate the app counters after the failover.
// - counters for all pods (with some exceptions) should continue incrementing
// - the specified number of pods should have stopped incrementing their counters and the same number of
//   new pods should have started incrementing the counters
func validateAppCounters(ctx *scheduler.Context, countersBefore, countersAfter map[string]appCounter,
	numPods int, numDeletions []int) {

	var survivingPods []string
	var terminatedPods []string
	var newPods []string

	if numDeletions == nil {
		// no pod deletions expected
		numDeletions = []int{0}
	}
	for podName, counterBefore := range countersBefore {
		// pod was not active even before the failover. This can happen when there are multiple failovers.
		if !counterBefore.active {
			continue
		}
		counterAfter, ok := countersAfter[podName]
		// Since we don't delete the files for the terminated pods, the file should still be there on the volume
		Expect(ok).To(BeTrue())
		if counterAfter.active {
			Expect(counterAfter.counter).To(BeNumerically(">", counterBefore.counter))
			survivingPods = append(survivingPods, podName)
		} else {
			terminatedPods = append(terminatedPods, podName)
		}
	}
	// check for the new pods
	for podName, counterAfter := range countersAfter {
		if _, ok := countersBefore[podName]; !ok {
			// a new pod must be active
			Expect(counterAfter.active).To(BeTrue(), "pod %v is not active", podName)
			newPods = append(newPods, podName)
		}
	}
	Expect(len(terminatedPods)).To(BeElementOf(numDeletions),
		"len of actual terminated pods %v was not one of %v: countersBefore=%v, countersAfter=%v",
		terminatedPods, numDeletions, countersBefore, countersAfter)

	Expect(len(newPods)).To(Equal(len(terminatedPods)),
		"len of actual new pods %v did not match len of %v: countersBefore=%v, countersAfter=%v",
		newPods, terminatedPods, countersBefore, countersAfter)

	currentPods := len(survivingPods) + len(newPods)
	Expect(currentPods).To(Equal(numPods),
		"count of current pods %d did not match %d: countersBefore=%v, countersAfter=%v",
		currentPods, numPods, countersBefore, countersAfter)
}

// Get active pods based on the incrementing counters.
func getActivePods(counters map[string]appCounter) []string {
	var activePods []string
	for podName, counter := range counters {
		if counter.active {
			activePods = append(activePods, podName)
		}
	}
	return activePods
}

// There should not be any errors in the pod logs.
func validateAppLogs(ctx *scheduler.Context, numPods int) {
	logsByPodName, err := Inst().S.GetPodLog(ctx, 0)
	Expect(err).NotTo(HaveOccurred())
	Expect(len(logsByPodName)).To(Equal(numPods))
	var errors []string
	for podName, output := range logsByPodName {
		lines := strings.Split(output, "\n")
		for _, line := range lines {
			if strings.Contains(line, "ERROR") {
				errors = append(errors, fmt.Sprintf("pod %s: %s", podName, line))
			} else if strings.Contains(line, "WARNING") {
				logrus.Warnf("pod %s: %s", podName, line)
			}
		}
	}
	Expect(errors).To(BeNil(), "Errors found in the pod logs: %v", errors)
}

// returns the appCounter structs for the app pods by scanning the export path on the NFS server
func validateExports(vol *api.Volume, attachedNodeBefore *node.Node, attachedNodeAfter *node.Node) {
	exports := getExportsOnNode(vol, attachedNodeBefore)
	Expect(len(exports)).To(BeNumerically("==", 0),
		"exports found on the old NFS server %s for volume %s: %v", attachedNodeBefore.Name, vol.Id, exports)

	exports = getExportsOnNode(vol, attachedNodeAfter)
	Expect(len(exports)).To(BeNumerically(">", 0),
		"no exports found on the new NFS server %s for volume %s", attachedNodeAfter.Name, vol.Id)
}

// Returns the IP addresses of the clients to which the volume is being exported from the specified node.
func getExportsOnNode(vol *api.Volume, node *node.Node) []string {
	output, err := runCmd("showmount --no-headers -e", *node)
	Expect(err).NotTo(HaveOccurred())

	// Sample output:
	//
	// /var/lib/osd/pxns/1006668872421973051 192.168.121.219,192.168.121.38,192.168.121.243,192.168.121.124,192.168.121.98
	// /var/lib/osd/pxns/366303365379384956  192.168.121.219,192.168.121.38,192.168.121.98,192.168.121.243,192.168.121.124
	//
	for _, line := range strings.Split(output, "\n") {
		clientsStr := strings.TrimPrefix(line, exportPathPrefix+vol.Id+" ")
		if clientsStr != line {
			// prefix was found
			return strings.Split(strings.TrimSpace(clientsStr), ",")
		}
	}
	return nil
}

func restartVolumeDriverOnNode(nodeObj *node.Node) {
	logrus.Infof("Stopping volume driver on node %s", nodeObj.Name)
	StopVolDriverAndWait([]node.Node{*nodeObj})

	logrus.Infof("Sleep to allow the failover before restarting the volume driver on node %s", nodeObj.Name)
	time.Sleep(30 * time.Second)

	logrus.Infof("Starting volume driver on node %s", nodeObj.Name)
	StartVolDriverAndWait([]node.Node{*nodeObj})

	logrus.Infof("Giving volume driver and the app pods some time to settle down on node %s", nodeObj.Name)
	time.Sleep(60 * time.Second)
}

func rebootNodeAndWaitForReady(nodeObj *node.Node) {
	logrus.Infof("Rebooting node %s", nodeObj.Name)
	err := Inst().N.RebootNode(*nodeObj, node.RebootNodeOpts{
		Force: true,
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         cmdTimeout,
			TimeBeforeRetry: cmdRetry,
		},
	})
	Expect(err).NotTo(HaveOccurred())

	logrus.Infof("Testing connection to node %s", nodeObj.Name)
	err = Inst().N.TestConnection(*nodeObj, node.ConnectionOpts{
		Timeout:         testConnTimeout,
		TimeBeforeRetry: rebootRetry,
	})
	Expect(err).NotTo(HaveOccurred())

	logrus.Infof("Waiting for node %s to be ready", nodeObj.Name)
	err = Inst().S.IsNodeReady(*nodeObj)
	Expect(err).NotTo(HaveOccurred())

	logrus.Infof("Waiting for driver to be up on node %s", nodeObj.Name)
	err = Inst().V.WaitDriverUpOnNode(*nodeObj, Inst().DriverStartTimeout)
	Expect(err).NotTo(HaveOccurred())

	logrus.Infof("Successfully rebooted the node %s", nodeObj.Name)
}

func scaleApps(contexts []*scheduler.Context, numPods int) {
	for _, ctx := range contexts {
		scaleApp(ctx, numPods)
	}
}

func scaleApp(ctx *scheduler.Context, numPods int) {
	logrus.Infof("scaling app %s to %d", ctx.App.Key, numPods)
	applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
	Expect(err).NotTo(HaveOccurred())
	for name := range applicationScaleUpMap {
		applicationScaleUpMap[name] = int32(numPods)
	}
	err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
	Expect(err).NotTo(HaveOccurred())
}

func validateAttachedNode(vol *volume.Volume, attachedNode *node.Node) {
	err := Inst().V.RefreshDriverEndpoints()
	Expect(err).NotTo(HaveOccurred())
	attachedNodeNow, err := Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
	Expect(err).NotTo(HaveOccurred())
	Expect(attachedNodeNow.Name).Should(Equal(attachedNode.Name), "unexpected failover")
}

func setHALevel(vol *volume.Volume, haLevel int64) {
	logrus.Infof("setting HA level to 2 on volume %s", vol.ID)
	err := Inst().V.SetReplicationFactor(vol, haLevel, nil)
	Expect(err).NotTo(HaveOccurred())

	logrus.Infof("validating successful update of HA level on volume %s", vol.ID)
	newRepl, err := Inst().V.GetReplicationFactor(vol)
	Expect(err).NotTo(HaveOccurred())
	Expect(newRepl).To(BeNumerically("==", haLevel))
}

func getReplicaNodeIDs(vol *volume.Volume) map[string]bool {
	replicaNodes := map[string]bool{}
	replicaSets, err := Inst().V.GetReplicaSets(vol)
	Expect(err).NotTo(HaveOccurred())
	for _, replicaSet := range replicaSets {
		for _, node := range replicaSet.Nodes {
			replicaNodes[node] = true
		}
	}
	return replicaNodes
}

func updateFailoverStrategyForApps(contexts []*scheduler.Context, val api.Sharedv4FailoverStrategy_Value) {
	for _, ctx := range contexts {
		vols, err := Inst().S.GetVolumes(ctx)
		Expect(err).NotTo(HaveOccurred(), "failed in getting volumes: %v", err)
		Expect(len(vols)).To(Equal(1))
		vol := vols[0]

		err = Inst().V.UpdateSharedv4FailoverStrategyUsingPxctl(vol.ID, val)
		Expect(err).NotTo(HaveOccurred(), "failed in updating sharedv4 strategy for volume %v: %v", vol.ID, err)

		apiVol, err := Inst().V.InspectVolume(vol.ID)
		Expect(err).NotTo(HaveOccurred(), "failed in inspect volume: %v", err)
		Expect(apiVol.Spec.Sharedv4Spec.FailoverStrategy == val).To(BeTrue(), "unexpected failover strategy")
	}
}

func getDeletionTimestampFromContext(ctx *scheduler.Context) *metav1.Time {
	ns := ctx.GetID()
	services, err := core.Instance().ListServices(ns, metav1.ListOptions{})
	Expect(err).NotTo(HaveOccurred())
	Expect(len(services.Items)).To(Equal(1))

	deletionTimestamp := services.Items[0].DeletionTimestamp
	logrus.Infof("deletion timestamp: %v", deletionTimestamp)
	return deletionTimestamp
}

func runCmd(cmd string, n node.Node) (string, error) {
	output, err := Inst().N.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         cmdTimeout,
		TimeBeforeRetry: cmdRetry,
		Sudo:            true,
	})
	if err != nil {
		logrus.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}
	return output, err
}
