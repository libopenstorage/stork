package tests

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/testrailuttils"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
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
	exportPathPrefix     = "/var/lib/osd/pxns"
)

// Verify that the pods on old and new NFS servers get terminated after a failover.
var _ = Describe("{Sharedv4SvcPodRestart}", func() {
	var contexts []*scheduler.Context
	var nodeReplicaMap map[string]bool
	JustBeforeEach(func() {
		StartTorpedoTest("Sharedv4SvcPodRestart", "Validate failover Test", nil, 0)
	})
	It("has to setup, validate, failover, make sure pods on old and new server got restarted, and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nfsserverfailover-%d", i))...)
		}

		// scale all the apps to have 2 pods since later we will have only 2 nodes where the pods can run
		Step("scale all apps to 2", func() {
			scaleApps(contexts, 2)
		})

		ValidateApplications(contexts)

		// wait until each app has 2 pods (some pods may be still terminating)
		for _, ctx := range contexts {
			Step(fmt.Sprintf("wait for app %s to have 2 pods", ctx.App.Key), func() {
				waitForNumPodsToEqual(ctx, 2)
			})
		}

		for _, ctx := range contexts {
			var volume *volume.Volume

			Step(fmt.Sprintf("getting app %s's volume", ctx.App.Key), func() {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(vols)).To(Equal(1))
				volume = vols[0]
			})

			aggrLevel, err := Inst().V.GetAggregationLevel(volume)
			Expect(err).NotTo(HaveOccurred())
			if aggrLevel > 1 {
				log.Infof("skipping app %s because volume %s has aggr level %d", ctx.App.Key, volume.ID, aggrLevel)
				continue
			}

			Step(fmt.Sprintf("checking the HA level for app %s's volume", ctx.App.Key), func() {
				nodeReplicaMap = getReplicaNodeIDs(volume)
				if len(nodeReplicaMap) != 2 {
					setHALevel(volume, 2)
					// ValidateContext() will fail with error "volume has invalid repl value. Expected:3 Actual:2"
					// without the line below.
					ctx.SkipVolumeValidation = true

					// get the replica map again
					nodeReplicaMap = getReplicaNodeIDs(volume)
					Expect(len(nodeReplicaMap)).To(Equal(2))
				}
			})

			Step("disable scheduling on non replica nodes", func() {
				allNodes := node.GetWorkerNodes()
				for _, aNode := range allNodes {
					if !nodeReplicaMap[aNode.VolDriverNodeID] {
						Inst().S.DisableSchedulingOnNode(aNode)
					}
				}
			})

			// scale down and then scale up the app, so that pods are only scheduled on replica nodes
			Step(fmt.Sprintf("scale down app: %s to 0 ", ctx.App.Key), func() {
				scaleApp(ctx, 0)
			})

			// wait until all pods are gone
			Step(fmt.Sprintf("wait for app %s to have 0 pods", ctx.App.Key), func() {
				waitForNumPodsToEqual(ctx, 0)
			})

			Step(fmt.Sprintf("scale up app: %s to 2", ctx.App.Key), func() {
				scaleApp(ctx, 2)
				ValidateContext(ctx)
			})

			Step("fail over nfs server, and make sure the pod on server gets restarted", func() {
				oldServer, err := Inst().V.GetNodeForVolume(volume, cmdTimeout, cmdRetry)
				Expect(err).NotTo(HaveOccurred())
				log.Infof("old nfs server %v [%v]", oldServer.SchedulerNodeName, oldServer.Addresses[0])
				pods, err := core.Instance().GetPodsUsingPV(volume.ID)
				Expect(err).NotTo(HaveOccurred())
				for _, pod := range pods {
					log.Infof("pod %s/%s in phase %v on node %v before the failover",
						pod.Namespace, pod.Name, pod.Status.Phase, pod.Spec.NodeName)
				}
				var oldPodOnOldServer corev1.Pod
				for _, pod := range pods {
					if pod.Spec.NodeName == oldServer.Name {
						oldPodOnOldServer = pod
					}
				}
				// make sure there is a pod running on the old nfs server
				Expect(oldPodOnOldServer.Name).NotTo(Equal(""))
				log.Infof("pod on old server %v, creation time %v", oldPodOnOldServer.Name, oldPodOnOldServer.CreationTimestamp)

				timestampBeforeFailOver := time.Now()
				err = Inst().V.StopDriver([]node.Node{*oldServer}, false, nil)
				Expect(err).NotTo(HaveOccurred())
				err = Inst().V.WaitDriverDownOnNode(*oldServer)
				Expect(err).NotTo(HaveOccurred())
				log.Infof("stopped px on nfs server node %v [%v]", oldServer.SchedulerNodeName, oldServer.Addresses[0])

				var newServer *node.Node

				for i := 0; i < 60; i++ {
					err := Inst().V.RefreshDriverEndpoints()
					Expect(err).NotTo(HaveOccurred())
					server, err := Inst().V.GetNodeForVolume(volume, cmdTimeout, cmdRetry)
					// there could be intermittent error here
					if err != nil {
						log.Infof("Failed to get node for volume. Error: %v", err)
					} else {
						if server.Id != oldServer.Id {
							log.Infof("nfs server failed over, new nfs server is %s [%s]", server.SchedulerNodeName, server.Addresses[0])
							newServer = server
							break
						}
					}
					time.Sleep(10 * time.Second)
				}
				// make sure nfs server failed over
				Expect(newServer).NotTo(BeNil())
				log.Infof("new nfs server is %v [%v]", newServer.SchedulerNodeName, newServer.Addresses[0])

				log.Infof("start px on old nfs server Id %v, Name %v", oldServer.Id, oldServer.Name)
				Inst().V.StartDriver(*oldServer)
				err = Inst().V.WaitDriverUpOnNode(*oldServer, Inst().DriverStartTimeout)
				Expect(err).NotTo(HaveOccurred())
				log.Infof("px is up on old nfs server Id %v, Name %v", oldServer.Id, oldServer.Name)

				ValidateApplications(contexts)

				// make sure the pods on both old and new server are restarted
				pods, err = core.Instance().GetPodsUsingPV(volume.ID)
				Expect(err).NotTo(HaveOccurred())
				for _, pod := range pods {
					log.Infof("pod %s/%s in phase %v on node %v after the failover",
						pod.Namespace, pod.Name, pod.Status.Phase, pod.Spec.NodeName)
				}
				podRestartedOnOldServer := false
				podRestartedOnNewServer := false
				for _, pod := range pods {
					if pod.Spec.NodeName == oldServer.Name {
						log.Infof("pod on old server %v, creation time %v", oldPodOnOldServer.Name, oldPodOnOldServer.CreationTimestamp)
						log.Infof("After failover, pod on old server %v, creation time %v", pod.Name, pod.CreationTimestamp)
						Expect(pod.CreationTimestamp.After(timestampBeforeFailOver)).To(BeTrue())
						podRestartedOnOldServer = true
					}
					if pod.Spec.NodeName == newServer.Name {
						log.Infof("After failover, pod on new server %v, creation time %v", pod.Name, pod.CreationTimestamp)
						Expect(pod.CreationTimestamp.After(timestampBeforeFailOver)).To(BeTrue())
						podRestartedOnNewServer = true
					}
				}

				Expect(podRestartedOnOldServer).To(BeTrue())
				Expect(podRestartedOnNewServer).To(BeTrue())

				// re-enable scheduling on non replica nodes
				for _, aNode := range node.GetWorkerNodes() {
					if !nodeReplicaMap[aNode.VolDriverNodeID] {
						Inst().S.EnableSchedulingOnNode(aNode)
					}
				}
				nodeReplicaMap = nil
			})
		}

		for _, ctx := range contexts {
			TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
	AfterEach(func() {
		if nodeReplicaMap == nil {
			return
		}
		// re-enable scheduling on non replica nodes
		for _, aNode := range node.GetWorkerNodes() {
			if !nodeReplicaMap[aNode.VolDriverNodeID] {
				Inst().S.EnableSchedulingOnNode(aNode)
			}
		}
	})
})

// Verify different combination of storageClass.sharedv4 and PVC.accessMode settings
var _ = Describe("{PVCAccessModeFunctional}", func() {
	var testrailID, runID int
	var contexts []*scheduler.Context
	var testName string
	var origCustomAppConfigs, customAppConfigs map[string]scheduler.AppConfig

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		StartTorpedoTest("PVCAccessModeFunctional", "PVC access mode functional Test", nil, testrailID)
		// save the original custom app configs
		origCustomAppConfigs = make(map[string]scheduler.AppConfig)
		for appName, customAppConfig := range Inst().CustomAppConfig {
			origCustomAppConfigs[appName] = customAppConfig
		}

		// replace with our custom app config
		for appName, customAppConfig := range customAppConfigs {
			Inst().CustomAppConfig[appName] = customAppConfig
		}
		log.Infof("JustBeforeEach using Inst().CustomAppConfig = %v", Inst().CustomAppConfig)

		err := Inst().S.RescanSpecs(Inst().SpecDir, Inst().V.String())
		Expect(err).NotTo(HaveOccurred(), "Failed to rescan specs from %s", Inst().SpecDir)

		contexts = ScheduleApplications(testName)
		ValidateApplications(contexts)

		// Skip the test if we don't find any of our apps
		for appName := range customAppConfigs {
			found := false
			for _, ctx := range contexts {
				if ctx.App.Key == appName {
					found = true
					break
				}
			}
			if !found {
				log.Warnf("App %v not found in %d contexts, skipping test", appName, len(contexts))
				Skip(fmt.Sprintf("app %v not found", appName))
			}
		}
	})

	When("sharedv4 storageClass is used with ReadWriteOnce PVC", func() {
		var appName string
		BeforeEach(func() {
			testrailID = 83069
			testName = "sharedv4-sc-rwo-pvc"
			appName = "test-sv4-svc-enc"
			customAppConfigs = map[string]scheduler.AppConfig{
				appName: {
					StorageClassSharedv4: "\"true\"",
					PVCAccessMode:        "\"ReadWriteOnce\"",
					Replicas:             1,
				},
			}
		})

		It("should create a non-sharedv4 volume", func() {
			ctx := findContext(contexts, appName)
			_, apiVol, _ := getSv4TestAppVol(ctx)
			Expect(apiVol.Spec.Sharedv4).To(BeFalse(), "sharedv4 volume was created unexpectedly")
		})
	})

	When("non-sharedv4 storageClass is used with ReadWriteMany PVC", func() {
		var appName string
		BeforeEach(func() {
			testrailID = 83070
			testName = "non-sharedv4-sc-rwx-pvc"
			appName = "test-sv4-svc"
			customAppConfigs = map[string]scheduler.AppConfig{
				appName: {
					StorageClassSharedv4: "\"false\"",
					PVCAccessMode:        "\"ReadWriteMany\"",
					Replicas:             3,
				},
			}
		})

		It("should create a sharedv4 volume", func() {
			ctx := findContext(contexts, appName)
			_, apiVol, _ := getSv4TestAppVol(ctx)
			Expect(apiVol.Spec.Sharedv4).To(BeTrue(), "sharedv4 volume was not created")
		})
	})

	JustAfterEach(func() {
		// restore the original custom app configs
		for appName, customAppConfig := range origCustomAppConfigs {
			Inst().CustomAppConfig[appName] = customAppConfig
		}
		// remove any keys that are not present in the orig map
		for appName := range Inst().CustomAppConfig {
			if _, ok := origCustomAppConfigs[appName]; !ok {
				delete(Inst().CustomAppConfig, appName)
			}
		}
		log.Infof("JustAfterEach restoring Inst().CustomAppConfig = %v", Inst().CustomAppConfig)

		err := Inst().S.RescanSpecs(Inst().SpecDir, Inst().V.String())
		Expect(err).NotTo(HaveOccurred(), "Failed to rescan specs from %s", Inst().SpecDir)

		AfterEachTest(contexts, testrailID, runID)
	})

	AfterEach(func() {
		defer EndTorpedoTest()
		Step("destroy apps", func() {
			if CurrentGinkgoTestDescription().Failed {
				log.Info("not destroying apps because the test failed\n")
				return
			}
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
			}
		})
	})
})

func findContext(contexts []*scheduler.Context, appName string) (ret *scheduler.Context) {
	for _, ctx := range contexts {
		if ctx.App.Key == appName {
			ret = ctx
			break
		}
	}
	Expect(ret).NotTo(BeNil(), "failed to find app %v in the contexts", appName)
	return
}

// Tests below use test-sharedv4 app https://github.com/portworx/test-sharedv4 .
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
	sleepBetweenFailovers() time.Duration
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

func (fm *failoverMethodRestartVolDriver) sleepBetweenFailovers() time.Duration {
	return 30 * time.Second
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

func (fm *failoverMethodReboot) sleepBetweenFailovers() time.Duration {
	return 2 * time.Minute
}

type failoverMethodMaintenance struct {
}

func (fm *failoverMethodMaintenance) doFailover(attachedNode *node.Node) {
	maintainVolumeDriverOnNode(attachedNode)
}

func (fm *failoverMethodMaintenance) String() string {
	return "maintenance"
}

func (fm *failoverMethodMaintenance) getExpectedPodDeletions() []int {
	// 2 pods, one on the old NFS server and one on the new NFS server should be deleted.
	return []int{2}
}

func (fm *failoverMethodMaintenance) sleepBetweenFailovers() time.Duration {
	return 30 * time.Second
}

var _ = Describe("{Sharedv4SvcFunctional}", func() {
	var testrailID, runID int
	var contexts, testSv4Contexts []*scheduler.Context
	var workers []node.Node
	var numPods int
	var namespacePrefix string
	var tcpDumpInstalled bool

	BeforeEach(func() {
		if !tcpDumpInstalled {
			// package installation hangs when using nsenter to run command on nodes, so do it only when using ssh
			if Inst().N.IsUsingSSH() {
				for _, anode := range node.GetWorkerNodes() {
					// TODO: support other OS'es
					log.Infof("installing tcpdump on node %s", anode.Name)
					cmd := "yum install -y tcpdump"
					_, err := Inst().N.RunCommandWithNoRetry(anode, cmd, node.ConnectionOpts{
						Timeout:         cmdTimeout,
						TimeBeforeRetry: cmdRetry,
						Sudo:            true,
					})
					if err != nil {
						log.Warnf("failed to install tcpdump on node %s: %v", anode.Name, err)
						break
					}
				}
			}
			tcpDumpInstalled = true
		}
	})

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		StartTorpedoTest("Sharedv4SvcFunctional", "Sharedv4 Svc Functional Test", nil, testrailID)
		// Set up all apps
		contexts = nil
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
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

	Context("{Sharedv4SvcFailoverFailback}", func() {
		var numFailovers int
		var fm failoverMethod
		var wg *sync.WaitGroup

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

						vol, apiVol, _ := getSv4TestAppVol(ctx)

						// Since the HA level is 2, we can verify failover and failback by repeating the steps below.
						for i := 0; i < numFailovers; i++ {
							var countersBefore, countersAfter map[string]appCounter
							var attachedNodeBefore, attachedNodeAfter *node.Node
							var err error

							counterCollectionInterval := 3 * time.Duration(numPods) * time.Second
							failoverLog := fmt.Sprintf("failover #%d by %s for app %s", i, fm, ctx.App.Key)

							if i > 0 {
								Step("sleep between the failovers",
									func() {
										dur := fm.sleepBetweenFailovers()
										log.Infof("sleeping for %v between the failovers", dur)
										time.Sleep(dur)
									})
							}

							Step(fmt.Sprintf("get the attached node for volume %s before %s", vol.ID, failoverLog),
								func() {
									attachedNodeBefore, err = Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
									Expect(err).NotTo(HaveOccurred())
									log.Infof("volume %v (%v) is attached to node %v before %s",
										vol.ID, apiVol.Id, attachedNodeBefore.Name, failoverLog)
								})

							Step(fmt.Sprintf("get counters from node %v before %s", attachedNodeBefore.Name, failoverLog),
								func() {
									Eventually(func() bool {
										countersBefore = getAppCounters(apiVol, attachedNodeBefore, counterCollectionInterval)
										activePods := getActivePods(countersBefore)
										return (len(activePods) == numPods)
									}, 3*time.Minute, 10*time.Second).Should(BeTrue(),
										"number of active keys did not match for volume %v (%v) for app %v. countersBefore map: %v",
										vol.ID, apiVol.Id, ctx.App.Key, countersBefore)

								})

							Step(fmt.Sprintf("failover #%d by %s", i, fm),
								func() {
									pcapFile := fmt.Sprintf("/var/cores/%s.%d.pcap", ctx.ScheduleOptions.Namespace, i)
									wg = startPacketCapture(pcapFile)
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
									log.Infof("volume %v (%v) is attached to node %v after %s",
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

							Step(fmt.Sprintf("wait for the packet capture goroutines to finish after %s", failoverLog),
								func() {
									if wg != nil {
										wg.Wait()
										wg = nil
									}
								})
						}
					})
				}
			})
		}

		// test failover/failback by restarting the volume driver
		Context("{Sharedv4SvcRestartVolDriver}", func() {
			BeforeEach(func() {
				namespacePrefix = "restartvoldriver"
				fm = &failoverMethodRestartVolDriver{}
				testrailID = 54374
			})
			testFailoverFailback()
		})

		// test failover/failback by rebooting the node
		Context("{Sharedv4SvcRebootNode}", func() {
			BeforeEach(func() {
				namespacePrefix = "rebootnode"
				fm = &failoverMethodReboot{}
				testrailID = 54385
			})
			testFailoverFailback()
		})

		// test failover/failback by putting the node in maintenance mode
		Context("{Shared4SvcMaintainNode}", func() {
			BeforeEach(func() {
				namespacePrefix = "maintainnode"
				fm = &failoverMethodMaintenance{}
				testrailID = 54375
			})
			testFailoverFailback()
		})

		AfterEach(func() {
			if wg != nil {
				log.Infof("waiting for the packet capture goroutines to finish")
				wg.Wait()
				wg = nil
			}
		})
	})

	// Bring PX down on the node where the volume is attached. Verify that pods on client nodes unmount the volume
	// and teardown successfully
	Context("{Sharedv4ClientTeardownWhenServerOffline}", func() {
		BeforeEach(func() {
			testrailID = 54780
			namespacePrefix = "clientteardown"

		})

		JustBeforeEach(func() {
			Step("change the sharedv4 failover strategy to normal", func() {
				updateFailoverStrategyForApps(testSv4Contexts, api.Sharedv4FailoverStrategy_NORMAL)
			})
		})

		It("has to schedule apps, stop volume driver on node where volume is attached, teardown the application", func() {
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("validate app %s", ctx.App.Key), func() {
					ValidateContext(ctx)
				})

				Step(
					fmt.Sprintf("stop the volume driver on attached node and verify app %s teardown succeeds", ctx.App.Key),
					func() {
						vol, _, attachedNode := getSv4TestAppVol(ctx)

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
								err := Inst().S.SelectiveWaitForTermination(
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
	Context("{Sharedv4SvcClientRestart}", func() {
		BeforeEach(func() {
			testrailID = 54383
			namespacePrefix = "clientrestart"
		})

		It("has to verify no I/O disruption for test-sv4-svc apps after PX restart on NFS client node", func() {
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("restart client nodes one by one and verify I/O for app %s", ctx.App.Key), func() {

					vol, apiVol, attachedNode := getSv4TestAppVol(ctx)

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
								_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
									countersBefore = getAppCounters(apiVol, attachedNode, 3*time.Duration(numPods)*time.Second)
									activePods := getActivePods(countersBefore)

									if len(activePods) != numPods {
										return nil, true, fmt.Errorf("number of active keys did not match for volume %v (%v) for app %v. countersBefore map: %v",
											vol.ID, apiVol.Id, ctx.App.Key, countersBefore)
									}

									return nil, false, nil

								}, 3*time.Minute, 10*time.Second)

								log.FailOnError(err, "Failed to verify counter in get counter before")
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
	Context("{Sharedv4SvcUnexportExport}", func() {
		BeforeEach(func() {
			testrailID = 54776
			namespacePrefix = "unexport"
		})

		It("has to verify that the server unexports and re-exports volume to the client node "+
			"after pod goes away and comes back", func() {
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("scale the deployment down and then up for app %s", ctx.App.Key), func() {

					vol, apiVol, attachedNode := getSv4TestAppVol(ctx)

					// Scale down the app by 1 and check if the pod on NFS client node got removed.
					// If not, scale down by 1 more. It should not take more than 2 attempts since
					// there is no more than 1 pod running on each node.
					var nodeWithNoPod *node.Node
					var exportsBeforeScaleDown []string
				Outer:
					for i := 1; i < 3; i++ {
						exportsBeforeScaleDown = getExportsOnNode(apiVol, attachedNode)
						newNumPods := len(workers) - i
						scaleApp(ctx, newNumPods)

						// give some time for the pod to terminate
						var pods []corev1.Pod
						var err error
						Eventually(func() (int, error) {
							pods, err = core.Instance().GetPodsUsingPV(vol.ID)
							return len(pods), err
						}, 3*time.Minute, 10*time.Second).Should(BeNumerically("==", newNumPods),
							"number of pods did not reach %v for volume %v (%v) for app %v",
							newNumPods, vol.ID, apiVol.Id, ctx.App.Key)

						// ensure that all pods are running
						ValidateContext(ctx)

						// check which node/s have no pod; we are looking for a client pod to be gone
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

					// IPs that were exported before scaling down the app that we expect to remain exported.
					// Note that this may include 2 IPs (data and mgmt) for the attached node
					// depending on whether we deleted the bind-mounted pod above.
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
	Context("{Sharedv4SvcClientOfflineTooLong}", func() {
		BeforeEach(func() {
			testrailID = 54778
			namespacePrefix = "clientoffline"
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
				var replicaNodeIDs map[string]bool
				var attachedNode, clientNode *node.Node
				var failover bool

				vol, apiVol, attachedNode = getSv4TestAppVol(ctx)
				if apiVol.Spec.AggregationLevel > 1 {
					log.Infof("skipping app %s because volume %s has aggr level %d",
						ctx.App.Key, vol.ID, apiVol.Spec.AggregationLevel)
					continue
				}

				Step(fmt.Sprintf("get replica nodes for app %s's volume", ctx.App.Key), func() {
					replicaNodeIDs = getReplicaNodeIDs(vol)
					Expect(len(replicaNodeIDs)).To(Equal(2))
				})

				Step(fmt.Sprintf("stop PX on a client node for app %s and wait for export gone", ctx.App.Key), func() {
					// We need at least 5 nodes to do a failover after stopping PX on the client node.
					// Otherwise, PX will lose quorum. (This assumes that there are 3 internal kvdb nodes.)
					failover = len(workers) >= 5

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
					log.Infof("chose client node %v to stop PX on", clientNode.Name)

					exports := getExportsOnNode(apiVol, attachedNode)
					Expect(exports).Should(ContainElement(clientNode.DataIp),
						"client IP not found in the exports before stopping PX on the client node")

					log.Infof("stopping volume driver on node %s", clientNode.Name)
					StopVolDriverAndWait([]node.Node{*clientNode})

					// TODO: offlineClientTimeout = 15 * time.Minute in ref.go
					// need to make it configurable so that we don't have to sleep for that long in the test
					dur := offlineClientTimeout + 1*time.Minute
					log.Infof("sleep for %v to allow the server to remove client %v's export", dur, clientNode.Name)
					time.Sleep(dur)

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
						log.Infof("volume %v (%v) is attached to node %v after failover",
							vol.ID, apiVol.Id, attachedNodeAfter.Name)
						attachedNode = attachedNodeAfter
					} else {
						log.Infof("skipping the failover since there are not enough nodes")
					}
				})

				Step(fmt.Sprintf("start PX on a client node %s for app %s", clientNode.Name, ctx.App.Key), func() {
					log.Infof("Starting volume driver on node %s", clientNode.Name)
					StartVolDriverAndWait([]node.Node{*clientNode})

					dur := 60 * time.Second
					log.Infof("sleeping for %v for app and PX to settle down on node %s", dur, clientNode.Name)
					time.Sleep(dur)
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

	// Verify the recovery procedure after user accidentally deletes the sharedv4 k8s service object
	// Recovery is done by scaling deployment to 0 and then back up.
	Context("{Sharedv4SvcDeleteK8sService}", func() {
		BeforeEach(func() {
			testrailID = 55050
			namespacePrefix = "deletek8ssvc"
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

	// Induce a failover while the NFS server node is in resync.
	Context("{Sharedv4SvcResync}", func() {
		// There are 3 replica nodes for the test-sharedv4 app's volume. We designate them as:
		//
		// - attachedNodeOrig: node where the volume was attached initially when the app was deployed
		//
		// - resyncReplica: node that will be resync'ing the volume while the volume fails over to
		//                  this node and then fails over again to another node
		//
		// - otherReplica: node that stays up the whole time
		//
		var otherReplica *node.Node

		BeforeEach(func() {
			testrailID = 54379
			namespacePrefix = "resyncfailover"
		})

		It("fails over while NFS server node is in resync", func() {
			for _, ctx := range testSv4Contexts {
				var replicaNodeIDs map[string]bool
				var resyncReplica, attachedNodeOrig *node.Node

				vol, apiVol, attachedNode := getSv4TestAppVol(ctx)
				if apiVol.Spec.AggregationLevel > 1 {
					log.Infof("skipping app %s because volume %s has aggr level %d",
						ctx.App.Key, vol.ID, apiVol.Spec.AggregationLevel)
					continue
				}

				attachedNodeOrig = attachedNode

				// directory on the export path in which we will dump data to make the volume
				// go into Resync state
				testDir := fmt.Sprintf("%s/%s/resync_test_dir", exportPathPrefix, apiVol.Id)

				replicaNodeIDs = getReplicaNodeIDs(vol)
				Expect(len(replicaNodeIDs)).To(Equal(3))

				Step(fmt.Sprintf("pick a replica node for resync for app %s", ctx.App.Key), func() {
					for _, worker := range workers {
						if replicaNodeIDs[worker.VolDriverNodeID] && worker.VolDriverNodeID != attachedNode.Id {
							// make a copy so that we get a fresh pointer
							worker := worker
							if resyncReplica == nil {
								resyncReplica = &worker
							} else {
								otherReplica = &worker
								break
							}
						}
					}
					Expect(resyncReplica).ToNot(BeNil())
					Expect(otherReplica).ToNot(BeNil())
					log.Infof("Volume %v (%v), resyncReplica=%v, otherReplica=%v, attachedNodeOrig=%v",
						vol.ID, apiVol.Id, resyncReplica.Name, otherReplica.Name, attachedNodeOrig.Name)
				})

				Step(fmt.Sprintf("stop PX on the replica node for app %s", ctx.App.Key), func() {
					StopVolDriverAndWait([]node.Node{*resyncReplica})
					// We don't expect a failover
					validateAttachedNode(vol, attachedNode)
				})

				Step(fmt.Sprintf("dump data on app %s volume", ctx.App.Key), func() {
					cmd := fmt.Sprintf("mkdir %s; dd if=/dev/urandom of=%s/data bs=1M count=20000 iflag=fullblock",
						testDir, testDir)
					output, err := runCmd(cmd, *attachedNode)
					Expect(err).NotTo(HaveOccurred(), output)
				})

				Step(fmt.Sprintf("cordon attachments on the other replica for app %s", ctx.App.Key), func() {
					// this ensures that the volume will fail over to the resyncReplica node
					cmd := fmt.Sprintf("pxctl service node cordon-attachments --node  %s", otherReplica.Id)
					output, err := runCmd(cmd, *otherReplica)
					Expect(err).NotTo(HaveOccurred(), output)
				})

				Step(fmt.Sprintf("start volume driver on node %s for app %s", resyncReplica.Name, ctx.App.Key), func() {
					// PX will start resync'ing the volume when it comes back up on this node
					StartVolDriverAndWait([]node.Node{*resyncReplica})
				})

				Step(fmt.Sprintf("failover to the node in resync for app %s", ctx.App.Key), func() {
					// call "systemctl" directly to avoid delays built into other utility functions
					// that can cause the node to finish resyncing before we do the failover
					err := Inst().N.Systemctl(*attachedNode, "portworx.service", node.SystemctlOpts{
						Action: "restart",
						ConnectionOpts: node.ConnectionOpts{
							Timeout:         5 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						}})
					Expect(err).NotTo(HaveOccurred())

					// wait for failover
					log.Infof("waiting for failover to the resync node %v", resyncReplica.Name)
					var attachedNodeNow *node.Node
					Eventually(func() (string, error) {
						attachedNodeNow, err = Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
						if err != nil {
							return "", err
						}
						return attachedNodeNow.Name, nil
					}, 1*time.Minute, 10*time.Second).ShouldNot(Equal(attachedNode.Name),
						"volume %v (%v) for app %v did not fail over", vol.ID, apiVol.Id, ctx.App.Key)

					log.Infof("volume %v (%v) is attached to node %v after failover",
						vol.ID, apiVol.Id, attachedNodeNow.Name)

					// ensure that we failed over to the node in resync
					Expect(attachedNodeNow.Name).To(Equal(resyncReplica.Name),
						"volume %v (%v) did not fail over to the node in resync", vol.ID, apiVol.Id)
					attachedNode = attachedNodeNow
				})

				Step(fmt.Sprintf("uncordon attachments on the other replica for app %s", ctx.App.Key), func() {
					cmd := fmt.Sprintf("pxctl service node uncordon-attachments --node  %s", otherReplica.Id)
					output, err := runCmd(cmd, *otherReplica)
					Expect(err).NotTo(HaveOccurred(), output)
				})

				Step(fmt.Sprintf("wait for the orig attached node %s to come back up", attachedNodeOrig.Name), func() {
					err := Inst().V.WaitDriverUpOnNode(*attachedNodeOrig, Inst().DriverStartTimeout)
					Expect(err).NotTo(HaveOccurred())
				})

				Step(fmt.Sprintf("verify that the volume %v (%v) is still in resync", vol.ID, apiVol.Id), func() {
					cmd := fmt.Sprintf("pxctl volume inspect %s | grep \"Replication Status\"", apiVol.Id)
					replStatus, err := runCmd(cmd, *resyncReplica)
					Expect(err).NotTo(HaveOccurred(), replStatus)
					Expect(replStatus).To(ContainSubstring("Resync"))
				})

				Step(fmt.Sprintf("failover from the node in resync for app %s", ctx.App.Key), func() {
					// restart vol driver to induce a failover
					restartVolumeDriverOnNode(resyncReplica)

					// attached node should have changed
					attachedNodeNow, err := Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
					Expect(err).NotTo(HaveOccurred())
					Expect(attachedNodeNow.Name).ToNot(Equal(resyncReplica.Name),
						"volume %v (%v) for app %v did not fail over", vol.ID, apiVol.Id, ctx.App.Key)

					log.Infof("volume %v (%v) is attached to node %v after failover from the node in resync",
						vol.ID, apiVol.Id, attachedNodeNow.Name)
					attachedNode = attachedNodeNow
				})

				Step(fmt.Sprintf("validate app %s after all nodes are up", ctx.App.Key), func() {
					ValidateContext(ctx)
				})

				Step(fmt.Sprintf("verify that app %s's pods are active", ctx.App.Key), func() {
					numPods := len(workers)
					counters := getAppCounters(apiVol, attachedNode, 3*time.Duration(numPods)*time.Second)
					activePods := getActivePods(counters)
					Expect(len(activePods)).To(Equal(numPods))
				})

				Step(fmt.Sprintf("remove data on app %s's volume", ctx.App.Key), func() {
					cmd := fmt.Sprintf("rm -rf %s", testDir)
					output, err := runCmd(cmd, *attachedNode)
					Expect(err).NotTo(HaveOccurred(), output)
				})

				Step(fmt.Sprintf("verify resync done for volume %v (%v)", vol.ID, apiVol.Id), func() {
					Eventually(func() (string, error) {
						cmd := fmt.Sprintf("pxctl volume inspect %s | grep \"Replication Status\"", apiVol.Id)
						return runCmd(cmd, *resyncReplica)
					}, 10*time.Minute, 10*time.Second).Should(ContainSubstring("Up"),
						"resync not done for volume %v (%v) for app %v", vol.ID, apiVol.Id, ctx.App.Key)
				})
			}
		})

		AfterEach(func() {
			// Do the cleanup in case the test failed before doing this.
			Step("uncordon attachments on the other replica node", func() {
				if otherReplica == nil {
					return
				}
				cmd := fmt.Sprintf("pxctl service node uncordon-attachments --node  %s", otherReplica.Id)
				output, err := runCmd(cmd, *otherReplica)
				Expect(err).NotTo(HaveOccurred(), output)
			})
		})
	})

	// Template for additional tests
	// Context("{}", func() {
	// 	BeforeEach(func() {
	// 		testrailID = 0
	//      namespacePrefix = ""
	// 	})
	//
	// 	JustBeforeEach(func() {
	//		// since the apps are deployed by JustBeforeEach in the outer block,
	// 		// any test-specific changes to the deployed apps should go here.
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
				log.Info("not destroying apps because the test failed\n")
				return
			}
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test runs a ML Workload that predicts Housing rents. It contains one pod that preprocesses model
// and prepares training data. One pod runs continuously and keeps on retraining the model every 5 mins.
// The other pods that are deployments run once every 5 mins and read the model, do some prediction and
// writes their predictions to shared volume on which model is saved. Retraining pod picks this data up
// and again retrains the model. This is a simple feed-forward neural network model.
var _ = Describe("{CreateMlWorkloadOnSharedv4Svc}", func() {
	ns := "ml-workload-ns"
	appContexts := make([]*scheduler.Context, 0)
	prereqContexts := make([]*scheduler.Context, 0)
	taskName := "prepare-ml-workload"
	var origAppList []string
	totalMlDeps := 5
	totalRunTime := 5
	JustBeforeEach(func() {
		StartTorpedoTest("CreateMlWorkloadOnSharedv4Svc", "Create multiple pods coming and going and trying to edit/read a model on same volume", nil, 0)
		log.Infof("Original App list : %v", Inst().AppList)
		origAppList = Inst().AppList
		// Runs Preprocess Workload to prepare the model
		Inst().AppList = []string{"ml-workload-preprocess-rwx"}
		prereqContexts = append(prereqContexts, ScheduleApplicationsOnNamespace(ns, taskName)...)
		// Runs Continuous Retraining module
		Inst().AppList = []string{"ml-workload-continuous-training"}
		prereqContexts = append(prereqContexts, ScheduleApplicationsOnNamespace(ns, taskName)...)
		// Read Test Parameters
		totalMlWorkloads := ReadEnvVariable("NUM_ML_WORKLOADS")
		if totalMlWorkloads != "" {
			var err error
			totalMlDeps, err = strconv.Atoi(totalMlWorkloads)
			if err != nil {
				log.Errorf("Failed to convert value %v to int with error: %v", totalMlWorkloads, err)
				totalMlDeps = 5
			}
		}
		mlWorkloadRuntime := ReadEnvVariable("ML_WORKLOAD_RUNTIME")
		if mlWorkloadRuntime != "" {
			var err error
			totalRunTime, err = strconv.Atoi(mlWorkloadRuntime)
			if err != nil {
				log.Errorf("Failed to convert value %v to int with error: %v", mlWorkloadRuntime, err)
				totalRunTime = 5
			}
		}
	})
	It("Create Multiple ML Apps going and reading from the Model created. Continuous Retraining Module is already running", func() {
		// Defining deployment of querying ML Workload apps. This is necessary otherwise Torpedo
		// not create multiple same apps in same namespace. With this, we are changing the name of
		// ML App Deployment everytime it runs
		type Deployment struct {
			APIVersion string `yaml:"apiVersion"`
			Kind       string
			Metadata   struct {
				Name string
			}
			Spec struct {
				Replicas int
				Selector struct {
					MatchLabels map[string]string `yaml:"matchLabels"`
				}
				Template struct {
					Metadata struct {
						Labels map[string]string `yaml:"labels"`
					}
					Spec struct {
						Containers []struct {
							Name    string
							Image   string
							Command []string
							Args    []string `yaml:"args"`
							Env     []struct {
								Name  string
								Value string
							}
							VolumeMounts []struct {
								Name      string
								MountPath string `yaml:"mountPath"`
							} `yaml:"volumeMounts"`
						}
						Volumes []struct {
							Name                  string
							PersistentVolumeClaim struct {
								ClaimName string `yaml:"claimName"`
							} `yaml:"persistentVolumeClaim"`
						}
						ImagePullSecrets []struct {
							Name string
						} `yaml:"imagePullSecrets"`
					}
				}
			}
		}
		// Editing the original yaml file and bringing in new file with new name and OUTPUT_PARAMS
		currentDir, err := os.Getwd()
		log.FailOnError(err, "Failed to find current directory")
		filePath := filepath.Join(currentDir, "..", "drivers", "scheduler", "k8s", "specs", "ml-workload-rwx", "query-ml-workload.yaml")
		flag := false
		for i := 1; i <= totalMlDeps; i++ {
			data, err := os.ReadFile(filePath)
			log.FailOnError(err, fmt.Sprintf("Error Reading Yaml File: %v", filePath))
			var deployment Deployment
			err = yaml.Unmarshal(data, &deployment)
			log.FailOnError(err, fmt.Sprintf("Error Unmarshaling Yaml File: %v", filePath))
			appName := fmt.Sprintf("querying-app-%d", i)
			deployment.Metadata.Name = appName
			deployment.Spec.Selector.MatchLabels = map[string]string{"app": appName}
			deployment.Spec.Template.Metadata.Labels = map[string]string{"app": appName}
			for j := range deployment.Spec.Template.Spec.Containers {
				for k := range deployment.Spec.Template.Spec.Containers[j].Env {
					if deployment.Spec.Template.Spec.Containers[j].Env[k].Name == "OUTPUT_FILE" {
						deployment.Spec.Template.Spec.Containers[j].Env[k].Value = fmt.Sprintf("query_output_%d.csv", i)
					}
				}
			}
			opFilePath := filepath.Join(currentDir, "..", "drivers", "scheduler", "k8s", "specs", "ml-workload-rwx", "query-ml-workload.yaml")
			modifiedData, err := yaml.Marshal(&deployment)
			log.FailOnError(err, fmt.Sprintf("Error Marshaling back to Yaml File: %v", filePath))
			err = os.WriteFile(opFilePath, modifiedData, 0644)
			log.FailOnError(err, fmt.Sprintf("Error Writing to Yaml File: %v", filePath))
			provider := Inst().V.String()
			err = Inst().S.RescanSpecs(Inst().SpecDir, provider)
			log.FailOnError(err, "Failed to rescan specs from %s for storage provider %s", Inst().SpecDir, provider)
			// Running Several Workload pods that read the model, make some predictions and write them to shared vol
			Inst().AppList = []string{"ml-workload-rwx"}
			appContexts = append(appContexts, ScheduleApplicationsOnNamespace(ns, taskName)...)
			log.Infof("Successfully created App querying-app-%d", i)
			ValidateApplications(appContexts)
			if i == totalMlDeps {
				flag = true
			}
		}
		if flag {
			log.Infof("All Workload apps are now up. Will let them run for %d minutes", totalRunTime)
			time.Sleep(time.Duration(totalRunTime) * time.Minute)
		} else {
			log.Infof("Error happened with one of the workload apps. Validate from above logs.")
		}
	})
	JustAfterEach(func() {
		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		for _, ctx := range appContexts {
			TearDownContext(ctx, opts)
		}
		for _, ctx := range prereqContexts {
			TearDownContext(ctx, opts)
		}
		Inst().AppList = origAppList
		log.Infof("Restored original App list : %v", Inst().AppList)
	})
})

func ReadEnvVariable(envVar string) string {
	envValue, present := os.LookupEnv(envVar)
	if present {
		return envValue
	}
	return ""
}

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
	log.Infof("app counters snapshot 1 for volume %v: %v", vol.Id, snap1)

	log.Infof("sleeping for %v between the app counter snaps", sleepInterval)
	time.Sleep(sleepInterval)

	snap2 := getAppCountersSnapshot(vol, attachedNode)
	log.Infof("app counters snapshot 2 for volume %v: %v", vol.Id, snap2)
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
	dropCaches(attachedNode)
	counterByPodName := map[string]int{}
	// We find all the files in the root dir where the pods are writing their counters.
	// Example:
	// # find /var/lib/osd/pxns/1088228603475411556 -maxdepth 1 -type f -exec tail -1 {} \; -exec echo -n ':' \; -print
	// 1500:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-vmdn5
	// 2458:/var/lib/osd/pxns/1088228603475411556/common
	// 1404:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-m9kzc
	// 1436:/var/lib/osd/pxns/1088228603475411556/sv4test-5d849459d7-g79kh
	//
	cmd := fmt.Sprintf("find %s/%s -maxdepth 1 -type f -exec tail -1 {} \\; -exec echo -n \":\" \\; -print",
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

func dropCaches(attachedNode *node.Node) {
	output, err := runCmd("sync; echo 1 > /proc/sys/vm/drop_caches", *attachedNode)
	Expect(err).NotTo(HaveOccurred(), "failed to drop caches: %v", output)
}

// Validate the app counters after the failover.
//   - counters for all pods (with some exceptions) should continue incrementing
//   - the specified number of pods should have stopped incrementing their counters and the same number of
//     new pods should have started incrementing the counters
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

	for _, terminatedPod := range terminatedPods {
		_, err := core.Instance().GetPodByName(terminatedPod, ctx.GetID())
		Expect(err).To(Equal(errors.ErrPodsNotFound),
			"pod %v is expected to be terminated (not found) but did not get terminated: terminatedPods=%v, countersBefore=%v, countersAfter=%v",
			terminatedPod, terminatedPods, countersBefore, countersAfter)
	}

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
	logsByPodName, err := Inst().S.GetPodLog(ctx, 0, "sv4test")
	Expect(err).NotTo(HaveOccurred())
	Expect(len(logsByPodName)).To(Equal(numPods))
	var errors []string
	for podName, output := range logsByPodName {
		lines := strings.Split(output, "\n")
		for _, line := range lines {
			if strings.Contains(line, "ERROR") {
				errors = append(errors, fmt.Sprintf("pod %s: %s", podName, line))
			} else if strings.Contains(line, "WARNING") {
				log.Warnf("pod %s: %s", podName, line)
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
		clientsStr := strings.TrimPrefix(line, exportPathPrefix+"/"+vol.Id+" ")
		if clientsStr != line {
			// prefix was found
			return strings.Split(strings.TrimSpace(clientsStr), ",")
		}
	}
	return nil
}

func restartVolumeDriverOnNode(nodeObj *node.Node) {
	log.Infof("Stopping volume driver on node %s", nodeObj.Name)
	StopVolDriverAndWait([]node.Node{*nodeObj})

	dur := 30 * time.Second
	log.Infof("sleep for %v to allow the failover before restarting the volume driver on node %s", dur, nodeObj.Name)
	time.Sleep(dur)

	log.Infof("Starting volume driver on node %s", nodeObj.Name)
	StartVolDriverAndWait([]node.Node{*nodeObj})

	dur = 60 * time.Second
	log.Infof("sleep for %v to allow volume driver and the app pods to settle down on node %s", dur, nodeObj.Name)
	time.Sleep(dur)
}

func maintainVolumeDriverOnNode(nodeObj *node.Node) {
	log.Infof("Putting node %s in maintenance mode", nodeObj.Name)
	err := Inst().V.EnterMaintenance(*nodeObj)
	Expect(err).NotTo(HaveOccurred())

	dur := 30 * time.Second
	log.Infof("sleep for %v to allow the failover before exiting node %s from maintenance mode", dur, nodeObj.Name)
	time.Sleep(dur)

	err = Inst().V.ExitMaintenance(*nodeObj)
	Expect(err).NotTo(HaveOccurred())

	err = Inst().V.WaitDriverUpOnNode(*nodeObj, Inst().DriverStartTimeout)
	Expect(err).NotTo(HaveOccurred())
}

func rebootNodeAndWaitForReady(nodeObj *node.Node) {
	log.Infof("Rebooting node %s", nodeObj.Name)
	err := Inst().N.RebootNode(*nodeObj, node.RebootNodeOpts{
		Force: true,
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         cmdTimeout,
			TimeBeforeRetry: cmdRetry,
		},
	})
	Expect(err).NotTo(HaveOccurred())

	log.Infof("Testing connection to node %s", nodeObj.Name)
	err = Inst().N.TestConnection(*nodeObj, node.ConnectionOpts{
		Timeout:         testConnTimeout,
		TimeBeforeRetry: rebootRetry,
	})
	Expect(err).NotTo(HaveOccurred())

	log.Infof("Waiting for node %s to be ready", nodeObj.Name)
	err = Inst().S.IsNodeReady(*nodeObj)
	Expect(err).NotTo(HaveOccurred())

	log.Infof("Waiting for driver to be up on node %s", nodeObj.Name)
	err = Inst().V.WaitDriverUpOnNode(*nodeObj, Inst().DriverStartTimeout)
	Expect(err).NotTo(HaveOccurred())

	log.Infof("Successfully rebooted the node %s", nodeObj.Name)
}

func scaleApps(contexts []*scheduler.Context, numPods int) {
	for _, ctx := range contexts {
		scaleApp(ctx, numPods)
	}
}

func scaleApp(ctx *scheduler.Context, numPods int) {
	log.Infof("scaling app %s to %d", ctx.App.Key, numPods)
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
	currentRepl, err := Inst().V.GetReplicationFactor(vol)
	Expect(err).NotTo(HaveOccurred())

	if currentRepl == haLevel {
		log.Infof("HA level on volume %s is already %d", vol.ID, haLevel)
		return
	}

	log.Infof("setting HA level to 2 on volume %s", vol.ID)
	err = Inst().V.SetReplicationFactor(vol, haLevel, nil, nil, true)
	Expect(err).NotTo(HaveOccurred())

	log.Infof("validating successful update of HA level on volume %s", vol.ID)
	newRepl, err := Inst().V.GetReplicationFactor(vol)
	Expect(err).NotTo(HaveOccurred())
	Expect(newRepl).To(BeNumerically("==", haLevel))
}

func getReplicaNodeIDs(vol *volume.Volume) map[string]bool {
	replicaNodes := map[string]bool{}
	replicaSets, err := Inst().V.GetReplicaSets(vol)
	Expect(err).NotTo(HaveOccurred())
	for _, replicaSet := range replicaSets {
		for _, aNode := range replicaSet.Nodes {
			replicaNodes[aNode] = true
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
	log.Infof("deletion timestamp: %v", deletionTimestamp)
	return deletionTimestamp
}

func waitForNumPodsToEqual(ctx *scheduler.Context, numPods int) {
	var vol *volume.Volume
	var pods []corev1.Pod

	Eventually(func() (int, error) {
		var err error
		if vol == nil || vol.ID == "" {
			vols, err := Inst().S.GetVolumes(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(vols)).To(Equal(1))
			vol = vols[0]
		}
		if vol.ID == "" {
			// PVC not bound yet?
			return 0, fmt.Errorf("empty vol.ID in volume %v, PVC not bound?", vol)
		}
		pods, err = core.Instance().GetPodsUsingPV(vol.ID)
		return len(pods), err
	}, 3*time.Minute, 10*time.Second).Should(BeNumerically("==", numPods),
		"number of pods did not reach %v for app %v", numPods, ctx.App.Key)
}

func getSv4TestAppVol(ctx *scheduler.Context) (*volume.Volume, *api.Volume, *node.Node) {
	vols, err := Inst().S.GetVolumes(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(len(vols)).To(Equal(1))
	vol := vols[0]

	apiVol, err := Inst().V.InspectVolume(vol.ID)
	Expect(err).NotTo(HaveOccurred())

	attachedNode, err := Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
	Expect(err).NotTo(HaveOccurred())
	log.Infof("volume %v (%v) is attached to node %v", vol.ID, apiVol.Id, attachedNode.Name)

	return vol, apiVol, attachedNode
}

func startPacketCapture(filePath string) *sync.WaitGroup {
	var wg sync.WaitGroup

	log.Infof("capturing packet trace in file %s", filePath)
	// command below exits after 4 minutes since filecount -W is 1
	runForSeconds := 240
	cmd := fmt.Sprintf("tcpdump -i any -s 4096 -w %s -G %v -W 1 port 2049", filePath, runForSeconds)
	for _, aNode := range node.GetWorkerNodes() {
		aNode := aNode
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Infof("starting packet capture on node %s", aNode.Name)
			output, err := Inst().N.RunCommandWithNoRetry(aNode, cmd, node.ConnectionOpts{
				Timeout:         time.Duration(runForSeconds+60) * time.Second,
				TimeBeforeRetry: cmdRetry,
				Sudo:            true,
			})
			if err != nil {
				log.Warnf("failed to start tcpdump (%q) on node %v: %v: %v", cmd, aNode.Name, output, err)
			}
		}()
	}
	return &wg
}

func runCmd(cmd string, n node.Node) (string, error) {
	output, err := Inst().N.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         cmdTimeout,
		TimeBeforeRetry: cmdRetry,
		Sudo:            true,
	})
	if err != nil {
		log.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}
	return output, err
}
