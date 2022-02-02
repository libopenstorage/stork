package tests

import (
	"fmt"
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
)

const (
	defaultCommandRetry   = 5 * time.Second
	defaultCommandTimeout = 1 * time.Minute
	exportPathPrefix      = "/var/lib/osd/pxns/"
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
			nodeReplicaMap := make(map[string]bool)
			var volume *volume.Volume
			Step("disable scheduling on non replica nodes", func() {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())
				volume = vols[0]
				replicaSets, err := Inst().V.GetReplicaSets(volume)
				Expect(err).NotTo(HaveOccurred())
				for _, replicaSet := range replicaSets {
					for _, node := range replicaSet.Nodes {
						nodeReplicaMap[node] = true
					}
				}
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
				oldServer, err := Inst().V.GetNodeForVolume(volume, defaultCommandTimeout, defaultCommandRetry)
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
					server, err := Inst().V.GetNodeForVolume(volume, defaultCommandTimeout, defaultCommandRetry)
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

// Bring PX down on the node where the volume is attached. Verify that there is no I/O disruption.
var _ = Describe("{Shared4SvcFailoverIO}", func() {
	var testrailID = 54374
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/54374
	var runID int
	var contexts, testSv4Contexts []*scheduler.Context
	var numPods int

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to schedule apps and stop volume driver on nodes where volumes are attached", func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("failover-io-%d", i))...)
		}

		Step("scale the test-sv4-svc apps so that one pod runs on each worker node", func() {
			testSv4Contexts = getTestSv4Contexts(contexts)
			if len(testSv4Contexts) == 0 {
				Skip("No test-sv4-svc apps were found")
			}
			numPods = len(node.GetWorkerNodes())
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("scale up app %s to %d", ctx.App.Key, numPods), func() {
					applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					for name := range applicationScaleUpMap {
						applicationScaleUpMap[name] = int32(numPods)
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
					Expect(err).NotTo(HaveOccurred())
				})
			}
		})

		ValidateApplications(contexts)

		Step("set HA level to 2 to verify failover and failback to the same node", func() {
			for _, ctx := range testSv4Contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())
				Step(
					fmt.Sprintf("set HA level to 2 on app %s's volume %v", ctx.App.Key, vols[0]),
					func() {
						err := Inst().V.SetReplicationFactor(vols[0], 2, nil)
						Expect(err).NotTo(HaveOccurred())
					})
				Step(
					fmt.Sprintf("validate successful update of HA level on app %s's volume %v", ctx.App.Key, vols[0]),
					func() {
						newRepl, err := Inst().V.GetReplicationFactor(vols[0])
						Expect(err).NotTo(HaveOccurred())
						Expect(newRepl).To(BeNumerically("==", 2))
						// ValidateContext() will fail with error "volume has invalid repl value. Expected:3 Actual:2"
						// without the line below.
						ctx.SkipVolumeValidation = true
					})
			}
		})

		Step("for each context, restart volume driver on NFS server node and verify I/O", func() {
			for _, ctx := range testSv4Contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())

				apiVol, err := Inst().V.InspectVolume(vols[0].ID)
				Expect(err).NotTo(HaveOccurred())

				// verify failover and failback by repeating the steps below
				for i := 0; i < 2; i++ {
					var countersBefore, countersAfter map[string]appCounter
					var attachedNodeBefore, attachedNodeAfter *node.Node

					// get the attached node before the failover
					attachedNodeBefore, err = Inst().V.GetNodeForVolume(vols[0],
						defaultCommandTimeout, defaultCommandRetry)
					Expect(err).NotTo(HaveOccurred())

					Step(
						fmt.Sprintf("get counter values from node %v for app %s's volume before failover",
							attachedNodeBefore.Name, ctx.App.Key),
						func() {
							countersBefore = getAppCounters(apiVol, attachedNodeBefore,
								3*time.Duration(numPods)*time.Second)
						})

					Step(
						fmt.Sprintf("failover #%d for app %s by stopping the volume driver on node %s",
							i, ctx.App.Key, attachedNodeBefore.Name),
						func() {
							StopVolDriverAndWait([]node.Node{*attachedNodeBefore})
						})

					Step(
						fmt.Sprintf("start volume driver %s on node where app %s's volume was attached: %s",
							Inst().V.String(), ctx.App.Key, attachedNodeBefore.Name),
						func() {
							StartVolDriverAndWait([]node.Node{*attachedNodeBefore})
						})

					Step("Giving few seconds for volume driver and the app pods to stabilize",
						func() {
							time.Sleep(60 * time.Second)
						})

					Step(fmt.Sprintf("validate app %s", ctx.App.Key),
						func() {
							ValidateContext(ctx)
						})

					Step(
						fmt.Sprintf("get counter values for app %s's volume after failover #%d", ctx.App.Key, 0),
						func() {
							attachedNodeAfter, err = Inst().V.GetNodeForVolume(vols[0],
								defaultCommandTimeout, defaultCommandRetry)
							Expect(err).NotTo(HaveOccurred())
							Expect(attachedNodeAfter.Name).NotTo(Equal(attachedNodeBefore.Name))
							countersAfter = getAppCounters(apiVol, attachedNodeAfter,
								3*time.Duration(numPods)*time.Second)
						})

					Step(fmt.Sprintf("validate no I/O disruption for app %s after failover #%d", ctx.App.Key, i),
						func() {
							// 2 pods (1 on the old NFS server and 1 on the new NFS server) should get restarted,
							// others should not be interrupted
							validateAppCounters(ctx, countersBefore, countersAfter, numPods, 2)
							validateAppLogs(ctx, numPods)
							validateExports(apiVol, attachedNodeBefore, attachedNodeAfter)
						})
				}
			}
		})

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})
	JustAfterEach(func() {
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Bring PX down on the node where the volume is attached. Verify that pods on client nodes unmount the volume
// and teardown successfully
var _ = Describe("{Sharedv4ClientTeardownWhenServerOffline}", func() {
	var testrailID = 54780
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/54780
	var runID, numPods int
	var testSv4Contexts, contexts []*scheduler.Context

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to schedule apps, stop volume driver on node where volume is attached, teardown the application", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("clientteardown-%d", i))...)
		}
		testSv4Contexts = getTestSv4Contexts(contexts)

		Step("scale the sharedv4 apps so that one pod runs on each worker node", func() {
			if len(testSv4Contexts) == 0 {
				Skip("No sharedv4 apps were found")
			}
			numPods = len(node.GetWorkerNodes())
			for _, ctx := range testSv4Contexts {
				Step(fmt.Sprintf("scale up app %s to %d", ctx.App.Key, numPods), func() {
					applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					for name := range applicationScaleUpMap {
						applicationScaleUpMap[name] = int32(numPods)
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
					Expect(err).NotTo(HaveOccurred())
				})
			}

			ValidateApplications(testSv4Contexts)
		})

		Step("change the sharedv4 failover strategy to normal", func() {
			for _, ctx := range testSv4Contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred(), "failed in getting volumes: %v", err)

				err = Inst().V.UpdateSharedv4FailoverStrategyUsingPxctl(vols[0].ID, api.Sharedv4FailoverStrategy_NORMAL)
				Expect(err).NotTo(HaveOccurred(), "failed in updating sharedv4 strategy for volume %v: %v", vols[0].ID, err)

				apiVol, err := Inst().V.InspectVolume(vols[0].ID)
				Expect(err).NotTo(HaveOccurred(), "failed in inspect volume: %v", err)
				Expect(apiVol.Spec.Sharedv4Spec.FailoverStrategy == api.Sharedv4FailoverStrategy_NORMAL).To(BeTrue(), "unexpected failover strategy")
			}
		})

		var attachedNode *node.Node
		Step("stop the volume driver on attached node and verify application teardown succeeds", func() {
			for _, ctx := range testSv4Contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())

				attachedNode, err = Inst().V.GetNodeForVolume(
					vols[0],
					defaultCommandTimeout,
					defaultCommandRetry,
				)
				Expect(err).NotTo(HaveOccurred())

				Step(
					fmt.Sprintf("stopping volume driver on node %s", attachedNode.Name),
					func() {
						StopVolDriverAndWait([]node.Node{*attachedNode})
					},
				)

				Step(fmt.Sprintf("scale down app %s to 0", ctx.App.Key), func() {
					applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					for name := range applicationScaleUpMap {
						applicationScaleUpMap[name] = int32(0)
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
					Expect(err).NotTo(HaveOccurred())
				})

				Step(
					fmt.Sprintf("ensure client pods have terminated"),
					func() {
						err = Inst().S.SelectiveWaitForTermination(
							ctx,
							Inst().DestroyAppTimeout,
							[]node.Node{*attachedNode},
						)
						Expect(err).NotTo(HaveOccurred())
					},
				)
				Step(
					fmt.Sprintf("ensure volume is detached"),
					func() {
						vols, err := Inst().S.GetVolumes(ctx)
						Expect(err).NotTo(HaveOccurred(), "failed in getting volumes: %v", err)

						apiVol, err := Inst().V.InspectVolume(vols[0].ID)
						Expect(err).NotTo(HaveOccurred(), "failed in inspect volume: %v", err)
						Expect(len(apiVol.AttachedOn) == 0).To(BeTrue(), "expected volume %v to be detached", vols[0].Name)
					},
				)
				Step(
					fmt.Sprintf("starting volume driver on node %s", attachedNode.Name),
					func() {
						StartVolDriverAndWait([]node.Node{*attachedNode})
					},
				)

				numPods = len(node.GetWorkerNodes())
				Step(fmt.Sprintf("scale up app %s to %d", ctx.App.Key, numPods), func() {
					applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					for name := range applicationScaleUpMap {
						applicationScaleUpMap[name] = int32(numPods)
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
					Expect(err).NotTo(HaveOccurred())
				})

				ValidateApplications([]*scheduler.Context{ctx})

			}
		})

		Step("change the sharedv4 failover strategy back to aggressive", func() {
			for _, ctx := range testSv4Contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred(), "failed in getting volumes: %v", err)

				err = Inst().V.UpdateSharedv4FailoverStrategyUsingPxctl(vols[0].ID, api.Sharedv4FailoverStrategy_AGGRESSIVE)
				Expect(err).NotTo(HaveOccurred(), "failed in updating sharedv4 strategy for volume %v: %v", vols[0].ID, err)

				apiVol, err := Inst().V.InspectVolume(vols[0].ID)
				Expect(err).NotTo(HaveOccurred(), "failed in inspect volume: %v", err)
				Expect(apiVol.Spec.Sharedv4Spec.FailoverStrategy == api.Sharedv4FailoverStrategy_AGGRESSIVE).To(BeTrue(), "unexpected failover strategy")
			}
		})

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
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
	// Exclude the common file, tmp files. Use grep '' to get the lines in "fileName:counter" format.
	// Example:
	// # find /var/lib/osd/pxns/283170809327294682 -maxdepth 1 -type f | grep -v -e common -e tmp | xargs grep ''
	// /var/lib/osd/pxns/283170809327294682/sv4test-88d78b767-n5gkc:478941
	// /var/lib/osd/pxns/283170809327294682/sv4test-88d78b767-5bgxn:5437
	// /var/lib/osd/pxns/283170809327294682/sv4test-88d78b767-48br7:521419
	//
	cmd := fmt.Sprintf("find %s%s -maxdepth 1 -type f | grep -v -e common -e tmp | xargs grep ''",
		exportPathPrefix, vol.Id)
	output, err := runCmd(cmd, *attachedNode)
	Expect(err).NotTo(HaveOccurred())
	for _, line := range strings.Split(output, "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, ":")
		Expect(len(parts)).To(Equal(2))

		val, err := strconv.Atoi(parts[1])
		Expect(err).NotTo(HaveOccurred())

		podName := path.Base(parts[0])
		counterByPodName[podName] = val
	}
	return counterByPodName
}

// Validate the app counters after the failover.
// - counters for all pods (with some exceptions) should continue incrementing
// - the specified number of pods should have stopped incrementing their counters and the same number of
//   new pods should have started incrementing the counters
func validateAppCounters(ctx *scheduler.Context, countersBefore, countersAfter map[string]appCounter,
	numPods, expectedPodRestarts int) {

	var survivingPods []string
	var terminatedPods []string
	var newPods []string

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
	// pods on old and new NFS server should have been terminated and new ones should have replaced them
	Expect(len(terminatedPods)).To(Equal(expectedPodRestarts))
	Expect(len(newPods)).To(Equal(expectedPodRestarts))
	Expect(len(survivingPods) + len(newPods)).To(Equal(numPods))
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

func runCmd(cmd string, n node.Node) (string, error) {
	output, err := Inst().N.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         defaultCommandTimeout,
		TimeBeforeRetry: defaultCommandRetry,
		Sudo:            true,
	})
	if err != nil {
		logrus.Warnf("failed to run cmd: %s. err: %v", cmd, err)
	}
	return output, err
}
