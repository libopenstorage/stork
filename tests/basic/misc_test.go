package tests

import (
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"

	opsapi "github.com/libopenstorage/openstorage/api"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/portworx/torpedo/pkg/pureutils"

	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var k8sCore = core.Instance()

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{SetupTeardown}", func() {
	var testrailID = 35258
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35258
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("SetupTeardown", "Validate setup tear down", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context

	It("has to setup, validate and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("setupteardown-%d", i))...)
		}
		ValidateApplications(contexts)

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func pureWriteRoutine(ctx *scheduler.Context, podName string, dataDir string, shouldStop *bool, errOutChan chan error) {
	for {
		if *shouldStop {
			return
		}
		// Proceed to the write

		// Get the current time in unix timestamp
		filename := fmt.Sprintf("purewritetest-%s-%d", podName, time.Now().Unix())
		log.Debugf("Writing to pod '%s' in namespace '%s' in data dir '%s'. This will be filename %s", podName, ctx.App.NameSpace, dataDir, filename)
		cmdArgs := []string{"exec", "-it", podName, "-n", ctx.App.NameSpace, "--", "touch", path.Join(dataDir, filename)}
		err = osutils.Kubectl(cmdArgs)
		if err != nil {
			log.Errorf("Error writing to pod '%s' in namespace '%s' in data dir '%s': %v", podName, ctx.App.NameSpace, dataDir, err)
			errOutChan <- fmt.Errorf("Error writing to pod '%s' in namespace '%s' in data dir '%s': %v", podName, ctx.App.NameSpace, dataDir, err)
			return
		}

		// Sleep for 10 seconds
		time.Sleep(10 * time.Second)
	}
}

func StartPureBackgroundWriteRoutines() func() {
	pureStopWriteRoutine := false
	pureErrOutChan := make(chan error, 1) // We only need one failure to fail the entire test: no reason to store more than we need

	Step("start write routine on all Pure volumes to ensure data continuity", func() {
		for _, ctx := range contexts {
			var vols []*volume.Volume
			Step(fmt.Sprintf("get %s app's volumes", ctx.App.Key), func() {
				vols, err = Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
			})

			podNames := map[string]bool{}
			for _, vol := range vols {
				pods, err := Inst().S.GetPodsForPVC(vol.Name, vol.Namespace)
				log.FailOnError(err, "Failed to get pods for PVC %s in app %s", vol.Name, ctx.App.Key)
				for _, pod := range pods {
					podNames[pod.Name] = true
				}
			}

			dataDir, _ := pureutils.GetAppDataDir(ctx.App.NameSpace)
			for podName := range podNames {
				// Start a routine that will repeatedly write to this pod's data directory until we send something to the stop channel
				// If any errors occur, they will be sent to the err channel, and checked at the end of the test
				log.Infof("Starting background write routine for pod '%s' in namespace '%s' in data dir '%s'", podName, ctx.App.NameSpace, dataDir)
				go pureWriteRoutine(ctx, podName, dataDir, &pureStopWriteRoutine, pureErrOutChan)
			}
		}
	})
	return func() {
		// Finish up the write routines from earlier
		// First, check for any errors
		var err error
		select {
		case err = <-pureErrOutChan:
			log.FailOnError(err, "Error writing to Pure volume")
		default:
			log.Infof("No errors found in error channel for Pure volume write validation")
		}
		// Then, close all the routines out so they stop writing
		pureStopWriteRoutine = true
	}
}

// Volume Driver Plugin is down, unavailable - and the client container should not be impacted.
var _ = Describe("{VolumeDriverDown}", func() {
	var testrailID = 35259
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35259
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeDriverDown", "Validate volume driver down", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to schedule apps and stop volume driver on app nodes"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverdown-%d", i))...)
		}

		ValidateApplications(contexts)

		var pureCleanupFunction func()
		if Inst().V.String() == portworx.PureDriverName {
			pureCleanupFunction = StartPureBackgroundWriteRoutines()
		}

		Step("get nodes bounce volume driver", func() {
			for _, appNode := range node.GetStorageDriverNodes() {
				stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
					Inst().V.String(), appNode.Name)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						StopVolDriverAndWait([]node.Node{appNode})
					})

				stepLog = fmt.Sprintf("starting volume %s driver on node %s",
					Inst().V.String(), appNode.Name)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						StartVolDriverAndWait([]node.Node{appNode})
					})

				stepLog = "Giving few seconds for volume driver to stabilize"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					time.Sleep(20 * time.Second)
				})

				Step("validate apps", func() {
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})
			}

			if pureCleanupFunction != nil {
				pureCleanupFunction() // Checks for any errors during the background writes and fails the test if any occurred
				return
			}

			err := ValidateDataIntegrity(&contexts)
			log.FailOnError(err, "error validating data integrity")
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
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Volume Driver Plugin is down, unavailable on the nodes where the volumes are
// attached - and the client container should not be impacted.
var _ = Describe("{VolumeDriverDownAttachedNode}", func() {
	var testrailID = 35260
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35260
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeDriverDownAttachedNode", "Validate Volume drive down on an volume attached node", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to schedule apps and stop volume driver on nodes where volumes are attached"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverdownattachednode-%d", i))...)
		}

		ValidateApplications(contexts)

		stepLog = "get nodes where app is running and restart volume driver"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				appNodes, err := Inst().S.GetNodesForApp(ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verify Get nodes for app %s", ctx.App.Key))
				for _, appNode := range appNodes {
					stepLog = fmt.Sprintf("stop volume driver %s on app %s's node: %s",
						Inst().V.String(), ctx.App.Key, appNode.Name)
					Step(stepLog,
						func() {
							StopVolDriverAndWait([]node.Node{appNode})
						})

					stepLog = fmt.Sprintf("starting volume %s driver on app %s's node %s",
						Inst().V.String(), ctx.App.Key, appNode.Name)
					Step(stepLog,
						func() {
							StartVolDriverAndWait([]node.Node{appNode})
						})

					stepLog = "Giving few seconds for volume driver to stabilize"
					Step(stepLog, func() {
						log.InfoD("Giving few seconds for volume driver to stabilize")
						time.Sleep(20 * time.Second)
					})

					stepLog = fmt.Sprintf("validate app %s", ctx.App.Key)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						ValidateContext(ctx)
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
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Volume Driver Plugin has crashed - and the client container should not be impacted.
var _ = Describe("{VolumeDriverCrash}", func() {
	var testrailID = 35261
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35261
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeDriverCrash", "Validate PX after volume driver crash", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to schedule apps and crash volume driver on app nodes"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldrivercrash-%d", i))...)
		}

		ValidateApplications(contexts)

		var pureCleanupFunction func()
		if Inst().V.String() == portworx.PureDriverName {
			pureCleanupFunction = StartPureBackgroundWriteRoutines()
		}

		stepLog = "crash volume driver in all nodes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appNode := range node.GetStorageDriverNodes() {
				stepLog = fmt.Sprintf("crash volume driver %s on node: %v",
					Inst().V.String(), appNode.Name)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						CrashVolDriverAndWait([]node.Node{appNode})
					})
			}
		})

		if pureCleanupFunction != nil {
			pureCleanupFunction() // Checks for any errors during the background writes and fails the test if any occurred
			return
		}

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Volume driver plugin is down and the client container gets terminated.
// There is a lost unmount call in this case. When the volume driver is
// back up, we should be able to detach and delete the volume.
var _ = Describe("{VolumeDriverAppDown}", func() {
	var testrailID = 35262
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35262
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeDriverAppDown", "Validate volume driver down and app deletion", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to schedule apps, stop volume driver on app nodes and destroy apps"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverappdown-%d", i))...)
		}

		ValidateApplications(contexts)

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		stepLog = "get nodes for all apps in test and bounce volume driver"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				appNodes, err := Inst().S.GetNodesForApp(ctx)
				log.FailOnError(err, "Failed to get nodes for the app %s", ctx.App.Key)
				appNode := appNodes[r.Intn(len(appNodes))]
				stepLog = fmt.Sprintf("stop volume driver %s on app %s's nodes: %v",
					Inst().V.String(), ctx.App.Key, appNode)
				Step(stepLog, func() {
					StopVolDriverAndWait([]node.Node{appNode})
				})

				stepLog = fmt.Sprintf("destroy app: %s", ctx.App.Key)
				Step(stepLog, func() {
					err = Inst().S.Destroy(ctx, nil)
					dash.VerifyFatal(err, nil, "Verify App delete")
					stepLog = "wait for few seconds for app destroy to trigger"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						time.Sleep(10 * time.Second)
					})
				})

				stepLog = "restarting volume driver"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					StartVolDriverAndWait([]node.Node{appNode})
				})

				stepLog = fmt.Sprintf("wait for destroy of app: %s", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					err = Inst().S.WaitForDestroy(ctx, Inst().DestroyAppTimeout)
					dash.VerifySafely(err, nil, fmt.Sprintf("Verify App %s deletion", ctx.App.Key))
				})

				err = DeleteVolumesAndWait(ctx, nil)
				dash.VerifySafely(err, nil, fmt.Sprintf("%s's volume deleted successfully?", ctx.App.Key))

			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test deletes all tasks of an application and checks if app converges back to desired state
var _ = Describe("{AppTasksDown}", func() {
	var testrailID = 35263
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35264
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("AppTasksDown", "Validate app after tasks are deleted", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to schedule app and delete app tasks"
	It(stepLog, func() {
		log.InfoD(stepLog)
		var err error
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("apptasksdown-%d", i))...)
		}

		ValidateApplications(contexts)

		stepLog = "delete all application tasks"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			// Add interval based sleep here to check what time we will exit out of this delete task loop
			minRunTime := Inst().MinRunTimeMins
			timeout := (minRunTime) * 60
			// set frequency mins depending on the chaos level
			var frequency int
			switch Inst().ChaosLevel {
			case 5:
				frequency = 1
			case 4:
				frequency = 3
			case 3:
				frequency = 5
			case 2:
				frequency = 7
			case 1:
				frequency = 10
			default:
				frequency = 10

			}
			if minRunTime == 0 {
				for _, ctx := range contexts {
					stepLog = fmt.Sprintf("delete tasks for app: %s", ctx.App.Key)
					Step(stepLog, func() {
						err = Inst().S.DeleteTasks(ctx, nil)
						if err != nil {
							PrintDescribeContext(ctx)
						}
						dash.VerifyFatal(err, nil, fmt.Sprintf("validate delete tasks for app: %s", ctx.App.Key))
					})

					ValidateContext(ctx)
				}
			} else {
				start := time.Now().Local()
				for int(time.Since(start).Seconds()) < timeout {
					for _, ctx := range contexts {
						stepLog = fmt.Sprintf("delete tasks for app: %s", ctx.App.Key)
						Step(stepLog, func() {
							err = Inst().S.DeleteTasks(ctx, nil)
							if err != nil {
								PrintDescribeContext(ctx)
							}
							dash.VerifyFatal(err, nil, fmt.Sprintf("validate delete tasks for app: %s", ctx.App.Key))
						})

						ValidateContext(ctx)
					}
					stepLog = fmt.Sprintf("Sleeping for given duration %d", frequency)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						d := time.Duration(frequency)
						time.Sleep(time.Minute * d)
					})
				}
			}
		})

		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test scales up and down an application and checks if app has actually scaled accordingly
var _ = Describe("{AppScaleUpAndDown}", func() {
	var testrailID = 35264
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35264
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("AppScaleUpAndDown", "Validate Apps sclae up and scale down", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to scale up and scale down the app"
	It(stepLog, func() {
		log.InfoD("has to scale up and scale down the app")
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("applicationscaleupdown-%d", i))...)
		}

		ValidateApplications(contexts)

		stepLog = "Scale up and down all app"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				stepLog = fmt.Sprintf("scale up app: %s by %d ", ctx.App.Key, len(node.GetWorkerNodes()))
				Step(stepLog, func() {
					log.InfoD(stepLog)
					applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
					log.FailOnError(err, "Failed to get application scale up factor map")
					//Scaling up by number of storage-nodes
					workerStorageNodes := int32(len(node.GetStorageNodes()))
					for name, scale := range applicationScaleUpMap {
						// limit scale up to the number of worker nodes
						if scale < workerStorageNodes {
							applicationScaleUpMap[name] = workerStorageNodes
						}
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
					dash.VerifyFatal(err, nil, "Validate application scale up")
				})

				stepLog = "Giving few seconds for scaled up applications to stabilize"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					time.Sleep(10 * time.Second)
				})

				ValidateContext(ctx)

				stepLog = fmt.Sprintf("scale down app %s by 1", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					applicationScaleDownMap, err := Inst().S.GetScaleFactorMap(ctx)
					log.FailOnError(err, "Failed to get application scale down factor map")

					for name, scale := range applicationScaleDownMap {
						applicationScaleDownMap[name] = scale - 1
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleDownMap)
					dash.VerifyFatal(err, nil, "Validate application scale down")
				})

				stepLog = "Giving few seconds for scaled up applications to stabilize"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					time.Sleep(10 * time.Second)
				})

				ValidateContext(ctx)
			}
		})

		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{CordonDeployDestroy}", func() {
	var testrailID = 54373
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/54373
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("CordonDeployDestroy", "Validate Cordon node and destroy app", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context

	stepLog := "has to cordon all nodes but one, deploy and destroy app"
	It(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "Cordon all nodes but one"

		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodes := node.GetStorageDriverNodes()
			for _, node := range nodes[1:] {
				err := Inst().S.DisableSchedulingOnNode(node)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate disable scheduling on node %s", node.Name))

			}
		})
		stepLog = "Deploy applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("cordondeploydestroy-%d", i))...)
			}
			ValidateApplications(contexts)

		})
		stepLog = "Destroy apps"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForDestroy] = false
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = false
			for _, ctx := range contexts {
				err := Inst().S.Destroy(ctx, opts)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate App %s detroy init", ctx.App.Key))
			}
		})
		Step("Validate destroy", func() {
			for _, ctx := range contexts {
				err := Inst().S.WaitForDestroy(ctx, Inst().DestroyAppTimeout)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate App %s detroy", ctx.App.Key))

			}
		})
		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
		Step("Uncordon all nodes", func() {
			nodes := node.GetStorageDriverNodes()
			for _, node := range nodes {
				err := Inst().S.EnableSchedulingOnNode(node)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate enable scheduling on node %s", node.Name))
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)

	})
})

var _ = Describe("{CordonStorageNodesDeployDestroy}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("CordonStorageNodesDeployDestroy", "Validate Cordon storage node , deploy and destroy app", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "has to cordon all storage nodes, deploy and destroy app"
	It(stepLog, func() {
		log.InfoD(stepLog)
		stepLog = "Cordon all storage nodes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodes := node.GetNodes()
			storageNodes := node.GetStorageNodes()
			if len(nodes) == len(storageNodes) {
				stepLog = "No storageless nodes detected. Skipping.."
				log.Warn(stepLog)
				Skip(stepLog)
			}
			for _, n := range storageNodes {
				err := Inst().S.DisableSchedulingOnNode(n)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate disable scheduling on node %s", n.Name))
			}
		})
		stepLog = "Deploy applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("cordondeploydestroy-%d", i))...)
			}
			ValidateApplications(contexts)

		})
		stepLog = "Destroy apps"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForDestroy] = false
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = false
			for _, ctx := range contexts {
				err := Inst().S.Destroy(ctx, opts)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate App %s detroy init", ctx.App.Key))
			}
		})
		Step("Validate destroy", func() {
			for _, ctx := range contexts {
				err := Inst().S.WaitForDestroy(ctx, Inst().DestroyAppTimeout)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate App %s detroy", ctx.App.Key))

			}
		})
		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
		Step("Uncordon all nodes", func() {
			nodes := node.GetStorageDriverNodes()
			for _, node := range nodes {
				err := Inst().S.EnableSchedulingOnNode(node)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate enable scheduling on node %s", node.Name))
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{SecretsVaultFunctional}", func() {
	var testrailID, runID int
	var contexts []*scheduler.Context
	var provider string

	const (
		vaultSecretProvider        = "vault"
		vaultTransitSecretProvider = "vault-transit"
		portworxContainerName      = "portworx"
	)

	BeforeEach(func() {
		StartTorpedoTest("SecretsVaultFunctional", "Validate Secrets Vault", nil, 0)
		isOpBased, _ := Inst().V.IsOperatorBasedInstall()
		if !isOpBased {
			k8sApps := apps.Instance()
			daemonSets, err := k8sApps.ListDaemonSets("kube-system", metav1.ListOptions{
				LabelSelector: "name=portworx",
			})
			log.FailOnError(err, "Failed to get daemon sets list")
			dash.VerifyFatal(len(daemonSets) > 0, true, "Daemon sets returned?")
			dash.VerifyFatal(len(daemonSets[0].Spec.Template.Spec.Containers) > 0, true, "Daemon set container is not empty?")
			usingVault := false
			for _, container := range daemonSets[0].Spec.Template.Spec.Containers {
				if container.Name == portworxContainerName {
					for _, arg := range container.Args {
						if arg == vaultSecretProvider || arg == vaultTransitSecretProvider {
							usingVault = true
							provider = arg
						}
					}
				}
			}
			if !usingVault {
				skipLog := fmt.Sprintf("Skip test for not using %s or %s ", vaultSecretProvider, vaultTransitSecretProvider)
				log.Warn(skipLog)
				Skip(skipLog)
			}
		} else {
			spec, err := Inst().V.GetDriver()
			log.FailOnError(err, "Failed to get storage cluster")
			if *spec.Spec.SecretsProvider != vaultSecretProvider &&
				*spec.Spec.SecretsProvider != vaultTransitSecretProvider {
				Skip(fmt.Sprintf("Skip test for not using %s or %s ", vaultSecretProvider, vaultTransitSecretProvider))
			}
			provider = *spec.Spec.SecretsProvider
		}
	})

	var _ = Describe("{RunSecretsLogin}", func() {
		testrailID = 82774
		// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/82774
		JustBeforeEach(func() {
			StartTorpedoTest("RunSecretsLogin", "Test secrets login for vaults", nil, 0)
			runID = testrailuttils.AddRunsToMilestone(testrailID)
		})

		stepLog := "has to run secrets login for vault or vault-transit"

		It(stepLog, func() {
			log.InfoD(stepLog)
			contexts = make([]*scheduler.Context, 0)
			n := node.GetStorageDriverNodes()[0]
			if provider == vaultTransitSecretProvider {
				// vault-transit login with `pxctl secrets vaulttransit login`
				provider = "vaulttransit"
			}
			err := Inst().V.RunSecretsLogin(n, provider)
			dash.VerifyFatal(err, nil, "Validate secrets login")
		})
	})

	AfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{VolumeCreatePXRestart}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeCreatePXRestart", "Validate restart PX while create and attach", nil, 0)

	})
	contexts := make([]*scheduler.Context, 0)

	stepLog := "Validate volume attachment when px is restarting"
	It(stepLog, func() {
		var createdVolIDs map[string]string
		var err error
		volCreateCount := 10
		stepLog := "Create multiple volumes , attached and restart PX"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			stNodes := node.GetStorageNodes()
			index := rand.Intn(len(stNodes))
			selectedNode := stNodes[index]

			log.InfoD("Creating and attaching %d volumes on node %s", volCreateCount, selectedNode.Name)

			wg := new(sync.WaitGroup)
			wg.Add(1)
			go func(appNode node.Node) {
				createdVolIDs, err = CreateMultiVolumesAndAttach(wg, volCreateCount, selectedNode.Id)
				if err != nil {
					log.Fatalf("Error while creating volumes. Err: %v", err)
				}
			}(selectedNode)
			time.Sleep(2 * time.Second)
			wg.Add(1)
			go func(appNode node.Node) {
				defer wg.Done()
				stepLog = fmt.Sprintf("restart volume driver %s on node: %s", Inst().V.String(), appNode.Name)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					err = Inst().V.RestartDriver(appNode, nil)
					log.FailOnError(err, "Error while restarting volume driver")

				})
			}(selectedNode)
			wg.Wait()

		})

		stepLog = "Validate the created volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			for vol, volPath := range createdVolIDs {
				cVol, err := Inst().V.InspectVolume(vol)
				if err == nil {
					dash.VerifySafely(cVol.State, opsapi.VolumeState_VOLUME_STATE_ATTACHED, fmt.Sprintf("Verify vol %s is attached", cVol.Id))
					dash.VerifySafely(cVol.DevicePath, volPath, fmt.Sprintf("Verify vol %s is has device path", cVol.Id))
				} else {
					log.Fatalf("Error while inspecting volume %s. Err: %v", vol, err)
				}
			}
		})

		stepLog = "Deleting the created volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			for vol := range createdVolIDs {
				log.Infof("Detaching and deleting volume: %s", vol)
				err := Inst().V.DetachVolume(vol)
				if err == nil {
					err = Inst().V.DeleteVolume(vol)
				}
				log.FailOnError(err, "Error while deleting volume %s", vol)

			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{AutoFSTrimReplAddWithNoPool0}", func() {
	/*
		1. Enable autofstrim, wait until autofstrim is actively trimming.
		2. Run back to back cmd `pxctl c options update --auto-fstrim off` , then  `pxctl c options update --auto-fstrim on`
		3.`pxctl v af status` should report the trim status correctly
		4. All Nodes have pool 0 and pool 1.
		5. Delete pool 0 in one of the node
		6. Create repl-2 volumes on nodes with pool 0 and pool 1.
		7. Check auto-fstrim is working
		8. Do a repl-add on all volumes on the node with only pool 1
		9. Check if auto-fstrim is working
	*/
	var testrailID = 84604
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/84604
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("AutoFSTrimReplAddWithNoPool0", "validate autofstrim with repl add with no pool 0", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to check the autofstrim status with fast switch and repl add"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		storageDriverNodes := node.GetStorageDriverNodes()
		stepLog = "perform fast switch on the autofstrim option"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			stNode := storageDriverNodes[0]
			clusterOpts, err := Inst().V.GetClusterOpts(stNode, []string{"AutoFstrim"})
			log.FailOnError(err, fmt.Sprintf("error getting AutoFstrim status using node [%s]", stNode.Name))
			if clusterOpts["AutoFstrim"] == "false" {
				EnableAutoFSTrim()
				//making sure autofstrim is actively running
				time.Sleep(1 * time.Minute)
			}

			//switching autofstrim feature
			err = Inst().V.SetClusterOpts(stNode, map[string]string{
				"--auto-fstrim": "off"})
			log.FailOnError(err, "error disabling AutoFstrim status using node [%s]", stNode.Name)
			err = Inst().V.SetClusterOpts(stNode, map[string]string{
				"--auto-fstrim": "on"})
			log.FailOnError(err, "error enabling AutoFstrim status using node [%s]", stNode.Name)
			_, err = Inst().V.GetAutoFsTrimStatus(stNode.DataIp)
			dash.VerifyFatal(err, nil, fmt.Sprintf("verify autofstrim status with fast switch from node [%v]", stNode.Name))

			//Check autofstrim using PXCTL
			opts := node.ConnectionOpts{
				IgnoreError:     false,
				TimeBeforeRetry: defaultRetryInterval,
				Timeout:         defaultTimeout,
				Sudo:            true,
			}

			output, err := Inst().V.GetPxctlCmdOutputConnectionOpts(stNode, "v af status", opts, false)
			log.FailOnError(err, "error checking autofstrim status on node [%s]", stNode.Name)
			log.Infof("autofstrim status: %s", output)
			dash.VerifyFatal(strings.Contains(output, "Filesystem Trim Initializing"), false, "verify no autofstrim initialization issue")

		})

		if !Contains(Inst().AppList, "fio-fstrim") {
			Inst().AppList = append(Inst().AppList, "fio-fstrim")
		}

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("afrepladd-%d", i))...)
		}
		ValidateApplications(contexts)

		stepLog = "setting volumes repl to 2"
		var appVolumes []*volume.Volume
		var selectedCtx *scheduler.Context
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				if !strings.Contains(ctx.App.Key, "fio-fstrim") {
					continue
				}
				selectedCtx = ctx

				var err error
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "Failed to get volumes")
					dash.VerifyFatal(len(appVolumes) > 0, true, fmt.Sprintf("Found %d app volmues", len(appVolumes)))
				})

				for _, v := range appVolumes {
					// Check if volumes are Pure FA/FB DA volumes
					isPureVol, err := Inst().V.IsPureVolume(v)
					log.FailOnError(err, "Failed to check is PURE volume")
					if isPureVol {
						log.Warnf("Repl increase on Pure DA Volume [%s] not supported. Skipping this operation", v.Name)
						continue
					}

					currRep, err := Inst().V.GetReplicationFactor(v)
					log.FailOnError(err, "Failed to get Repl factor for vil %s", v.Name)

					//Reduce replication factor
					if currRep == 3 {
						log.Infof("Current replication is 3, reducing before proceeding")
						opts := volume.Options{
							ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
						}
						err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true, opts)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set repl factor to %d", currRep-1))
					}

				}
			}
		})

		stepLog = "has to check the autofstrim status with volume update on node with no pool 0"
		var fsTrimStatuses map[string]opsapi.FilesystemTrim_FilesystemTrimStatus
		var fsUsage map[string]*opsapi.FstrimVolumeUsageInfo
		var err error
		Step(stepLog, func() {
			log.InfoD(stepLog)

			fsTrimStatuses, err = GetAutoFsTrimStatusForCtx(selectedCtx)
			log.FailOnError(err, "error getting autofs status")

			fsUsage, err = GetAutoFstrimUsageForCtx(selectedCtx)
			log.FailOnError(err, "error getting autofs usage")

			volReplNodes := make([]string, 0)
			for k := range fsTrimStatuses {
				selectedVol, err := Inst().V.InspectVolume(k)
				log.FailOnError(err, "error inspecting vol [%s]", k)
				volReplNodes = append(volReplNodes, selectedVol.ReplicaSets[0].Nodes...)
			}

			//Selecting a node not part of selected volume replica set and multiple pools
			stNodes := node.GetStorageNodes()
			var selectedNode node.Node
			for _, n := range stNodes {
				if !Contains(volReplNodes, n.Id) && len(n.Pools) > 1 {
					//verifying if node has pool 0
					var ispoolExist bool
					for _, p := range n.Pools {
						if p.ID == 0 {
							ispoolExist = true
						}
					}
					if ispoolExist {
						selectedNode = n
						break
					}
				}
			}

			if selectedNode.Id == "" {
				log.FailOnError(fmt.Errorf("no node found"), "error identifying node for repl increase and pool deletion")
			}

			stepLog = fmt.Sprintf("delete pool 0 from the node [%s]", selectedNode.Name)
			Step(stepLog, func() {
				poolsBfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
				log.FailOnError(err, "Failed to list storage pools")

				log.InfoD(stepLog)
				log.InfoD("Setting pools in maintenance on node %s", selectedNode.Name)
				err = Inst().V.EnterPoolMaintenance(selectedNode)
				log.FailOnError(err, "failed to set pool maintenance mode on node %s", selectedNode.Name)

				time.Sleep(1 * time.Minute)
				expectedStatus := "In Maintenance"
				err = WaitForPoolStatusToUpdate(selectedNode, expectedStatus)
				log.FailOnError(err, fmt.Sprintf("node %s pools are not in status %s", selectedNode.Name, expectedStatus))

				err = Inst().V.DeletePool(selectedNode, "0", true)
				log.FailOnError(err, "failed to delete poolID 0 on node %s", selectedNode.Name)

				err = Inst().V.ExitPoolMaintenance(selectedNode)
				log.FailOnError(err, "failed to exit pool maintenance mode on node %s", selectedNode.Name)

				err = Inst().V.WaitDriverUpOnNode(selectedNode, 5*time.Minute)
				log.FailOnError(err, "volume driver down on node %s", selectedNode.Name)

				expectedStatus = "Online"
				err = WaitForPoolStatusToUpdate(selectedNode, expectedStatus)
				log.FailOnError(err, fmt.Sprintf("node %s pools are not in status %s", selectedNode.Name, expectedStatus))

				poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
				log.FailOnError(err, "Failed to list storage pools")

				dash.VerifySafely(len(poolsBfr) > len(poolsAfr), true, "verify pools count is updated after pools deletion")
			})

			stepLog = fmt.Sprintf("Repl increase of volumes to the node %s", selectedNode.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				appVolumes, err = Inst().S.GetVolumes(selectedCtx)
				log.FailOnError(err, "Failed to get volumes")
				for _, v := range appVolumes {
					appVol, err := Inst().V.InspectVolume(v.ID)
					log.FailOnError(err, fmt.Sprintf("error inspecting volume [%s]", v.ID))

					if _, ok := fsTrimStatuses[appVol.Id]; ok {
						opts := volume.Options{
							ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
						}
						err = Inst().V.SetReplicationFactor(v, 3, []string{selectedNode.Id}, nil, true, opts)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set repl factor to 3 for volume [%s]", v.Name))
					}
				}

			})

			stepLog = fmt.Sprintf("validate autofstrim status for the volumes")
			Step(stepLog, func() {
				newFsTrimStatuses, err := GetAutoFsTrimStatusForCtx(selectedCtx)
				log.FailOnError(err, "error getting autofs status")

				for k := range fsTrimStatuses {
					val, ok := newFsTrimStatuses[k]
					dash.VerifySafely(ok, true, fmt.Sprintf("verify autofstrim started for volume %s", k))
					dash.VerifySafely(val != opsapi.FilesystemTrim_FS_TRIM_FAILED, true, fmt.Sprintf("verify autofstrim for volume %s, current status %v", k, val))
				}
				newFsUsage, err := GetAutoFstrimUsageForCtx(selectedCtx)
				log.FailOnError(err, "error getting autofs usage")

				for k := range fsUsage {
					val, ok := newFsUsage[k]
					dash.VerifySafely(ok, true, fmt.Sprintf("verify autofstrim usage for volume %s", k))
					dash.VerifySafely(val.PerformAutoFstrim, "Enabled", fmt.Sprintf("verify autofstrim for volume %s is not disabled, current status %v", k, val))
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

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Add a test to detach all the disks from a node and then reattach them to a different node
var _ = Describe("{NodeDiskDetachAttach}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("NodeDiskDetachAttach", "Validate disk detach and attach", nil, 0)
	})
	var contexts []*scheduler.Context

	testName := "nodediskdetachattach"
	stepLog := "has to detach all disks from a node and reattach them to a different node"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		randomNum := rand.New(rand.NewSource(time.Now().Unix()))

		ValidateApplications(contexts)
		// Fetch the node where PX is not started
		nonPXNodes := node.GetPXDisabledNodes()
		// Remove disks from all nodes in nonPXNodes
		for _, n := range nonPXNodes {
			// Fetch the disks attached to the node
			err := Inst().N.RemoveNonRootDisks(n)
			log.FailOnError(err, fmt.Sprintf("Failed to remove disks on node %s", n.Name))
		}
		randNonPxNode := nonPXNodes[randomNum.Intn(len(nonPXNodes))]

		// Fetch a random node from the StorageNodes
		storageNodes := node.GetStorageNodes()
		randomStorageNode := storageNodes[randomNum.Intn(len(storageNodes))]

		// Fetch random storage node and detach the disks attached to that node
		var oldNodeIDtoMatch string
		var newNodeIDtoMatch string
		Step(fmt.Sprintf("detach disks attached from a random storage node %s and attach to non px node %s", randomStorageNode.Name, randNonPxNode.Name), func() {
			log.Infof("Detaching disks from node %s and attaching to node %s", randomStorageNode.Name, randNonPxNode.Name)
			// ToDo - Ensure the above selected node has volumes/apps running on it
			// Store the Node ID of randomStorageNode for future use
			oldNodeIDtoMatch = randomStorageNode.Id
			// Stop PX on the node
			err = k8sCore.AddLabelOnNode(randomStorageNode.Name, schedops.PXServiceLabelKey, "stop")
			log.FailOnError(err, fmt.Sprintf("Failed to add label %s=stop on node %s", schedops.PXServiceLabelKey, randomStorageNode.Name))

			err = Inst().N.MoveDisks(randomStorageNode, randNonPxNode)
			log.FailOnError(err, fmt.Sprintf("Failed to move disks from node %s to node %s", randomStorageNode.Name, randNonPxNode.Name))

			// Add PXEnabled false label to srcVM
			err = k8sCore.AddLabelOnNode(randomStorageNode.Name, schedops.PXEnabledLabelKey, "false")
			log.FailOnError(err, fmt.Sprintf("Failed to add label %s=stop on node %s", schedops.PXServiceLabelKey, randomStorageNode.Name))

			// Start PX on the node
			err = k8sCore.AddLabelOnNode(randNonPxNode.Name, schedops.PXEnabledLabelKey, "true")
			log.FailOnError(err, fmt.Sprintf("Failed to add label %s=true on node %s", schedops.PXEnabledLabelKey, randNonPxNode.Name))
			err = k8sCore.AddLabelOnNode(randNonPxNode.Name, schedops.PXServiceLabelKey, "start")
			log.FailOnError(err, fmt.Sprintf("Failed to add label %s=start on node %s", schedops.PXServiceLabelKey, randNonPxNode.Name))
			err = Inst().V.WaitForPxPodsToBeUp(randNonPxNode)
			log.FailOnError(err, fmt.Sprintf("Failed to wait for PX pods to be up on node %s", randNonPxNode.Name))
			// Refresh the driver endpoints
			err = Inst().S.RefreshNodeRegistry()
			log.FailOnError(err, "error refreshing node registry")
			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing storage drive endpoints")
			// Wait for the driver to be up on the node
			randNonPxNode, err = node.GetNodeByName(randNonPxNode.Name)
			log.FailOnError(err, fmt.Sprintf("Failed to get node %s", randNonPxNode.Name))
			err = Inst().V.WaitDriverUpOnNode(randNonPxNode, Inst().DriverStartTimeout)
			dash.VerifyFatal(err, nil, "Validate volume is driver up")
			// Verify the node ID is same as earlier stored Node ID
			newNodeIDtoMatch = randNonPxNode.Id

			dash.VerifyFatal(oldNodeIDtoMatch, newNodeIDtoMatch, fmt.Sprintf("Node ID mismatch for node %s after moving the disks", randNonPxNode.Name))
			log.Infof("Node ID matches for node %s [%s == %s]", randNonPxNode.Name, oldNodeIDtoMatch, newNodeIDtoMatch)
		})

		// ToDo - Verify the integrity of apps, cluster and volumes

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

var _ = Describe("{DeployApps}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("DeployApps", "Validate Apps deployment", nil, 0)

	})

	var contexts []*scheduler.Context

	It("has to deploy and  validate  apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("deployapps-%d", i))...)
		}
		ValidateApplications(contexts)

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{RestartMultipleStorageNodeOneKVDBMaster}", func() {
	/*
		Restart Multiple Storage Nodes with one KVDB Master in parallel and wait for the node to come back online
		https://portworx.atlassian.net/browse/PTX-17618
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("RestartMultipleStorageNodeOneKVDBMaster",
			"Restart Multiple Storage Nodes with one KVDB Master",
			nil, 0)
	})
	var contexts []*scheduler.Context
	stepLog := "Expand multiple pool in the cluster at once in parallel"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)
		var wg sync.WaitGroup

		listOfStorageNodes := node.GetStorageNodes()
		// Test Needs minimum of 3 nodes other than 3 KVDB Member nodes
		// so that few storage nodes (except kvdb nodes ) can be restarted
		dash.VerifyFatal(len(listOfStorageNodes) >= 6, true, "Test Needs minimum of 6 Storage Nodes")

		// assuming that there are minimum number of 3 nodes minus kvdb member nodes , we pick atleast 50% of the nodes for restating
		var nodesToReboot []node.Node
		getKVDBNodes, err := GetAllKvdbNodes()
		log.FailOnError(err, "failed to get list of all kvdb nodes")

		// Verifying if we have kvdb quorum set
		dash.VerifyFatal(len(getKVDBNodes) == 3, true, "missing required kvdb member nodes")

		// Get 50 % of other nodes for restart
		nodeCountsForRestart := (len(listOfStorageNodes) - len(getKVDBNodes)) / 2
		log.InfoD("total nodes picked for rebooting [%v]", nodeCountsForRestart)

		isKVDBNode := func(n node.Node) (bool, bool) {
			for _, eachKvdb := range getKVDBNodes {
				if n.Id == eachKvdb.ID {
					if eachKvdb.Leader == true {
						return true, true
					} else {
						return true, false
					}
				}
			}
			return false, false
		}

		count := 0
		// Add one KVDB node to the List
		for _, each := range listOfStorageNodes {
			kvdbNode, master := isKVDBNode(each)
			if kvdbNode == true && master == true {
				nodesToReboot = append(nodesToReboot, each)
				count = count + 1
			}
		}
		// Add nodes which are not KVDB Nodes
		for _, each := range listOfStorageNodes {
			kvdbNode, _ := isKVDBNode(each)
			if kvdbNode == false {
				if count <= nodeCountsForRestart {
					nodesToReboot = append(nodesToReboot, each)
					count = count + 1
				}
			}
		}

		for _, eachNode := range nodesToReboot {
			log.InfoD("Selected Node [%v] for Restart", eachNode.Name)
		}

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rebootmulparallel-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Initiate all node reboot at once using Go Routines
		wg.Add(len(nodesToReboot))

		rebootNode := func(n node.Node) {
			defer wg.Done()
			defer GinkgoRecover()
			log.InfoD("Rebooting Node [%v]", n.Name)

			err := Inst().N.RebootNode(n, node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         1 * time.Minute,
					TimeBeforeRetry: 5 * time.Second,
				},
			})
			log.FailOnError(err, "failed to reboot Node [%v]", n.Name)

		}

		// Initiating Go Routing to reboot all the nodes at once
		rebootAllNodes := func() {
			for _, each := range nodesToReboot {
				log.InfoD("Node to Reboot [%v]", each.Name)
				go rebootNode(each)
			}
			wg.Wait()

			// Wait for connection to come back online after reboot
			for _, each := range nodesToReboot {
				err = Inst().N.TestConnection(each, node.ConnectionOpts{
					Timeout:         15 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})

				err = Inst().S.IsNodeReady(each)
				log.FailOnError(err, "Node [%v] is not in ready state", each.Name)

				err = Inst().V.WaitDriverUpOnNode(each, Inst().DriverStartTimeout)
				log.FailOnError(err, "failed waiting for driver up on Node[%v]", each.Name)
			}
		}

		// Reboot all the Nodes at once
		rebootAllNodes()

		// Verifications
		getKVDBNodes, err = GetAllKvdbNodes()
		log.FailOnError(err, "failed to get list of all kvdb nodes")
		dash.VerifyFatal(len(getKVDBNodes) == 3, true, "missing required kvdb member nodes after node reboot")

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{KvdbFailoverSnapVolCreateDelete}", func() {
	/*
		KVDB failover when lots of snap create/delete, volume inspect requests are coming
		https://portworx.atlassian.net/browse/PTX-17729
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("KvdbFailoverSnapVolCreateDelete",
			"KVDB failover when lot of snap create/delete, volume inspect requests are coming",
			nil, 0)
	})
	var contexts []*scheduler.Context
	stepLog := "Expand multiple pool in the cluster at once in parallel"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)
		var wg sync.WaitGroup
		wg.Add(4)
		var volumesCreated []string
		var snapshotsCreated []string

		terminate := false

		stopRoutine := func() {
			if !terminate {
				terminate = true
				wg.Done()
				for _, each := range volumesCreated {
					if IsVolumeExits(each) {
						log.FailOnError(Inst().V.DeleteVolume(each), "volume deletion failed on the cluster with volume ID [%s]", each)
					}

				}
				for _, each := range snapshotsCreated {
					if IsVolumeExits(each) {
						log.FailOnError(Inst().V.DeleteVolume(each), "Snapshot Volume deletion failed on the cluster with ID [%s]", each)
					}
				}
			}
		}
		defer stopRoutine()

		go func() {
			defer wg.Done()
			defer GinkgoRecover()

			// Volume Create continuously
			for {
				if terminate {
					break
				}
				// Create Volume on the Cluster
				uuidObj := uuid.New()
				VolName := fmt.Sprintf("volume_%s", uuidObj.String())
				Size := uint64(rand.Intn(10) + 1)   // Size of the Volume between 1G to 10G
				haUpdate := int64(rand.Intn(3) + 1) // Size of the HA between 1 and 3

				volId, err := Inst().V.CreateVolume(VolName, Size, int64(haUpdate))
				log.FailOnError(err, "volume creation failed on the cluster with volume name [%s]", VolName)
				log.InfoD("Volume created with name [%s] having id [%s]", VolName, volId)

				volumesCreated = append(volumesCreated, volId)
			}
		}()

		inspectDeleteVolume := func(volumeId string) error {
			defer GinkgoRecover()
			if IsVolumeExits(volumeId) {
				// inspect volume
				appVol, err := Inst().V.InspectVolume(volumeId)
				if err != nil {
					stopRoutine()
					return err
				}

				err = Inst().V.DeleteVolume(appVol.Id)
				if err != nil {
					stopRoutine()
					return err
				}
			}
			return nil
		}

		go func() {
			defer wg.Done()
			defer GinkgoRecover()

			// Create Snapshots on Volumes continuously
			for {
				if terminate {
					break
				}
				if len(volumesCreated) > 5 {
					for _, eachVol := range volumesCreated {
						uuidCreated := uuid.New()
						snapshotName := fmt.Sprintf("snapshot_%s_%s", eachVol, uuidCreated.String())

						snapshotResponse, err := Inst().V.CreateSnapshot(eachVol, snapshotName)
						if err != nil {
							stopRoutine()
							log.FailOnError(err, "error Creating Snapshot [%s]", eachVol)
						}

						snapshotsCreated = append(snapshotsCreated, snapshotResponse.GetSnapshotId())
						log.InfoD("Snapshot [%s] created with ID [%s]", snapshotName, snapshotResponse.GetSnapshotId())

						err = inspectDeleteVolume(eachVol)
						log.FailOnError(err, "Inspect and Delete Volume failed on cluster with Volume ID [%v]", eachVol)

						// Remove the first element
						for i := 0; i < len(volumesCreated)-1; i++ {
							volumesCreated[i] = volumesCreated[i+1]
						}
						// Resize the array by truncating the last element
						volumesCreated = volumesCreated[:len(volumesCreated)-1]
					}
				}
			}
		}()

		go func() {
			defer wg.Done()
			defer GinkgoRecover()

			// Delete Snapshots on Volumes continuously
			for {
				if terminate {
					break
				}
				if len(snapshotsCreated) > 5 {
					for _, each := range snapshotsCreated {
						err := inspectDeleteVolume(each)
						log.FailOnError(err, "Inspect and Delete Snapshot failed on cluster with snapshot ID [%v]", each)

						// Remove the first element
						for i := 0; i < len(snapshotsCreated)-1; i++ {
							snapshotsCreated[i] = snapshotsCreated[i+1]
						}
						// Resize the array by truncating the last element
						snapshotsCreated = snapshotsCreated[:len(snapshotsCreated)-1]
					}
				}
			}
		}()

		for i := 0; i < 6; i++ {
			// Wait for KVDB Members to be online
			err := WaitForKVDBMembers()
			if err != nil {
				stopRoutine()
				log.FailOnError(err, "failed waiting for KVDB members to be active")
			}

			// Kill KVDB Master Node
			masterNode, err := GetKvdbMasterNode()
			if err != nil {
				stopRoutine()
				log.FailOnError(err, "failed getting details of KVDB master node")
			}

			// Get KVDB Master PID
			pid, err := GetKvdbMasterPID(*masterNode)
			if err != nil {
				stopRoutine()
				log.FailOnError(err, "failed getting PID of KVDB master node")
			}

			log.InfoD("KVDB Master is [%v] and PID is [%v]", masterNode.Name, pid)

			// Kill kvdb master PID for regular intervals
			err = KillKvdbMemberUsingPid(*masterNode)
			if err != nil {
				stopRoutine()
				log.FailOnError(err, "failed to kill KVDB Node")
			}

			// Wait for some time after killing kvdb master Node
			time.Sleep(5 * time.Minute)
		}

		terminate = true
		wg.Wait()
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})