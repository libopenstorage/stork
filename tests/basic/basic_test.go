package tests

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Basic", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{SetupTeardown}", func() {
	var testrailID = 35258
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35258
	var runID int
	JustBeforeEach(func() {
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
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Volume Driver Plugin is down, unavailable - and the client container should not be impacted.
var _ = Describe("{VolumeDriverDown}", func() {
	var testrailID = 35259
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35259
	var runID int
	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to schedule apps and stop volume driver on app nodes", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverdown-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("get nodes bounce volume driver", func() {
			for _, appNode := range node.GetStorageDriverNodes() {
				Step(
					fmt.Sprintf("stop volume driver %s on node: %s",
						Inst().V.String(), appNode.Name),
					func() {
						StopVolDriverAndWait([]node.Node{appNode})
					})

				Step(
					fmt.Sprintf("starting volume %s driver on node %s",
						Inst().V.String(), appNode.Name),
					func() {
						StartVolDriverAndWait([]node.Node{appNode})
					})

				Step("Giving few seconds for volume driver to stabilize", func() {
					time.Sleep(20 * time.Second)
				})

				Step("validate apps", func() {
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})
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

// Volume Driver Plugin is down, unavailable on the nodes where the volumes are
// attached - and the client container should not be impacted.
var _ = Describe("{VolumeDriverDownAttachedNode}", func() {
	var testrailID = 35260
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35260
	var runID int
	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to schedule apps and stop volume driver on nodes where volumes are attached", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverdownattachednode-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("get nodes where app is running and restart volume driver", func() {
			for _, ctx := range contexts {
				appNodes, err := Inst().S.GetNodesForApp(ctx)
				Expect(err).NotTo(HaveOccurred())
				for _, appNode := range appNodes {
					Step(
						fmt.Sprintf("stop volume driver %s on app %s's node: %s",
							Inst().V.String(), ctx.App.Key, appNode.Name),
						func() {
							StopVolDriverAndWait([]node.Node{appNode})
						})

					Step(
						fmt.Sprintf("starting volume %s driver on app %s's node %s",
							Inst().V.String(), ctx.App.Key, appNode.Name),
						func() {
							StartVolDriverAndWait([]node.Node{appNode})
						})

					Step("Giving few seconds for volume driver to stabilize", func() {
						time.Sleep(20 * time.Second)
					})

					Step(fmt.Sprintf("validate app %s", ctx.App.Key), func() {
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
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Volume Driver Plugin has crashed - and the client container should not be impacted.
var _ = Describe("{VolumeDriverCrash}", func() {
	var testrailID = 35261
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35261
	var runID int
	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to schedule apps and crash volume driver on app nodes", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldrivercrash-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("crash volume driver in all nodes", func() {
			for _, appNode := range node.GetStorageDriverNodes() {
				Step(
					fmt.Sprintf("crash volume driver %s on node: %v",
						Inst().V.String(), appNode.Name),
					func() {
						CrashVolDriverAndWait([]node.Node{appNode})
					})
			}
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
	JustAfterEach(func() {
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
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to schedule apps, stop volume driver on app nodes and destroy apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverappdown-%d", i))...)
		}

		ValidateApplications(contexts)

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		Step("get nodes for all apps in test and bounce volume driver", func() {
			for _, ctx := range contexts {
				appNodes, err := Inst().S.GetNodesForApp(ctx)
				Expect(err).NotTo(HaveOccurred())
				appNode := appNodes[r.Intn(len(appNodes))]
				Step(fmt.Sprintf("stop volume driver %s on app %s's nodes: %v",
					Inst().V.String(), ctx.App.Key, appNode), func() {
					StopVolDriverAndWait([]node.Node{appNode})
				})

				Step(fmt.Sprintf("destroy app: %s", ctx.App.Key), func() {
					err = Inst().S.Destroy(ctx, nil)
					Expect(err).NotTo(HaveOccurred())

					Step("wait for few seconds for app destroy to trigger", func() {
						time.Sleep(10 * time.Second)
					})
				})

				Step("restarting volume driver", func() {
					StartVolDriverAndWait([]node.Node{appNode})
				})

				Step(fmt.Sprintf("wait for destroy of app: %s", ctx.App.Key), func() {
					err = Inst().S.WaitForDestroy(ctx, Inst().DestroyAppTimeout)
					Expect(err).NotTo(HaveOccurred())
				})

				DeleteVolumesAndWait(ctx, nil)
			}
		})
	})
	JustAfterEach(func() {
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test deletes all tasks of an application and checks if app converges back to desired state
var _ = Describe("{AppTasksDown}", func() {
	var testrailID = 35263
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35264
	var runID int
	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to schedule app and delete app tasks", func() {
		var err error
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("apptasksdown-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("delete all application tasks", func() {
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
					Step(fmt.Sprintf("delete tasks for app: %s", ctx.App.Key), func() {
						err = Inst().S.DeleteTasks(ctx, nil)
						Expect(err).NotTo(HaveOccurred())
					})

					ValidateContext(ctx)
				}
			} else {
				start := time.Now().Local()
				for int(time.Since(start).Seconds()) < timeout {
					for _, ctx := range contexts {
						Step(fmt.Sprintf("delete tasks for app: %s", ctx.App.Key), func() {
							err = Inst().S.DeleteTasks(ctx, nil)
							Expect(err).NotTo(HaveOccurred())
						})

						ValidateContext(ctx)
					}
					Step(fmt.Sprintf("Sleeping for given duration %d", frequency), func() {
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
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test scales up and down an application and checks if app has actually scaled accordingly
var _ = Describe("{AppScaleUpAndDown}", func() {
	var testrailID = 35264
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35264
	var runID int
	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to scale up and scale down the app", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("applicationscaleupdown-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("Scale up and down all app", func() {
			for _, ctx := range contexts {
				Step(fmt.Sprintf("scale up app: %s by %d ", ctx.App.Key, len(node.GetWorkerNodes())), func() {
					applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					workerNodes := int32(len(node.GetWorkerNodes()))
					for name, scale := range applicationScaleUpMap {
						// limit scale up to the number of worker nodes
						if scale < workerNodes {
							applicationScaleUpMap[name] = workerNodes
						}
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
					Expect(err).NotTo(HaveOccurred())
				})

				Step("Giving few seconds for scaled up applications to stabilize", func() {
					time.Sleep(10 * time.Second)
				})

				ValidateContext(ctx)

				Step(fmt.Sprintf("scale down app %s by 1", ctx.App.Key), func() {
					applicationScaleDownMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					for name, scale := range applicationScaleDownMap {
						applicationScaleDownMap[name] = scale - 1
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleDownMap)
					Expect(err).NotTo(HaveOccurred())
				})

				Step("Giving few seconds for scaled down applications to stabilize", func() {
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
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{CordonDeployDestroy}", func() {
	var contexts []*scheduler.Context

	It("has to cordon all nodes but one, deploy and destroy app", func() {

		Step("Cordon all nodes but one", func() {
			nodes := node.GetWorkerNodes()
			for _, node := range nodes[1:] {
				err := Inst().S.DisableSchedulingOnNode(node)
				Expect(err).NotTo(HaveOccurred())
			}
		})
		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("cordondeploydestroy-%d", i))...)
			}
			ValidateApplications(contexts)

		})
		Step("Destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForDestroy] = false
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = false
			for _, ctx := range contexts {
				err := Inst().S.Destroy(ctx, opts)
				Expect(err).NotTo(HaveOccurred())
			}
		})
		Step("Validate destroy", func() {
			for _, ctx := range contexts {
				err := Inst().S.WaitForDestroy(ctx, Inst().DestroyAppTimeout)
				Expect(err).NotTo(HaveOccurred())
			}
		})
		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
		Step("Uncordon all nodes", func() {
			nodes := node.GetWorkerNodes()
			for _, node := range nodes {
				err := Inst().S.EnableSchedulingOnNode(node)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
