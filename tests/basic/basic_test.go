package tests

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
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
	It("has to setup, validate and teardown apps", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("setupteardown-%d", i))...)
		}

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
})

// Volume Driver Plugin is down, unavailable - and the client container should not be impacted.
var _ = Describe("{VolumeDriverDown}", func() {
	It("has to schedule apps and stop volume driver on app nodes", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("voldriverdown-%d", i))...)
		}

		Step("get nodes for all apps in test and bounce volume driver", func() {
			for _, ctx := range contexts {
				appNodes := getNodesThatCanBeDown(ctx)
				Step(
					fmt.Sprintf("stop volume driver %s on app %s's nodes: %v",
						Inst().V.String(), ctx.App.Key, appNodes),
					func() {
						StopVolDriverAndWait(appNodes)
					})

				Step("starting volume driver", func() {
					StartVolDriverAndWait(appNodes)
				})

				Step("Giving few seconds for volume driver to stabilize", func() {
					time.Sleep(20 * time.Second)
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
})

// Volume Driver Plugin is down, unavailable on the nodes where the volumes are
// attached - and the client container should not be impacted.
var _ = Describe("{VolumeDriverDownAttachedNode}", func() {
	It("has to schedule apps and stop volume driver on nodes where volumes are attached", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("voldriverdownattachednode-%d", i))...)
		}

		Step("get nodes for all apps in test and restart volume driver", func() {
			for _, ctx := range contexts {
				appNodes := getNodesThatCanBeDown(ctx)
				Step(fmt.Sprintf("stop volume driver %s on app %s's nodes: %v",
					Inst().V.String(), ctx.App.Key, appNodes), func() {
					StopVolDriverAndWait(appNodes)
				})

				Step("starting volume driver", func() {
					StartVolDriverAndWait(appNodes)
				})

				Step("Giving few seconds for volume driver to stabilize", func() {
					time.Sleep(20 * time.Second)
				})
			}
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
})

// Volume Driver Plugin has crashed - and the client container should not be impacted.
var _ = Describe("{VolumeDriverCrash}", func() {
	It("has to schedule apps and crash volume driver on app nodes", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("voldrivercrash-%d", i))...)
		}

		Step("get nodes for all apps in test and crash volume driver", func() {
			for _, ctx := range contexts {
				appNodes := getNodesThatCanBeDown(ctx)
				Step(
					fmt.Sprintf("crash volume driver %s on app %s's nodes: %v",
						Inst().V.String(), ctx.App.Key, appNodes),
					func() {
						CrashVolDriverAndWait(appNodes)
					})
			}
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
})

// Volume driver plugin is down and the client container gets terminated.
// There is a lost unmount call in this case. When the volume driver is
// back up, we should be able to detach and delete the volume.
var _ = Describe("{VolumeDriverAppDown}", func() {
	It("has to schedule apps, stop volume driver on app nodes and destroy apps", func() {
		var err error
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("voldriverappdown-%d", i))...)
		}

		Step("get nodes for all apps in test and bounce volume driver", func() {
			for _, ctx := range contexts {
				nodesToBeDown := getNodesThatCanBeDown(ctx)

				Step(fmt.Sprintf("stop volume driver %s on app %s's nodes: %v",
					Inst().V.String(), ctx.App.Key, nodesToBeDown), func() {
					StopVolDriverAndWait(nodesToBeDown)
				})

				Step(fmt.Sprintf("destroy app: %s", ctx.App.Key), func() {
					err = Inst().S.Destroy(ctx, nil)
					Expect(err).NotTo(HaveOccurred())

					Step("wait for few seconds for app destroy to trigger", func() {
						time.Sleep(10 * time.Second)
					})
				})

				Step("restarting volume driver", func() {
					StartVolDriverAndWait(nodesToBeDown)
				})

				Step(fmt.Sprintf("wait for destroy of app: %s", ctx.App.Key), func() {
					err = Inst().S.WaitForDestroy(ctx)
					Expect(err).NotTo(HaveOccurred())
				})

				DeleteVolumesAndWait(ctx)
			}
		})
	})
})

func getNodesThatCanBeDown(ctx *scheduler.Context) []node.Node {
	var appNodes []node.Node
	var err error
	Step(fmt.Sprintf("get nodes for %s app", ctx.App.Key), func() {
		appNodes, err = Inst().S.GetNodesForApp(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(appNodes).NotTo(BeEmpty())
	})
	var appVolumes []*volume.Volume
	Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
		appVolumes, err = Inst().S.GetVolumes(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(appVolumes).NotTo(BeEmpty())
	})
	// avoid dup
	nodesThatCantBeDown := make(map[string]bool)
	nodesToBeDown := make([]node.Node, 0)
	Step(fmt.Sprintf("choose nodes to be down for %s app", ctx.App.Key), func() {
		for _, vol := range appVolumes {
			replicas, err := Inst().V.GetReplicaSetNodes(vol)
			Expect(err).NotTo(HaveOccurred())
			Expect(replicas).NotTo(BeEmpty())
			// at least n-1 nodes with replica need to be up
			maxNodesToBeDown := getMaxNodesToBeDown(len(replicas))
			for _, nodeName := range replicas[maxNodesToBeDown:] {
				nodesThatCantBeDown[nodeName] = true
			}
		}

		for _, node := range appNodes {
			if _, exists := nodesThatCantBeDown[node.Name]; !exists {
				nodesToBeDown = append(nodesToBeDown, node)
			}
		}

	})
	return nodesToBeDown
}

// This test deletes all tasks of an application and checks if app converges back to desired state
var _ = Describe("{AppTasksDown}", func() {
	It("has to schedule app and delete app tasks", func() {
		var err error
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("apptasksdown-%d", i))...)
		}

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
						err = Inst().S.DeleteTasks(ctx)
						Expect(err).NotTo(HaveOccurred())
					})

					ValidateContext(ctx)
				}
			} else {
				start := time.Now().Local()
				for int(time.Since(start).Seconds()) < timeout {
					for _, ctx := range contexts {
						Step(fmt.Sprintf("delete tasks for app: %s", ctx.App.Key), func() {
							err = Inst().S.DeleteTasks(ctx)
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
})

func getMaxNodesToBeDown(replicas int) int {
	if replicas == 1 {
		return 0
	}
	if replicas%2 != 0 {
		return replicas/2 + 1
	}
	return replicas / 2
}

// This test scales up and down an application and checks if app has actually scaled accordingly
var _ = Describe("{AppScaleUpAndDown}", func() {
	It("has to scale up and scale down the app", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("applicationscaleupdown-%d", i))...)
		}

		Step("Scale up and down all app", func() {
			for _, ctx := range contexts {
				Step(fmt.Sprintf("scale up app: %s by %d ", ctx.App.Key, len(node.GetWorkerNodes())), func() {
					applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					for name, scale := range applicationScaleUpMap {
						applicationScaleUpMap[name] = scale + int32(len(node.GetWorkerNodes()))
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
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	CollectSupport()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
